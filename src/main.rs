//! Orient orchestrator: the only service the phone talks to.
//!
//! Implements docs/contracts.md from tungsten-united/project-description.
//!
//! A client is one phone from Start to Stop: it holds the token and the event stream.
//! A session is one spoken action (go to a destination). When speech-to-text yields a
//! different action, a new session starts with an empty frame buffer and no previous output.

mod pipeline;

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::io::Write;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::rejection::JsonRejection;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::task::AbortHandle;
use tower_http::cors::{AllowOrigin, CorsLayer};

use pipeline::{Command, Destination, Output, Pipeline, Route, env};

const AUDIO_TYPES: [&str; 2] = ["audio/webm", "audio/mp4"];
const NAV_FAILURES_BEFORE_ERROR: u32 = 3;
const UNAVAILABLE: &str = "Guidance is unavailable. Double tap to try again.";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Shallow merge of two JSON objects; `extra` wins.
fn merge(base: &Value, extra: Value) -> Value {
    let mut out = base.clone();
    if let (Some(o), Value::Object(e)) = (out.as_object_mut(), extra) {
        o.extend(e);
    }
    out
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Limits {
    max_audio_ms: i64, // enforced by the phone; the server only checks bytes
    max_audio_bytes: usize,
    max_frame_bytes: usize,
    max_frame_edge_px: i64, // enforced by the phone
    max_input_age_ms: i64,
    heartbeat_ms: u64,
    nav_frames: usize,
}

struct AppState {
    route: Route,
    pipeline: Pipeline,
    limits: Limits,
    trace_path: Option<String>,
    // ponytail: in-memory clients in one process, never expired. Fine for one demo phone; add a store if we scale out.
    clients: Mutex<HashMap<String, ClientRef>>,
}

type App = Arc<AppState>;
type ClientRef = Arc<Mutex<Client>>;

impl AppState {
    fn new(route: Route, pipeline: Pipeline) -> Self {
        AppState {
            route,
            pipeline,
            limits: Limits {
                max_audio_ms: 10_000,
                max_audio_bytes: 1_000_000,
                max_frame_bytes: 512_000,
                max_frame_edge_px: 1280,
                max_input_age_ms: env("MAX_INPUT_AGE_MS", "3000")
                    .parse()
                    .expect("MAX_INPUT_AGE_MS"),
                heartbeat_ms: 5000,
                nav_frames: env("NAV_FRAMES", "5").parse().expect("NAV_FRAMES"),
            },
            trace_path: std::env::var("TRACE_PATH").ok(),
            clients: Mutex::default(),
        }
    }

    fn destination(&self, id: &str) -> &Destination {
        self.route
            .destinations
            .iter()
            .find(|d| d.destination_id == id)
            .expect("validated destination")
    }

    fn hint(&self, step: &str) -> &str {
        self.route.steps.get(step).map_or("", |s| s.hint.as_str())
    }
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Meta {
    request_id: String,
    generation: i64,
    sequence: i64,
    captured_at: i64,
    client_route_step_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StopBody {
    request_id: String,
    #[allow(dead_code)] // stop always wins, whatever the generation
    generation: i64,
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum RetryFrom {
    DestinationPrompt,
    LastConfirmedStep,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RetryBody {
    request_id: String,
    #[allow(dead_code)] // retry, like stop, accepts any generation
    generation: i64,
    from: RetryFrom,
}

/// One spoken action: guidance to one destination.
struct Session {
    id: String,
    destination: Destination,
    step: String,
    frames: VecDeque<(Meta, Vec<u8>)>, // last `nav_frames` accepted frames, oldest first
    pending: Option<Meta>,             // newest frame not evaluated yet
    previous: Option<Output>,          // the worker compares each output with this one
    last_spoken_at: i64,
    arrived: bool,
    nav_failures: u32,
}

impl Session {
    fn new(destination: Destination, step: String) -> Self {
        Session {
            id: uuid::Uuid::new_v4().to_string(),
            destination,
            step,
            frames: VecDeque::new(),
            pending: None,
            previous: None,
            last_spoken_at: 0,
            arrived: false,
            nav_failures: 0,
        }
    }
}

/// One phone from Start to Stop.
struct Client {
    id: String,
    token: String,
    generation: i64,
    stopped: bool,
    sequence: i64,
    responses: HashMap<String, Value>, // requestId -> response body, for idempotent replays
    events: broadcast::Sender<Value>,
    tasks: Vec<AbortHandle>,
    worker: Option<tokio::task::Id>,
    session: Option<Session>,
    entries: Vec<Value>,
    trace_path: Option<String>,
}

fn lock(c: &ClientRef) -> MutexGuard<'_, Client> {
    c.lock().unwrap()
}

impl Client {
    fn phase(&self) -> &'static str {
        match &self.session {
            _ if self.stopped => "stopped",
            None => "awaiting_destination",
            Some(s) if s.arrived => "arrived",
            Some(_) => "navigating",
        }
    }

    fn event(&self, kind: &str, request_id: Option<&str>, data: Value) -> Value {
        merge(
            &json!({
                "type": kind, "eventId": uuid::Uuid::new_v4().to_string(), "clientId": self.id,
                "sessionId": self.session.as_ref().map(|s| &s.id), "generation": self.generation,
                "requestId": request_id, "emittedAt": now_ms(),
            }),
            data,
        )
    }

    fn emit(&self, kind: &str, request_id: Option<&str>, data: Value) {
        let _ = self.events.send(self.event(kind, request_id, data)); // Err only means nobody is listening
    }

    fn state(&self) -> Value {
        json!({
            "phase": self.phase(),
            "sessionId": self.session.as_ref().map(|s| &s.id),
            "destinationId": self.session.as_ref().map(|s| &s.destination.destination_id),
            "routeStepId": self.session.as_ref().map(|s| &s.step),
        })
    }

    fn heartbeat(&self, last_request_id: Option<&str>, quiet_reason: Option<&str>) -> Value {
        merge(
            &self.state(),
            json!({"lastRequestId": last_request_id, "quietReason": quiet_reason}),
        )
    }

    /// Invalidate everything in flight: late results see a newer generation and are dropped.
    fn reset(&mut self) {
        self.generation += 1;
        self.worker = None;
        if let Some(s) = &mut self.session {
            s.pending = None;
        }
        let me = tokio::task::try_id();
        for task in self.tasks.drain(..) {
            if Some(task.id()) != me {
                task.abort();
            }
        }
    }

    fn halt(&mut self, kind: &str, request_id: Option<&str>, data: Value) {
        self.emit(kind, request_id, data);
        self.reset();
        self.stopped = true;
    }

    /// A different action: new session, empty frame buffer, no previous output.
    fn start_session(&mut self, destination: Destination) {
        self.reset();
        self.stopped = false;
        // Physical position carries over when the new route passes through it.
        let step = match &self.session {
            Some(s) if destination.steps.contains(&s.step) => s.step.clone(),
            _ => destination.steps[0].clone(),
        };
        self.session = Some(Session::new(destination, step));
    }

    fn log(&mut self, request_id: Option<&str>, kind: &str, fields: Value) {
        let entry = merge(
            &json!({
                "at": now_ms(), "clientId": self.id,
                "sessionId": self.session.as_ref().map(|s| &s.id), "generation": self.generation,
                "requestId": request_id, "kind": kind, "clientRouteStepId": null,
                "routeStepId": self.session.as_ref().map(|s| &s.step),
                "destinationId": self.session.as_ref().map(|s| &s.destination.destination_id),
                "transcript": null, "command": null, "engine": null, "framesSent": null,
                "action": null, "confidence": null, "observation": null, "spoke": false,
                "text": null, "timingsMs": {}, "dropped": null, "error": null,
            }),
            fields,
        );
        if let Some(path) = &self.trace_path
            && let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
        {
            let _ = writeln!(f, "{entry}");
        }
        self.entries.push(entry);
    }

    fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) -> tokio::task::Id {
        self.tasks.retain(|t| !t.is_finished());
        let handle = tokio::spawn(fut);
        self.tasks.push(handle.abort_handle());
        handle.id()
    }

    fn accepted(&mut self, request_id: &str) -> Value {
        let body = json!({"requestId": request_id, "accepted": true});
        self.responses.insert(request_id.to_string(), body.clone());
        body
    }
}

/// Every accepted frame joins the session's buffer. Only the newest one triggers an evaluation;
/// one still waiting is superseded, but its image stays in the buffer.
fn submit_frame(app: &App, cref: &ClientRef, c: &mut Client, meta: Meta, frame: Vec<u8>) {
    let Some(s) = c.session.as_mut() else { return };
    s.frames.push_back((meta.clone(), frame));
    while s.frames.len() > app.limits.nav_frames {
        s.frames.pop_front();
    }
    let superseded = s.pending.replace(meta).map(|m| m.request_id);
    if let Some(rid) = superseded {
        c.log(Some(&rid), "frame", json!({"dropped": "superseded"}));
    }
    if c.worker.is_none() {
        c.worker = Some(c.spawn(worker(app.clone(), cref.clone())));
    }
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    retryable: bool,
}

fn api_error(status: StatusCode, code: &'static str, message: impl Into<String>) -> ApiError {
    ApiError {
        status,
        code,
        message: message.into(),
        retryable: false,
    }
}

fn bad_request(message: impl ToString) -> ApiError {
    api_error(StatusCode::BAD_REQUEST, "bad_request", message.to_string())
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({"error": {"code": self.code, "message": self.message, "retryable": self.retryable}});
        (self.status, Json(body)).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

fn get_client(app: &App, id: &str, token: &str) -> ApiResult<ClientRef> {
    let cref = app.clients.lock().unwrap().get(id).cloned();
    let cref = cref
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "client_not_found", "Unknown client."))?;
    let expected = lock(&cref).token.clone();
    let same = token.len() == expected.len()
        && token
            .bytes()
            .zip(expected.bytes())
            .fold(0, |acc, (a, b)| acc | (a ^ b))
            == 0; // constant time
    if !same {
        return Err(api_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Missing or wrong token.",
        ));
    }
    Ok(cref)
}

fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map_or("", |v| v.trim_start_matches("Bearer "))
}

fn body<T>(body: Result<Json<T>, JsonRejection>) -> ApiResult<T> {
    body.map(|Json(b)| b)
        .map_err(|e| bad_request(e.body_text()))
}

fn accept(app: &App, c: &mut Client, m: &Meta, kind: &str) -> ApiResult<()> {
    if m.generation != c.generation {
        return Err(api_error(
            StatusCode::CONFLICT,
            "stale_generation",
            "Session was stopped or replaced.",
        ));
    }
    if m.sequence <= c.sequence {
        c.log(
            Some(&m.request_id),
            kind,
            json!({"dropped": "stale_sequence"}),
        );
        return Err(api_error(
            StatusCode::CONFLICT,
            "stale_sequence",
            "A newer input was already received.",
        ));
    }
    if now_ms() - m.captured_at > app.limits.max_input_age_ms {
        c.log(
            Some(&m.request_id),
            kind,
            json!({"dropped": "expired_input"}),
        );
        let mut e = api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "expired_input",
            "Input is too old.",
        );
        e.retryable = true;
        return Err(e);
    }
    c.sequence = m.sequence;
    Ok(())
}

#[derive(Default)]
struct Parts {
    meta: Option<String>,
    audio: Option<(Vec<u8>, String)>,
    frame: Option<Vec<u8>>,
}

async fn read_parts(app: &App, mut mp: Multipart) -> ApiResult<Parts> {
    let mp_error = |e: axum::extract::multipart::MultipartError| match e.status() {
        StatusCode::PAYLOAD_TOO_LARGE => api_error(e.status(), "payload_too_large", e.body_text()),
        _ => bad_request(e.body_text()),
    };
    let mut p = Parts::default();
    while let Some(field) = mp.next_field().await.map_err(mp_error)? {
        let name = field.name().unwrap_or("").to_string();
        let content_type = field
            .content_type()
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        let (types, limit): (&[&str], usize) = match name.as_str() {
            "meta" => {
                p.meta = Some(field.text().await.map_err(mp_error)?);
                continue;
            }
            "audio" => (&AUDIO_TYPES, app.limits.max_audio_bytes),
            "frame" => (&["image/jpeg"], app.limits.max_frame_bytes),
            _ => continue,
        };
        if !types.contains(&content_type.as_str()) {
            let msg = format!("{name}: expected one of {types:?}.");
            return Err(api_error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                msg,
            ));
        }
        let data = field.bytes().await.map_err(mp_error)?.to_vec();
        if data.len() > limit {
            let msg = format!("{name}: larger than {limit} bytes.");
            return Err(api_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                msg,
            ));
        }
        if name == "audio" {
            p.audio = Some((data, content_type));
        } else {
            p.frame = Some(data);
        }
    }
    Ok(p)
}

fn parse_meta(raw: Option<String>) -> ApiResult<Meta> {
    serde_json::from_str(&raw.ok_or_else(|| bad_request("meta: missing"))?)
        .map_err(|e| bad_request(format!("meta: {e}")))
}

fn ask_again(route: &Route, reason: &str) -> String {
    let places = route
        .destinations
        .iter()
        .map(|d| d.label.as_str())
        .collect::<Vec<_>>()
        .join(" or the ");
    let first = match reason {
        "empty" => "I didn't hear anything.",
        "unsupported" => "I can't guide you there yet.",
        _ => "Sorry, I didn't understand.",
    };
    format!("{first} Where would you like to go? The {places}.")
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "version": env!("CARGO_PKG_VERSION")}))
}

async fn create_client(State(app): State<App>) -> (StatusCode, Json<Value>) {
    let id = uuid::Uuid::new_v4().to_string();
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let mut c = Client {
        id: id.clone(),
        token: token.clone(),
        generation: 1,
        stopped: false,
        sequence: -1,
        responses: HashMap::new(),
        events: broadcast::channel(64).0,
        tasks: Vec::new(),
        worker: None,
        session: None,
        entries: Vec::new(),
        trace_path: app.trace_path.clone(),
    };
    c.log(None, "client", json!({}));
    let destinations: Vec<Value> = app
        .route
        .destinations
        .iter()
        .map(|d| json!({"destinationId": d.destination_id, "label": d.label}))
        .collect();
    let response = json!({
        "clientId": id,
        "clientToken": token,
        "generation": c.generation,
        "serverTime": now_ms(),
        "phase": c.phase(),
        "route": {"routeId": app.route.route_id, "startStepId": app.route.start_step_id, "destinations": destinations},
        "limits": app.limits,
    });
    app.clients
        .lock()
        .unwrap()
        .insert(id, Arc::new(Mutex::new(c)));
    (StatusCode::CREATED, Json(response))
}

async fn events(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<impl IntoResponse> {
    let cref = get_client(&app, &id, q.get("token").map_or("", String::as_str))?;
    let (rx, first) = {
        let c = lock(&cref);
        (c.events.subscribe(), c.event("state", None, c.state()))
    };
    let heartbeat = Duration::from_millis(app.limits.heartbeat_ms);
    let stream = futures::stream::unfold((rx, Some(first)), move |(mut rx, first)| {
        let cref = cref.clone();
        async move {
            let ev = match first {
                Some(ev) => ev,
                None => match tokio::time::timeout(heartbeat, rx.recv()).await {
                    Ok(Ok(ev)) => ev,
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                        let c = lock(&cref);
                        c.event("state", None, c.state()) // fell behind: resync from a snapshot
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => return None,
                    Err(_) => {
                        let c = lock(&cref);
                        c.event("heartbeat", None, c.heartbeat(None, None))
                    }
                },
            };
            Some((
                Ok::<_, Infallible>(Event::default().data(ev.to_string())),
                (rx, None),
            ))
        }
    });
    Ok(Sse::new(stream))
}

async fn utterance(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    mp: Multipart,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let cref = get_client(&app, &id, bearer(&headers))?;
    let parts = read_parts(&app, mp).await?;
    let m = parse_meta(parts.meta)?;
    let mut c = lock(&cref);
    if let Some(r) = c.responses.get(&m.request_id) {
        return Ok((StatusCode::ACCEPTED, Json(r.clone())));
    }
    let audio = parts.audio.ok_or_else(|| bad_request("audio: missing"))?;
    if app.pipeline.stt_url.is_empty() {
        let mut e = api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream_unavailable",
            "Speech to text is not configured.",
        );
        e.retryable = true;
        return Err(e);
    }
    accept(&app, &mut c, &m, "utterance")?;
    c.spawn(handle_utterance(
        app.clone(),
        cref.clone(),
        m.clone(),
        audio,
        parts.frame,
    ));
    Ok((StatusCode::ACCEPTED, Json(c.accepted(&m.request_id))))
}

async fn frames(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    mp: Multipart,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let cref = get_client(&app, &id, bearer(&headers))?;
    let parts = read_parts(&app, mp).await?;
    let m = parse_meta(parts.meta)?;
    let frame = parts.frame.ok_or_else(|| bad_request("frame: missing"))?;
    let mut c = lock(&cref);
    if let Some(r) = c.responses.get(&m.request_id) {
        return Ok((StatusCode::ACCEPTED, Json(r.clone())));
    }
    if c.phase() != "navigating" {
        return Err(api_error(
            StatusCode::CONFLICT,
            "not_navigating",
            format!("Client is {}.", c.phase()),
        ));
    }
    accept(&app, &mut c, &m, "frame")?;
    let request_id = m.request_id.clone();
    submit_frame(&app, &cref, &mut c, m, frame);
    Ok((StatusCode::ACCEPTED, Json(c.accepted(&request_id))))
}

/// Stop always wins: any generation is accepted.
async fn stop(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    b: Result<Json<StopBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let cref = get_client(&app, &id, bearer(&headers))?;
    let b = body(b)?;
    let mut c = lock(&cref);
    if !c.responses.contains_key(&b.request_id) {
        c.halt("stop", Some(&b.request_id), json!({"reason": "user_stop"}));
        c.log(Some(&b.request_id), "stop", json!({}));
        let r = merge(
            &json!({"clientId": c.id, "generation": c.generation}),
            c.state(),
        );
        c.responses.insert(b.request_id.clone(), r);
    }
    Ok(Json(c.responses[&b.request_id].clone()))
}

/// Like stop, accepts any generation: the user asked for it, and errors bump the generation.
async fn retry(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    b: Result<Json<RetryBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let cref = get_client(&app, &id, bearer(&headers))?;
    let b = body(b)?;
    let mut c = lock(&cref);
    if let Some(r) = c.responses.get(&b.request_id) {
        return Ok(Json(r.clone()));
    }
    if b.from == RetryFrom::LastConfirmedStep && c.session.is_none() {
        return Err(bad_request("No confirmed step to resume from."));
    }
    c.reset();
    c.stopped = false;
    if b.from == RetryFrom::DestinationPrompt {
        c.session = None;
    } else if let Some(s) = &mut c.session {
        // Same session and step, but old frames and the previous output no longer describe the scene.
        s.frames.clear();
        s.previous = None;
        s.arrived = false;
        s.nav_failures = 0;
    }
    let state = c.state();
    c.emit("state", Some(&b.request_id), state.clone());
    c.log(Some(&b.request_id), "retry", json!({}));
    let r = merge(
        &json!({"clientId": c.id, "generation": c.generation}),
        state,
    );
    c.responses.insert(b.request_id, r.clone());
    Ok(Json(r))
}

async fn trace(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let cref = get_client(&app, &id, bearer(&headers))?;
    let c = lock(&cref);
    Ok(Json(json!({"clientId": c.id, "entries": c.entries})))
}

async fn handle_utterance(
    app: App,
    cref: ClientRef,
    m: Meta,
    (audio, content_type): (Vec<u8>, String),
    frame: Option<Vec<u8>>,
) {
    let (started_gen, phase) = {
        let c = lock(&cref);
        (c.generation, c.phase())
    };
    let rid = Some(m.request_id.as_str());
    let mut timings = serde_json::Map::new();
    let t = now_ms();
    let transcript = match app.pipeline.transcribe(audio, &content_type).await {
        Ok(text) => text.trim().to_string(),
        Err(e) => {
            let mut c = lock(&cref);
            if c.generation == started_gen {
                c.log(rid, "utterance", json!({"error": format!("stt: {e}")}));
                let data = json!({"code": "upstream_unavailable", "stage": "stt", "text": UNAVAILABLE, "retryable": true});
                c.halt("error", rid, data);
            }
            return;
        }
    };
    timings.insert("stt".into(), json!(now_ms() - t));
    let t = now_ms();
    let cmd = if transcript.is_empty() {
        Command::Empty
    } else {
        app.pipeline
            .command(&transcript, phase, &app.route.destinations)
            .await
    };
    timings.insert("command".into(), json!(now_ms() - t));
    timings.insert("total".into(), json!(now_ms() - m.captured_at));

    let mut c = lock(&cref);
    if c.generation != started_gen {
        c.log(
            rid,
            "utterance",
            json!({"transcript": transcript, "dropped": "stale_generation"}),
        );
        return;
    }
    c.log(
        rid,
        "utterance",
        json!({"transcript": transcript, "command": cmd.name(), "timingsMs": timings}),
    );
    match cmd {
        Command::Start(id) => {
            let same_action = c.phase() == "navigating"
                && c.session
                    .as_ref()
                    .is_some_and(|s| s.destination.destination_id == id);
            if !same_action {
                c.start_session(app.destination(&id).clone());
            }
            let state = c.state();
            c.emit("state", rid, state);
            if let Some(frame) = frame {
                submit_frame(&app, &cref, &mut c, m.clone(), frame);
            }
        }
        Command::Cancel => c.halt("stop", rid, json!({"reason": "voice_cancel"})),
        other => {
            let reason = other.name();
            c.emit(
                "needs_input",
                rid,
                json!({"reason": reason, "text": ask_again(&app.route, reason)}),
            );
        }
    }
}

/// One worker per client at a time: evaluates the newest frame against the session's buffer.
async fn worker(app: App, cref: ClientRef) {
    loop {
        let job = {
            let mut c = lock(&cref);
            match c.session.as_mut().and_then(|s| s.pending.take()) {
                Some(m) => m,
                None => {
                    // Same lock as the check, so a frame submitted now always finds the worker slot empty.
                    if c.worker == tokio::task::try_id() {
                        c.worker = None;
                    }
                    return;
                }
            }
        };
        evaluate(&app, &cref, job).await;
    }
}

async fn evaluate(app: &App, cref: &ClientRef, m: Meta) {
    let (started_gen, session_id, step, dest, frames) = {
        let c = lock(cref);
        let Some(s) = &c.session else { return };
        (
            c.generation,
            s.id.clone(),
            s.step.clone(),
            s.destination.clone(),
            s.frames.iter().cloned().collect::<Vec<_>>(),
        )
    };
    let path = &dest.steps;
    let rid = Some(m.request_id.as_str());
    let mut timings = serde_json::Map::new();
    timings.insert("upload".into(), json!((now_ms() - m.captured_at).max(0)));
    let mut base = json!({
        "clientRouteStepId": m.client_route_step_id, "engine": app.pipeline.engine, "framesSent": frames.len(),
    });
    let allowed: Vec<&String> = path
        .iter()
        .skip_while(|s| **s != step)
        .skip(1)
        .take(1)
        .collect();
    let meta = json!({
        "requestId": m.request_id, "sessionId": session_id, "destinationId": dest.destination_id,
        "routeStepId": step, "allowedNextStepIds": allowed, "stepHint": app.hint(&step),
        "frames": frames.iter().map(|(fm, _)| json!({"requestId": fm.request_id, "capturedAt": fm.captured_at})).collect::<Vec<_>>(),
    });

    let t = now_ms();
    let images = frames.into_iter().map(|(_, data)| data).collect();
    let nav = match app.pipeline.navigate(images, meta).await {
        Ok(nav) => nav,
        Err(e) => {
            let mut c = lock(cref);
            if c.generation != started_gen {
                return;
            }
            let failures = c.session.as_mut().map_or(0, |s| {
                s.nav_failures += 1;
                s.nav_failures
            });
            c.log(
                rid,
                "frame",
                merge(&base, json!({"error": format!("navigate: {e}")})),
            );
            if failures >= NAV_FAILURES_BEFORE_ERROR {
                let data = json!({"code": "upstream_unavailable", "stage": "navigate", "text": UNAVAILABLE, "retryable": true});
                c.halt("error", rid, data);
            }
            return;
        }
    };
    timings.insert("navigate".into(), json!(now_ms() - t));
    base = merge(
        &base,
        json!({"confidence": nav["confidence"], "observation": nav["observation"]}),
    );
    let output = app.pipeline.validate(&nav, &step, path);

    let mut c = lock(cref);
    if c.generation != started_gen {
        c.log(
            rid,
            "frame",
            merge(
                &base,
                json!({"action": output.action, "dropped": "stale_generation"}),
            ),
        );
        return;
    }
    let s = c
        .session
        .as_mut()
        .expect("same generation keeps the session");
    s.nav_failures = 0;
    let now = now_ms();
    let (speak, reason) =
        app.pipeline
            .should_speak(&output, s.previous.as_ref(), s.last_spoken_at, now);
    // Jev answers typed questions but does not generate text, so the sentence is a template.
    let text = speak.then(|| {
        pipeline::template(
            &output.action,
            output.direction.as_deref(),
            output.uncertain,
            &dest.label,
        )
    });
    s.step = output.step.clone();
    s.arrived = output.action == "arrived";
    s.previous = Some(output.clone());
    if speak {
        s.last_spoken_at = now;
    }
    timings.insert("total".into(), json!(now_ms() - m.captured_at));

    if let Some(text) = &text {
        c.emit(
            "guidance",
            rid,
            json!({
                "guidanceId": uuid::Uuid::new_v4().to_string(), "text": text, "action": output.action,
                "direction": output.direction, "routeStepId": step, "nextRouteStepId": output.step,
                "uncertain": output.uncertain, "debug": {"engine": app.pipeline.engine, "timingsMs": timings},
            }),
        );
    } else {
        let hb = c.heartbeat(rid, Some(reason));
        c.emit("heartbeat", rid, hb);
    }
    c.log(
        rid,
        "frame",
        merge(
            &base,
            json!({"action": output.action, "spoke": speak, "text": text, "timingsMs": timings}),
        ),
    );
}

fn router(app: App) -> Router {
    let origins = env("ALLOW_ORIGINS", "*");
    let allow_origin = if origins == "*" {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(
            origins
                .split(',')
                .map(|o| HeaderValue::from_str(o.trim()).expect("ALLOW_ORIGINS")),
        )
    };
    let cors = CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]);
    let mut routes = Router::new();
    if std::env::var("DEBUG_PAGE").is_ok() {
        // Local debugging only: drives the API and shows events and the trace live. See examples/fakes.rs.
        routes = routes.route("/debug", get(|| async { Html(include_str!("debug.html")) }));
    }
    routes
        .route("/v1/health", get(health))
        .route("/v1/clients", post(create_client))
        .route("/v1/clients/{id}/events", get(events))
        .route("/v1/clients/{id}/utterances", post(utterance))
        .route("/v1/clients/{id}/frames", post(frames))
        .route("/v1/clients/{id}/stop", post(stop))
        .route("/v1/clients/{id}/retry", post(retry))
        .route("/v1/clients/{id}/trace", get(trace))
        .layer(cors)
        .with_state(app)
}

fn load_route() -> Route {
    let raw = match std::env::var("ROUTE_PATH") {
        Ok(path) => std::fs::read_to_string(path).expect("ROUTE_PATH"),
        Err(_) => include_str!("route.json").to_string(),
    };
    serde_json::from_str(&raw).expect("route definition")
}

#[tokio::main]
async fn main() {
    let addr = env("BIND", "0.0.0.0:8000");
    let app = Arc::new(AppState::new(load_route(), Pipeline::from_env()));
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    println!("orient-orchestrator listening on {addr}");
    axum::serve(listener, router(app)).await.expect("serve");
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::multipart::{Form, Part};

    #[derive(Default)]
    struct FakeNav {
        answer: Value,
        delay_ms: u64,
        frames_seen: usize,
    }

    struct Harness {
        base: String,
        http: reqwest::Client,
        app: App,
        nav: Arc<Mutex<FakeNav>>,
    }

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{addr}")
    }

    async fn harness() -> Harness {
        let nav = Arc::new(Mutex::new(FakeNav::default()));
        let fake = nav.clone();
        let fake_vla = Router::new().route(
            "/v1/navigate",
            post(move |mut mp: Multipart| {
                let fake = fake.clone();
                async move {
                    let mut frames = 0;
                    while let Some(f) = mp.next_field().await.unwrap() {
                        frames += usize::from(f.name() == Some("frames"));
                    }
                    let (answer, delay) = {
                        let mut n = fake.lock().unwrap();
                        n.frames_seen = frames;
                        (n.answer.clone(), n.delay_ms)
                    };
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    Json(answer)
                }
            }),
        );
        // Fake STT: the "audio" bytes are the transcript.
        let fake_stt = Router::new().route(
            "/stt",
            post(|mut mp: Multipart| async move {
                let audio = mp
                    .next_field()
                    .await
                    .unwrap()
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                Json(json!({"transcript": String::from_utf8_lossy(&audio)}))
            }),
        );
        let mut pipeline = Pipeline::from_env();
        pipeline.nav_url = serve(fake_vla).await;
        pipeline.stt_url = format!("{}/stt", serve(fake_stt).await);
        pipeline.jev_api_key = String::new();
        let app = Arc::new(AppState::new(load_route(), pipeline));
        Harness {
            base: serve(router(app.clone())).await,
            http: reqwest::Client::new(),
            app,
            nav,
        }
    }

    fn meta(rid: &str, generation: i64, sequence: i64, captured_at: i64) -> String {
        json!({"requestId": rid, "generation": generation, "sequence": sequence, "capturedAt": captured_at}).to_string()
    }

    fn jpeg() -> Part {
        Part::bytes(vec![0xff, 0xd8, 0xff])
            .file_name("f.jpg")
            .mime_str("image/jpeg")
            .unwrap()
    }

    fn audio(text: &str) -> Part {
        Part::bytes(text.as_bytes().to_vec())
            .file_name("a.webm")
            .mime_str("audio/webm")
            .unwrap()
    }

    async fn next(rx: &mut broadcast::Receiver<Value>) -> Value {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("event")
            .unwrap()
    }

    #[tokio::test]
    async fn sessions_frames_worker_and_stop() {
        let h = harness().await;
        let c: Value = h
            .http
            .post(format!("{}/v1/clients", h.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let (cid, token) = (
            c["clientId"].as_str().unwrap(),
            c["clientToken"].as_str().unwrap(),
        );
        let url = |p: &str| format!("{}/v1/clients/{cid}/{p}", h.base);
        let mut rx = h.app.clients.lock().unwrap()[cid]
            .lock()
            .unwrap()
            .events
            .subscribe();
        let post_frame = |rid: &str, generation: i64, seq: i64, at: i64| {
            let form = Form::new()
                .text("meta", meta(rid, generation, seq, at))
                .part("frame", jpeg());
            h.http
                .post(url("frames"))
                .bearer_auth(token)
                .multipart(form)
                .send()
        };
        let say = |rid: &str, generation: i64, seq: i64, text: &str| {
            let form = Form::new()
                .text("meta", meta(rid, generation, seq, now_ms()))
                .part("audio", audio(text));
            h.http
                .post(url("utterances"))
                .bearer_auth(token)
                .multipart(form)
                .send()
        };
        let set_nav = |answer: Value, delay_ms: u64| {
            let mut n = h.nav.lock().unwrap();
            n.answer = answer;
            n.delay_ms = delay_ms;
        };
        let frames_seen = || h.nav.lock().unwrap().frames_seen;

        // No token: 401. Transcript instead of audio: 400.
        assert_eq!(h.http.get(url("trace")).send().await.unwrap().status(), 401);
        let form = Form::new()
            .text("meta", meta("t0", 1, 0, now_ms()))
            .text("transcript", "coffee");
        let r = h
            .http
            .post(url("utterances"))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 400);

        // Audio with the first frame starts session 1.
        set_nav(
            json!({"action": "continue", "proposedNextStepId": "corridor", "confidence": 0.9}),
            0,
        );
        let form = Form::new()
            .text("meta", meta("u1", 1, 0, now_ms()))
            .part("audio", audio("take me to the coffee"))
            .part("frame", jpeg());
        let r = h
            .http
            .post(url("utterances"))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202);
        let ev = next(&mut rx).await;
        assert_eq!(
            (ev["type"].as_str(), ev["phase"].as_str()),
            (Some("state"), Some("navigating"))
        );
        let session1 = ev["sessionId"].as_str().unwrap().to_string();
        let ev = next(&mut rx).await;
        assert_eq!(
            (ev["type"].as_str(), ev["text"].as_str()),
            (Some("guidance"), Some("Keep going straight."))
        );
        assert_eq!(frames_seen(), 1);

        // Replays, old sequences and old captures are rejected without side effects.
        assert_eq!(say("u1", 2, 0, "bathroom").await.unwrap().status(), 202);
        assert_eq!(
            post_frame("f0", 2, 0, now_ms()).await.unwrap().status(),
            409
        );
        assert_eq!(
            post_frame("f1", 2, 1, now_ms() - 60_000)
                .await
                .unwrap()
                .status(),
            422
        );

        // Same output as before: the worker stays quiet. The buffer caps at 5 frames.
        for seq in 2..8 {
            assert_eq!(
                post_frame(&format!("f{seq}"), 2, seq, now_ms())
                    .await
                    .unwrap()
                    .status(),
                202
            );
            let ev = next(&mut rx).await;
            assert_eq!(
                (ev["type"].as_str(), ev["quietReason"].as_str()),
                (Some("heartbeat"), Some("unchanged"))
            );
        }
        assert_eq!(frames_seen(), 5);

        // A different output is spoken.
        set_nav(
            json!({"action": "turn", "direction": "left", "proposedNextStepId": "corridor", "confidence": 0.9}),
            0,
        );
        assert_eq!(
            post_frame("f8", 2, 8, now_ms()).await.unwrap().status(),
            202
        );
        assert_eq!(next(&mut rx).await["text"], "Turn left.");

        // Same action again: same session, same generation.
        assert_eq!(
            say("u2", 2, 9, "the coffee please").await.unwrap().status(),
            202
        );
        let ev = next(&mut rx).await;
        assert_eq!(
            (ev["sessionId"].as_str(), ev["generation"].as_i64()),
            (Some(session1.as_str()), Some(2))
        );

        // Different action: new session, new generation, empty buffer and no previous output.
        assert_eq!(
            say("u3", 2, 10, "where is the toilet")
                .await
                .unwrap()
                .status(),
            202
        );
        let ev = next(&mut rx).await;
        let session2 = ev["sessionId"].as_str().unwrap().to_string();
        assert_ne!(session2, session1);
        assert_eq!(
            (ev["generation"].as_i64(), ev["destinationId"].as_str()),
            (Some(3), Some("bathroom"))
        );
        assert_eq!(
            post_frame("f11", 2, 11, now_ms()).await.unwrap().status(),
            409
        );
        assert_eq!(
            post_frame("f12", 3, 12, now_ms()).await.unwrap().status(),
            202
        );
        assert_eq!(next(&mut rx).await["text"], "Turn left.");
        assert_eq!(frames_seen(), 1);

        // Stop while the navigation model is still thinking: the late answer is never emitted.
        set_nav(
            json!({"action": "turn", "direction": "right", "proposedNextStepId": "bathroom", "confidence": 0.9}),
            300,
        );
        assert_eq!(
            post_frame("f13", 3, 13, now_ms()).await.unwrap().status(),
            202
        );
        let r: Value = h
            .http
            .post(url("stop"))
            .bearer_auth(token)
            .json(&json!({"requestId": "s1", "generation": 3}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            (r["generation"].as_i64(), r["phase"].as_str()),
            (Some(4), Some("stopped"))
        );
        assert_eq!(next(&mut rx).await["type"], "stop");
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(rx.try_recv().is_err(), "no event after stop");

        // Retry from the last confirmed step keeps the session and its step.
        let r: Value = h
            .http
            .post(url("retry"))
            .bearer_auth(token)
            .json(&json!({"requestId": "r1", "generation": 4, "from": "last_confirmed_step"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            (
                r["phase"].as_str(),
                r["sessionId"].as_str(),
                r["routeStepId"].as_str(),
                r["generation"].as_i64()
            ),
            (
                Some("navigating"),
                Some(session2.as_str()),
                Some("corridor"),
                Some(5)
            )
        );

        // Trace is readable and holds no media or token.
        let t = h
            .http
            .get(url("trace"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(t.contains("\"framesSent\":5") && !t.contains(token));
    }
}
