//! Orient orchestrator: the only service the phone talks to.
//!
//! Implements docs/contracts.md from tungsten-united/project-description.

mod pipeline;

use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::rejection::JsonRejection;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::task::AbortHandle;
use tower_http::cors::{AllowOrigin, CorsLayer};

use pipeline::{Command, Destination, Pipeline, Route, Spoken, env};

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
}

struct AppState {
    route: Route,
    pipeline: Pipeline,
    limits: Limits,
    trace_path: Option<String>,
    // ponytail: in-memory sessions in one process, never expired. Fine for one demo phone; add a store if we scale out.
    sessions: Mutex<HashMap<String, SessionRef>>,
}

type App = Arc<AppState>;
type SessionRef = Arc<Mutex<Session>>;

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
            },
            trace_path: std::env::var("TRACE_PATH").ok(),
            sessions: Mutex::default(),
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

struct Pending {
    meta: Meta,
    frame: Vec<u8>,
    received: i64,
}

struct Session {
    id: String,
    token: String,
    generation: i64,
    phase: &'static str,
    step: String,
    destination: Option<Destination>,
    sequence: i64,
    responses: HashMap<String, Value>, // requestId -> response body, for idempotent replays
    events: broadcast::Sender<Value>,
    tasks: Vec<AbortHandle>,
    pending: Option<Pending>, // latest frame waiting for the navigation loop
    nav_task: Option<tokio::task::Id>,
    nav_failures: u32,
    last_spoken: Option<Spoken>,
    entries: Vec<Value>,
    trace_path: Option<String>,
}

fn lock(s: &SessionRef) -> MutexGuard<'_, Session> {
    s.lock().unwrap()
}

impl Session {
    fn event(&self, kind: &str, request_id: Option<&str>, data: Value) -> Value {
        merge(
            &json!({
                "type": kind, "eventId": uuid::Uuid::new_v4().to_string(), "sessionId": self.id,
                "generation": self.generation, "requestId": request_id, "emittedAt": now_ms(),
            }),
            data,
        )
    }

    fn emit(&self, kind: &str, request_id: Option<&str>, data: Value) {
        let _ = self.events.send(self.event(kind, request_id, data)); // Err only means nobody is listening
    }

    fn state(&self) -> Value {
        json!({
            "phase": self.phase, "routeStepId": self.step,
            "destinationId": self.destination.as_ref().map(|d| &d.destination_id),
        })
    }

    fn heartbeat(&self, last_request_id: Option<&str>, quiet_reason: Option<&str>) -> Value {
        json!({"phase": self.phase, "routeStepId": self.step, "lastRequestId": last_request_id, "quietReason": quiet_reason})
    }

    /// Invalidate everything in flight: late results see a newer generation and are dropped.
    fn reset(&mut self) {
        self.generation += 1;
        self.pending = None;
        self.nav_task = None;
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
        self.phase = "stopped";
    }

    fn log(&mut self, request_id: Option<&str>, kind: &str, fields: Value) {
        let entry = merge(
            &json!({
                "at": now_ms(), "sessionId": self.id, "generation": self.generation, "requestId": request_id,
                "kind": kind, "clientRouteStepId": null, "routeStepId": self.step,
                "destinationId": self.destination.as_ref().map(|d| &d.destination_id),
                "transcript": null, "command": null, "engine": null, "action": null, "confidence": null,
                "observation": null, "spoke": false, "text": null, "timingsMs": {}, "dropped": null, "error": null,
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

/// Latest frame wins: a frame still waiting is replaced, never queued.
fn submit_frame(app: &App, sref: &SessionRef, s: &mut Session, meta: Meta, frame: Vec<u8>) {
    if let Some(old) = s.pending.take() {
        s.log(
            Some(&old.meta.request_id),
            "frame",
            json!({"dropped": "superseded"}),
        );
    }
    s.pending = Some(Pending {
        meta,
        frame,
        received: now_ms(),
    });
    if s.nav_task.is_none() {
        s.nav_task = Some(s.spawn(nav_loop(app.clone(), sref.clone())));
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

fn get_session(app: &App, id: &str, token: &str) -> ApiResult<SessionRef> {
    let sref = app.sessions.lock().unwrap().get(id).cloned();
    let sref = sref.ok_or_else(|| {
        api_error(
            StatusCode::NOT_FOUND,
            "session_not_found",
            "Unknown session.",
        )
    })?;
    let expected = lock(&sref).token.clone();
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
    Ok(sref)
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

fn accept(app: &App, s: &mut Session, m: &Meta, kind: &str) -> ApiResult<()> {
    if m.generation != s.generation {
        return Err(api_error(
            StatusCode::CONFLICT,
            "stale_generation",
            "Session was stopped or restarted.",
        ));
    }
    if m.sequence <= s.sequence {
        s.log(
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
        s.log(
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
    s.sequence = m.sequence;
    Ok(())
}

#[derive(Default)]
struct Parts {
    meta: Option<String>,
    transcript: Option<String>,
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
            "transcript" => {
                p.transcript = Some(field.text().await.map_err(mp_error)?);
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

async fn create_session(State(app): State<App>) -> (StatusCode, Json<Value>) {
    let id = uuid::Uuid::new_v4().to_string();
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let mut s = Session {
        id: id.clone(),
        token: token.clone(),
        generation: 1,
        phase: "awaiting_destination",
        step: app.route.start_step_id.clone(),
        destination: None,
        sequence: -1,
        responses: HashMap::new(),
        events: broadcast::channel(64).0,
        tasks: Vec::new(),
        pending: None,
        nav_task: None,
        nav_failures: 0,
        last_spoken: None,
        entries: Vec::new(),
        trace_path: app.trace_path.clone(),
    };
    s.log(None, "session", json!({}));
    let destinations: Vec<Value> = app
        .route
        .destinations
        .iter()
        .map(|d| json!({"destinationId": d.destination_id, "label": d.label}))
        .collect();
    let response = json!({
        "sessionId": id,
        "sessionToken": token,
        "generation": s.generation,
        "serverTime": now_ms(),
        "phase": s.phase,
        "route": {"routeId": app.route.route_id, "startStepId": app.route.start_step_id, "destinations": destinations},
        "limits": app.limits,
    });
    app.sessions
        .lock()
        .unwrap()
        .insert(id, Arc::new(Mutex::new(s)));
    (StatusCode::CREATED, Json(response))
}

async fn events(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<impl IntoResponse> {
    let sref = get_session(&app, &id, q.get("token").map_or("", String::as_str))?;
    let (rx, first) = {
        let s = lock(&sref);
        (s.events.subscribe(), s.event("state", None, s.state()))
    };
    let heartbeat = Duration::from_millis(app.limits.heartbeat_ms);
    let stream = futures::stream::unfold((rx, Some(first)), move |(mut rx, first)| {
        let sref = sref.clone();
        async move {
            let ev = match first {
                Some(ev) => ev,
                None => match tokio::time::timeout(heartbeat, rx.recv()).await {
                    Ok(Ok(ev)) => ev,
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                        let s = lock(&sref);
                        s.event("state", None, s.state()) // fell behind: resync from a snapshot
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => return None,
                    Err(_) => {
                        let s = lock(&sref);
                        s.event("heartbeat", None, s.heartbeat(None, None))
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
    let sref = get_session(&app, &id, bearer(&headers))?;
    let parts = read_parts(&app, mp).await?;
    let m = parse_meta(parts.meta)?;
    let mut s = lock(&sref);
    if let Some(r) = s.responses.get(&m.request_id) {
        return Ok((StatusCode::ACCEPTED, Json(r.clone())));
    }
    if parts.audio.is_some() == parts.transcript.is_some() {
        return Err(bad_request("Send exactly one of audio or transcript."));
    }
    if parts.audio.is_some() && app.pipeline.stt_url.is_empty() {
        let mut e = api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "upstream_unavailable",
            "Speech to text is not configured; send a transcript.",
        );
        e.retryable = true;
        return Err(e);
    }
    accept(&app, &mut s, &m, "utterance")?;
    s.spawn(handle_utterance(
        app.clone(),
        sref.clone(),
        m.clone(),
        parts.transcript,
        parts.audio,
        parts.frame,
    ));
    Ok((StatusCode::ACCEPTED, Json(s.accepted(&m.request_id))))
}

async fn frames(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    mp: Multipart,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let sref = get_session(&app, &id, bearer(&headers))?;
    let parts = read_parts(&app, mp).await?;
    let m = parse_meta(parts.meta)?;
    let frame = parts.frame.ok_or_else(|| bad_request("frame: missing"))?;
    let mut s = lock(&sref);
    if let Some(r) = s.responses.get(&m.request_id) {
        return Ok((StatusCode::ACCEPTED, Json(r.clone())));
    }
    if s.phase != "navigating" {
        return Err(api_error(
            StatusCode::CONFLICT,
            "not_navigating",
            format!("Session is {}.", s.phase),
        ));
    }
    accept(&app, &mut s, &m, "frame")?;
    let request_id = m.request_id.clone();
    submit_frame(&app, &sref, &mut s, m, frame);
    Ok((StatusCode::ACCEPTED, Json(s.accepted(&request_id))))
}

/// Stop always wins: any generation is accepted.
async fn stop(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    b: Result<Json<StopBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let sref = get_session(&app, &id, bearer(&headers))?;
    let b = body(b)?;
    let mut s = lock(&sref);
    if !s.responses.contains_key(&b.request_id) {
        s.halt("stop", Some(&b.request_id), json!({"reason": "user_stop"}));
        s.log(Some(&b.request_id), "stop", json!({}));
        let r = json!({"sessionId": s.id, "generation": s.generation, "phase": s.phase});
        s.responses.insert(b.request_id.clone(), r);
    }
    Ok(Json(s.responses[&b.request_id].clone()))
}

/// Like stop, accepts any generation: the user asked for it, and errors bump the generation.
async fn retry(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
    b: Result<Json<RetryBody>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let sref = get_session(&app, &id, bearer(&headers))?;
    let b = body(b)?;
    let mut s = lock(&sref);
    if let Some(r) = s.responses.get(&b.request_id) {
        return Ok(Json(r.clone()));
    }
    if b.from == RetryFrom::LastConfirmedStep && s.destination.is_none() {
        return Err(bad_request("No confirmed step to resume from."));
    }
    s.reset();
    s.nav_failures = 0;
    s.last_spoken = None;
    if b.from == RetryFrom::DestinationPrompt {
        s.phase = "awaiting_destination";
        s.step = app.route.start_step_id.clone();
        s.destination = None;
    } else {
        s.phase = "navigating";
    }
    let state = s.state();
    s.emit("state", Some(&b.request_id), state.clone());
    s.log(Some(&b.request_id), "retry", json!({}));
    let r = merge(
        &json!({"sessionId": s.id, "generation": s.generation}),
        state,
    );
    s.responses.insert(b.request_id, r.clone());
    Ok(Json(r))
}

async fn trace(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let sref = get_session(&app, &id, bearer(&headers))?;
    let s = lock(&sref);
    Ok(Json(json!({"sessionId": s.id, "entries": s.entries})))
}

async fn handle_utterance(
    app: App,
    sref: SessionRef,
    m: Meta,
    transcript: Option<String>,
    audio: Option<(Vec<u8>, String)>,
    frame: Option<Vec<u8>>,
) {
    let (started_gen, phase) = {
        let s = lock(&sref);
        (s.generation, s.phase)
    };
    let rid = Some(m.request_id.as_str());
    let mut timings = serde_json::Map::new();
    let transcript = match audio {
        Some((bytes, content_type)) => {
            let t = now_ms();
            match app.pipeline.transcribe(bytes, &content_type).await {
                Ok(text) => {
                    timings.insert("stt".into(), json!(now_ms() - t));
                    text
                }
                Err(e) => {
                    let mut s = lock(&sref);
                    if s.generation == started_gen {
                        s.log(rid, "utterance", json!({"error": format!("stt: {e}")}));
                        let data = json!({"code": "upstream_unavailable", "stage": "stt", "text": UNAVAILABLE, "retryable": true});
                        s.halt("error", rid, data);
                    }
                    return;
                }
            }
        }
        None => transcript.unwrap_or_default(),
    };
    let transcript = transcript.trim().to_string();
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

    let mut s = lock(&sref);
    if s.generation != started_gen {
        s.log(
            rid,
            "utterance",
            json!({"transcript": transcript, "dropped": "stale_generation"}),
        );
        return;
    }
    s.log(
        rid,
        "utterance",
        json!({"transcript": transcript, "command": cmd.name(), "timingsMs": timings}),
    );
    match cmd {
        Command::Start(id) => {
            let dest = app.destination(&id).clone();
            if !dest.steps.contains(&s.step) {
                s.step = dest.steps[0].clone();
            }
            s.destination = Some(dest);
            s.phase = "navigating";
            s.last_spoken = None;
            s.nav_failures = 0;
            let state = s.state();
            s.emit("state", rid, state);
            if let Some(frame) = frame {
                submit_frame(&app, &sref, &mut s, m.clone(), frame);
            }
        }
        Command::Cancel => s.halt("stop", rid, json!({"reason": "voice_cancel"})),
        other => {
            let reason = other.name();
            s.emit(
                "needs_input",
                rid,
                json!({"reason": reason, "text": ask_again(&app.route, reason)}),
            );
        }
    }
}

async fn nav_loop(app: App, sref: SessionRef) {
    loop {
        let pending = {
            let mut s = lock(&sref);
            match s.pending.take() {
                Some(p) => p,
                None => {
                    // Same lock as the check, so a frame submitted now always finds nav_task empty.
                    if s.nav_task == tokio::task::try_id() {
                        s.nav_task = None;
                    }
                    return;
                }
            }
        };
        process_frame(&app, &sref, pending).await;
    }
}

async fn process_frame(app: &App, sref: &SessionRef, p: Pending) {
    let (started_gen, step, dest) = {
        let s = lock(sref);
        (s.generation, s.step.clone(), s.destination.clone())
    };
    let Some(dest) = dest else { return };
    let path = &dest.steps;
    let rid = Some(p.meta.request_id.as_str());
    let mut timings = serde_json::Map::new();
    timings.insert(
        "upload".into(),
        json!((p.received - p.meta.captured_at).max(0)),
    );
    let mut base =
        json!({"clientRouteStepId": p.meta.client_route_step_id, "engine": app.pipeline.engine});
    let allowed: Vec<&String> = path
        .iter()
        .skip_while(|s| **s != step)
        .skip(1)
        .take(1)
        .collect();
    let meta = json!({
        "requestId": p.meta.request_id, "destinationId": dest.destination_id, "routeStepId": step,
        "allowedNextStepIds": allowed, "stepHint": app.hint(&step),
    });

    let t = now_ms();
    let nav = match app.pipeline.navigate(p.frame, meta).await {
        Ok(nav) => nav,
        Err(e) => {
            let mut s = lock(sref);
            if s.generation != started_gen {
                return;
            }
            s.nav_failures += 1;
            s.log(
                rid,
                "frame",
                merge(&base, json!({"error": format!("navigate: {e}")})),
            );
            if s.nav_failures >= NAV_FAILURES_BEFORE_ERROR {
                let data = json!({"code": "upstream_unavailable", "stage": "navigate", "text": UNAVAILABLE, "retryable": true});
                s.halt("error", rid, data);
            }
            return;
        }
    };
    timings.insert("navigate".into(), json!(now_ms() - t));
    let last = {
        let mut s = lock(sref);
        if s.generation != started_gen {
            s.log(
                rid,
                "frame",
                merge(&base, json!({"dropped": "stale_generation"})),
            );
            return;
        }
        s.nav_failures = 0;
        s.last_spoken.clone()
    };
    base = merge(
        &base,
        json!({"confidence": nav["confidence"], "observation": nav["observation"]}),
    );

    let (action, direction, new_step, uncertain) = app.pipeline.validate(&nav, &step, path);
    let t = now_ms();
    let (speak, reason) = app
        .pipeline
        .decide(&action, &new_step, uncertain, last.as_ref(), t);
    timings.insert("decide".into(), json!(now_ms() - t));
    // Jev answers typed questions but does not generate text, so the writer is a template.
    let text =
        speak.then(|| pipeline::template(&action, direction.as_deref(), uncertain, &dest.label));

    let mut s = lock(sref);
    if s.generation != started_gen {
        s.log(
            rid,
            "frame",
            merge(
                &base,
                json!({"action": action, "dropped": "stale_generation"}),
            ),
        );
        return;
    }
    timings.insert("total".into(), json!(now_ms() - p.meta.captured_at));
    s.step = new_step.clone();
    if action == "arrived" {
        s.phase = "arrived";
    }
    if let Some(text) = &text {
        s.last_spoken = Some(Spoken {
            action: action.clone(),
            route_step_id: new_step.clone(),
            uncertain,
            at: now_ms(),
        });
        s.emit("guidance", rid, json!({
            "guidanceId": uuid::Uuid::new_v4().to_string(), "text": text, "action": action, "direction": direction,
            "routeStepId": step, "nextRouteStepId": new_step, "uncertain": uncertain,
            "debug": {"engine": app.pipeline.engine, "timingsMs": timings},
        }));
    } else {
        let hb = s.heartbeat(rid, Some(reason));
        s.emit("heartbeat", rid, hb);
    }
    s.log(
        rid,
        "frame",
        merge(
            &base,
            json!({"action": action, "spoke": speak, "text": text, "timingsMs": timings}),
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
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/sessions", post(create_session))
        .route("/v1/sessions/{id}/events", get(events))
        .route("/v1/sessions/{id}/utterances", post(utterance))
        .route("/v1/sessions/{id}/frames", post(frames))
        .route("/v1/sessions/{id}/stop", post(stop))
        .route("/v1/sessions/{id}/retry", post(retry))
        .route("/v1/sessions/{id}/trace", get(trace))
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

    struct Harness {
        base: String,
        http: reqwest::Client,
        app: App,
        nav: Arc<Mutex<(Value, u64)>>, // fake VLA answer and delay in ms
    }

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{addr}")
    }

    async fn harness() -> Harness {
        let nav: Arc<Mutex<(Value, u64)>> = Arc::new(Mutex::new((json!({}), 0)));
        let fake = nav.clone();
        let fake_vla = Router::new().route(
            "/v1/navigate",
            post(move |_: Multipart| {
                let fake = fake.clone();
                async move {
                    let (answer, delay) = fake.lock().unwrap().clone();
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    Json(answer)
                }
            }),
        );
        let mut pipeline = Pipeline::from_env();
        pipeline.nav_url = serve(fake_vla).await;
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

    async fn next(rx: &mut broadcast::Receiver<Value>) -> Value {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("event")
            .unwrap()
    }

    #[tokio::test]
    async fn full_loop_stale_inputs_and_stop() {
        let h = harness().await;
        let s: Value = h
            .http
            .post(format!("{}/v1/sessions", h.base))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let (sid, token) = (
            s["sessionId"].as_str().unwrap(),
            s["sessionToken"].as_str().unwrap(),
        );
        let url = |p: &str| format!("{}/v1/sessions/{sid}/{p}", h.base);
        let mut rx = h.app.sessions.lock().unwrap()[sid]
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

        // No token: 401.
        assert_eq!(h.http.get(url("trace")).send().await.unwrap().status(), 401);

        // Destination by voice, first frame attached.
        *h.nav.lock().unwrap() = (
            json!({"action": "continue", "proposedNextStepId": "corridor", "confidence": 0.9}),
            0,
        );
        let form = Form::new()
            .text("meta", meta("u1", 1, 0, now_ms()))
            .text("transcript", "take me to the coffee")
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
        let ev = next(&mut rx).await;
        assert_eq!(ev["type"], "guidance");
        assert_eq!(ev["text"], "Keep going straight.");
        assert_eq!(ev["nextRouteStepId"], "corridor");

        // Replayed requestId returns the original response; old sequence and old capture are rejected.
        let form = Form::new()
            .text("meta", meta("u1", 1, 0, now_ms()))
            .text("transcript", "x");
        let r = h
            .http
            .post(url("utterances"))
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202);
        assert_eq!(
            post_frame("f0", 1, 0, now_ms()).await.unwrap().status(),
            409
        );
        assert_eq!(
            post_frame("f1", 1, 1, now_ms() - 60_000)
                .await
                .unwrap()
                .status(),
            422
        );

        // The VLA proposes a step off the route: no progress, the user is told to wait.
        *h.nav.lock().unwrap() = (
            json!({"action": "arrived", "proposedNextStepId": "bathroom", "confidence": 0.9}),
            0,
        );
        assert_eq!(
            post_frame("f2", 1, 2, now_ms()).await.unwrap().status(),
            202
        );
        let ev = next(&mut rx).await;
        assert_eq!(
            (ev["action"].as_str(), ev["nextRouteStepId"].as_str()),
            (Some("wait"), Some("corridor"))
        );

        // Stop while the VLA is still thinking: the late answer is never emitted.
        *h.nav.lock().unwrap() = (
            json!({"action": "turn", "direction": "left", "proposedNextStepId": "counter", "confidence": 0.9}),
            300,
        );
        assert_eq!(
            post_frame("f3", 1, 3, now_ms()).await.unwrap().status(),
            202
        );
        let r: Value = h
            .http
            .post(url("stop"))
            .bearer_auth(token)
            .json(&json!({"requestId": "s1", "generation": 1}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            (r["generation"].as_i64(), r["phase"].as_str()),
            (Some(2), Some("stopped"))
        );
        assert_eq!(next(&mut rx).await["type"], "stop");
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(rx.try_recv().is_err(), "no event after stop");
        assert_eq!(
            post_frame("f4", 1, 4, now_ms()).await.unwrap().status(),
            409
        );

        // Retry from the last confirmed step keeps route progress.
        let r: Value = h
            .http
            .post(url("retry"))
            .bearer_auth(token)
            .json(&json!({"requestId": "r1", "generation": 2, "from": "last_confirmed_step"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            (
                r["phase"].as_str(),
                r["routeStepId"].as_str(),
                r["generation"].as_i64()
            ),
            (Some("navigating"), Some("corridor"), Some(3))
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
        assert!(t.contains("\"kind\":\"frame\"") && !t.contains(token));
    }
}
