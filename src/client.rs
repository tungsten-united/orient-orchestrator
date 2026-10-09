//! Clients, sessions, the API operations and the worker. No HTTP framework here:
//! `server` and `cloudflare` parse requests, call these, and write the responses.
//!
//! A client is one phone from Start to Stop: it holds the token and the event stream.
//! A session is one spoken action (go to a destination). When speech-to-text yields a
//! different action, a new session starts with an empty frame buffer and no previous output.

use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures::future::Either;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::pipeline::{self, Audio, Command, Destination, Output, Pipeline, Route, Vars, var};

const AUDIO_TYPES: [&str; 2] = ["audio/webm", "audio/mp4"];
const NAV_FAILURES_BEFORE_ERROR: u32 = 3;
const UNAVAILABLE: &str = "Guidance is unavailable. Double tap to try again.";
const MAX_SPEECH_CHARS: usize = 240;
// ponytail: speech cache capped by entry count, never evicted. Templates and prompts are a small fixed set.
const SPEECH_CACHE_ENTRIES: usize = 256;

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

async fn sleep(d: Duration) {
    tokio::time::sleep(d).await
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
pub struct Limits {
    max_audio_ms: i64, // enforced by the phone; the server only checks bytes
    pub max_audio_bytes: usize,
    pub max_frame_bytes: usize,
    max_frame_edge_px: i64, // enforced by the phone
    max_input_age_ms: i64,
    heartbeat_ms: u64,
    nav_frames: usize,
}

impl Limits {
    pub fn from_vars(get: Vars) -> Self {
        Limits {
            max_audio_ms: 10_000,
            max_audio_bytes: 1_000_000,
            max_frame_bytes: 512_000,
            max_frame_edge_px: 1280,
            max_input_age_ms: var(get, "MAX_INPUT_AGE_MS", "3000")
                .parse()
                .expect("MAX_INPUT_AGE_MS"),
            heartbeat_ms: 5000,
            nav_frames: var(get, "NAV_FRAMES", "5").parse().expect("NAV_FRAMES"),
        }
    }
}

pub struct AppState {
    pub route: Route,
    pub pipeline: Pipeline,
    pub limits: Limits,
    trace_path: Option<String>,
    trace_stdout: bool,
    pub debug_page: bool,
    pub commit: Option<String>, // GIT_SHA, set by the deploy
    speech_cache: Mutex<HashMap<String, bytes::Bytes>>, // text -> complete MP3
}

pub type App = Arc<AppState>;
pub type ClientRef = Arc<Mutex<Client>>;

impl AppState {
    pub fn new(route: Route, pipeline: Pipeline, get: Vars) -> Self {
        AppState {
            route,
            pipeline,
            limits: Limits::from_vars(get),
            trace_path: get("TRACE_PATH"),
            trace_stdout: get("TRACE_STDOUT").is_some(),
            debug_page: get("DEBUG_PAGE").is_some(),
            commit: get("GIT_SHA"),
            speech_cache: Mutex::default(),
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

/// The open event streams of one client.
// ponytail: unbounded queues, events are small and streams are few; bound them if a slow reader shows up.
#[derive(Default)]
pub struct Events(Vec<UnboundedSender<Value>>);

impl Events {
    fn subscribe(&mut self) -> UnboundedReceiver<Value> {
        let (tx, rx) = unbounded();
        self.0.push(tx);
        rx
    }

    /// Sends to every open stream and forgets the closed ones.
    fn send(&mut self, ev: Value) {
        self.0.retain(|tx| tx.unbounded_send(ev.clone()).is_ok());
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// One phone from Start to Stop.
pub struct Client {
    id: String,
    token: String,
    generation: i64,
    stopped: bool,
    sequence: i64,
    responses: HashMap<String, Value>, // requestId -> response body, for idempotent replays
    events: Events,
    worker: Option<i64>, // generation of the running worker
    session: Option<Session>,
    entries: Vec<Value>,
    trace_path: Option<String>,
    trace_stdout: bool,
    stamp: Value, // which app produced the trace: commit, version, route
}

pub fn lock(c: &ClientRef) -> MutexGuard<'_, Client> {
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

    fn emit(&mut self, kind: &str, request_id: Option<&str>, data: Value) {
        let ev = self.event(kind, request_id, data);
        self.events.send(ev);
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
    // ponytail: in-flight model calls are not aborted, they finish and their results are dropped.
    fn reset(&mut self) {
        self.generation += 1;
        self.worker = None;
        if let Some(s) = &mut self.session {
            s.pending = None;
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
            merge(&self.stamp, fields),
        );
        if self.trace_stdout {
            println!("{entry}"); // one JSON line: Cloud Logging stores it as a structured entry
        }
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
        c.worker = Some(c.generation);
        spawn(worker(app.clone(), cref.clone(), c.generation));
    }
}

pub struct ApiError {
    pub status: u16,
    code: &'static str,
    message: String,
    retryable: bool,
}

impl ApiError {
    pub fn body(&self) -> Value {
        json!({"error": {"code": self.code, "message": self.message, "retryable": self.retryable}})
    }
}

pub fn api_error(status: u16, code: &'static str, message: impl Into<String>) -> ApiError {
    ApiError {
        status,
        code,
        message: message.into(),
        retryable: false,
    }
}

pub fn bad_request(message: impl ToString) -> ApiError {
    api_error(400, "bad_request", message.to_string())
}

pub type ApiResult<T> = Result<T, ApiError>;

/// A new client with its token. The response body is the contract's `POST /v1/clients` reply.
pub fn create_client(app: &App, id: String) -> (ClientRef, Value) {
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
        events: Events::default(),
        worker: None,
        session: None,
        entries: Vec::new(),
        trace_path: app.trace_path.clone(),
        trace_stdout: app.trace_stdout,
        stamp: json!({
            "commit": app.commit, "version": env!("CARGO_PKG_VERSION"),
            "routeId": app.route.route_id,
        }),
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
    (Arc::new(Mutex::new(c)), response)
}

pub fn client_not_found() -> ApiError {
    api_error(404, "client_not_found", "Unknown client.")
}

pub fn authorize(cref: &ClientRef, token: &str) -> ApiResult<()> {
    let expected = lock(cref).token.clone();
    let same = token.len() == expected.len()
        && token
            .bytes()
            .zip(expected.bytes())
            .fold(0, |acc, (a, b)| acc | (a ^ b))
            == 0; // constant time
    if !same {
        return Err(api_error(401, "unauthorized", "Missing or wrong token."));
    }
    Ok(())
}

fn accept(app: &App, c: &mut Client, m: &Meta, kind: &str) -> ApiResult<()> {
    if m.generation != c.generation {
        return Err(api_error(
            409,
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
            409,
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
        let mut e = api_error(422, "expired_input", "Input is too old.");
        e.retryable = true;
        return Err(e);
    }
    c.sequence = m.sequence;
    Ok(())
}

/// The multipart parts of an utterance or frame upload, each already checked with `check_part`.
#[derive(Default)]
pub struct Parts {
    pub meta: Option<String>,
    pub audio: Option<(Vec<u8>, String)>,
    pub frame: Option<Vec<u8>>,
}

/// Media type and size of one uploaded part. Unknown part names are ignored by the callers.
pub fn check_part(limits: &Limits, name: &str, content_type: &str, len: usize) -> ApiResult<()> {
    let (types, limit): (&[&str], usize) = match name {
        "audio" => (&AUDIO_TYPES, limits.max_audio_bytes),
        "frame" => (&["image/jpeg"], limits.max_frame_bytes),
        _ => return Ok(()),
    };
    let content_type = content_type.split(';').next().unwrap_or("").trim();
    if !types.contains(&content_type) {
        let msg = format!("{name}: expected one of {types:?}.");
        return Err(api_error(415, "unsupported_media_type", msg));
    }
    if len > limit {
        let msg = format!("{name}: larger than {limit} bytes.");
        return Err(api_error(413, "payload_too_large", msg));
    }
    Ok(())
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

/// The SSE body: a state snapshot first, then every event, with a heartbeat when quiet.
pub fn events(app: &App, cref: &ClientRef) -> impl Stream<Item = String> + use<> {
    let (rx, first) = {
        let mut c = lock(cref);
        let first = c.event("state", None, c.state());
        (c.events.subscribe(), first)
    };
    let heartbeat = Duration::from_millis(app.limits.heartbeat_ms);
    let cref = cref.clone();
    futures::stream::unfold((rx, Some(first)), move |(mut rx, first)| {
        let cref = cref.clone();
        async move {
            let ev = match first {
                Some(ev) => ev,
                None => match futures::future::select(rx.next(), std::pin::pin!(sleep(heartbeat)))
                    .await
                {
                    Either::Left((Some(ev), _)) => ev,
                    Either::Left((None, _)) => return None,
                    Either::Right(_) => {
                        let c = lock(&cref);
                        c.event("heartbeat", None, c.heartbeat(None, None))
                    }
                },
            };
            Some((format!("data: {ev}\n\n"), (rx, None)))
        }
    })
}

pub fn utterance(app: &App, cref: &ClientRef, parts: Parts) -> ApiResult<Value> {
    let m = parse_meta(parts.meta)?;
    let mut c = lock(cref);
    if let Some(r) = c.responses.get(&m.request_id) {
        return Ok(r.clone());
    }
    let audio = parts.audio.ok_or_else(|| bad_request("audio: missing"))?;
    if !app.pipeline.speech_configured() {
        let mut e = api_error(
            503,
            "upstream_unavailable",
            "Speech to text is not configured.",
        );
        e.retryable = true;
        return Err(e);
    }
    accept(app, &mut c, &m, "utterance")?;
    spawn(handle_utterance(
        app.clone(),
        cref.clone(),
        m.clone(),
        audio,
        parts.frame,
    ));
    Ok(c.accepted(&m.request_id))
}

pub fn frames(app: &App, cref: &ClientRef, parts: Parts) -> ApiResult<Value> {
    let m = parse_meta(parts.meta)?;
    let frame = parts.frame.ok_or_else(|| bad_request("frame: missing"))?;
    let mut c = lock(cref);
    if let Some(r) = c.responses.get(&m.request_id) {
        return Ok(r.clone());
    }
    if c.phase() != "navigating" {
        return Err(api_error(
            409,
            "not_navigating",
            format!("Client is {}.", c.phase()),
        ));
    }
    accept(app, &mut c, &m, "frame")?;
    let request_id = m.request_id.clone();
    submit_frame(app, cref, &mut c, m, frame);
    Ok(c.accepted(&request_id))
}

/// Stop always wins: any generation is accepted.
pub fn stop(cref: &ClientRef, body: &[u8]) -> ApiResult<Value> {
    let b: StopBody = serde_json::from_slice(body).map_err(bad_request)?;
    let mut c = lock(cref);
    if !c.responses.contains_key(&b.request_id) {
        c.halt("stop", Some(&b.request_id), json!({"reason": "user_stop"}));
        c.log(Some(&b.request_id), "stop", json!({}));
        let r = merge(
            &json!({"clientId": c.id, "generation": c.generation}),
            c.state(),
        );
        c.responses.insert(b.request_id.clone(), r);
    }
    Ok(c.responses[&b.request_id].clone())
}

/// Like stop, accepts any generation: the user asked for it, and errors bump the generation.
pub fn retry(cref: &ClientRef, body: &[u8]) -> ApiResult<Value> {
    let b: RetryBody = serde_json::from_slice(body).map_err(bad_request)?;
    let mut c = lock(cref);
    if let Some(r) = c.responses.get(&b.request_id) {
        return Ok(r.clone());
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
    Ok(r)
}

/// Text to speech for anything the phone says. From the cache, or streamed from ElevenLabs and
/// cached once the stream completes. Any failure is a 503: the phone then uses browser TTS.
pub async fn speech(app: &App, text: String) -> ApiResult<Audio> {
    if text.is_empty() || text.chars().count() > MAX_SPEECH_CHARS {
        return Err(bad_request(format!(
            "text: 1 to {MAX_SPEECH_CHARS} characters."
        )));
    }
    if let Some(mp3) = app.speech_cache.lock().unwrap().get(&text) {
        return Ok(futures::stream::iter([Ok(mp3.clone())]).boxed());
    }
    let upstream = app.pipeline.speak(&text).await.map_err(|e| {
        let mut err = api_error(503, "upstream_unavailable", format!("tts: {e}"));
        err.retryable = true;
        err
    })?;
    let app = app.clone();
    let stream = futures::stream::unfold(
        (upstream, Vec::new(), Some(text)),
        move |(mut upstream, mut mp3, text)| {
            let app = app.clone();
            async move {
                match upstream.next().await {
                    Some(Ok(chunk)) => {
                        mp3.extend_from_slice(&chunk);
                        Some((Ok(chunk), (upstream, mp3, text)))
                    }
                    Some(Err(e)) => Some((Err(e), (upstream, mp3, None))), // never cache a broken stream
                    None => {
                        let mut cache = app.speech_cache.lock().unwrap();
                        if let Some(text) = text
                            && cache.len() < SPEECH_CACHE_ENTRIES
                        {
                            cache.insert(text, mp3.into());
                        }
                        None
                    }
                }
            }
        },
    );
    Ok(stream.boxed())
}

pub fn trace(cref: &ClientRef) -> Value {
    let c = lock(cref);
    json!({"clientId": c.id, "entries": c.entries})
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

/// One worker per client and generation: evaluates the newest frame against the session's buffer.
/// A worker from an older generation stops at its next turn and leaves the slot to the new one.
async fn worker(app: App, cref: ClientRef, generation: i64) {
    loop {
        let job = {
            let mut c = lock(&cref);
            let pending = if c.generation == generation {
                c.session.as_mut().and_then(|s| s.pending.take())
            } else {
                None
            };
            match pending {
                Some(m) => m,
                None => {
                    // Same lock as the check, so a frame submitted now always finds the worker slot empty.
                    if c.worker == Some(generation) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn should_stamp_every_trace_entry_with_the_app_version() {
        let get = |k: &str| (k == "GIT_SHA").then(|| "abc123".to_string());
        let route: Route = serde_json::from_str(include_str!("route.json")).unwrap();
        let app = Arc::new(AppState::new(route, Pipeline::from_vars(&get), &get));
        let (cref, _) = create_client(&app, "c1".into());
        let entry = &trace(&cref)["entries"][0];
        assert_eq!(
            (&entry["commit"], &entry["version"], &entry["routeId"]),
            (
                &json!("abc123"),
                &json!(env!("CARGO_PKG_VERSION")),
                &json!(app.route.route_id)
            )
        );
    }

    #[test]
    fn should_send_to_every_subscriber_and_forget_closed_ones() {
        let mut events = Events::default();
        let mut a = events.subscribe();
        let b = events.subscribe();
        drop(b);
        events.send(json!({"type": "state"}));
        assert_eq!(events.len(), 1);
        let got = futures::executor::block_on(a.next()).unwrap();
        assert_eq!(got["type"], "state");
    }

    #[test]
    fn should_reject_parts_with_wrong_type_or_size() {
        let limits = Limits::from_vars(&|_| None);
        assert!(check_part(&limits, "frame", "image/jpeg", 10).is_ok());
        assert_eq!(
            check_part(&limits, "frame", "image/png", 10)
                .unwrap_err()
                .status,
            415
        );
        assert_eq!(
            check_part(&limits, "audio", "audio/webm", 2_000_000)
                .unwrap_err()
                .status,
            413
        );
    }
}
