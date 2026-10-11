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

use crate::pipeline::{
    self, Ask, Audio, Belief, Caller, Command, Destination, Graph, Output, Pipeline, Place, Vars,
    var,
};

const AUDIO_TYPES: [&str; 2] = ["audio/webm", "audio/mp4"];
const NAV_FAILURES_BEFORE_ERROR: u32 = 3;
const UNAVAILABLE: &str = "Guidance is unavailable. Double tap to try again.";
const MAX_SPEECH_CHARS: usize = 240;
// ponytail: speech cache capped by entry count, never evicted. Templates and prompts are a small fixed set.
const SPEECH_CACHE_ENTRIES: usize = 256;
const RECENT_SPOKEN_ENTRIES: usize = 32;
/// A recording can hold audio from up to this long before it ended, plus playback delay.
const ECHO_WINDOW_BEFORE_MS: i64 = 15_000;
const ECHO_WINDOW_AFTER_MS: i64 = 1_500;

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
    frame_gap_ms: u64, // the phone waits this long after each frame upload: the pace of `localize` calls
}

impl Limits {
    pub fn from_vars(get: Vars) -> Self {
        Limits {
            max_audio_ms: 10_000,
            max_audio_bytes: 1_000_000,
            max_frame_bytes: 512_000,
            max_frame_edge_px: var(get, "MAX_FRAME_EDGE_PX", "640")
                .parse()
                .expect("MAX_FRAME_EDGE_PX"),
            max_input_age_ms: var(get, "MAX_INPUT_AGE_MS", "3000")
                .parse()
                .expect("MAX_INPUT_AGE_MS"),
            heartbeat_ms: 5000,
            nav_frames: var(get, "NAV_FRAMES", "4").parse().expect("NAV_FRAMES"),
            frame_gap_ms: var(get, "FRAME_GAP_MS", "100")
                .parse()
                .expect("FRAME_GAP_MS"),
        }
    }
}

pub struct AppState {
    pub pipeline: Pipeline,
    pub limits: Limits,
    trace_path: Option<String>,
    trace_stdout: bool,
    pub debug_page: bool,
    pub commit: Option<String>, // GIT_SHA, set by the deploy
    speech_cache: Mutex<HashMap<String, bytes::Bytes>>, // text -> complete MP3
    recent_spoken: Mutex<VecDeque<(i64, String)>>, // when each phone last asked for a sentence
}

pub type App = Arc<AppState>;
pub type ClientRef = Arc<Mutex<Client>>;

impl AppState {
    pub fn new(pipeline: Pipeline, get: Vars) -> Self {
        AppState {
            pipeline,
            limits: Limits::from_vars(get),
            trace_path: get("TRACE_PATH"),
            trace_stdout: get("TRACE_STDOUT").is_some(),
            debug_page: get("DEBUG_PAGE").is_some(),
            commit: get("GIT_SHA"),
            speech_cache: Mutex::default(),
            recent_spoken: Mutex::default(),
        }
    }

    /// Remembers a sentence some phone is about to speak, so a microphone hearing it back can be told apart.
    pub fn note_spoken(&self, text: &str) {
        let mut recent = self.recent_spoken.lock().unwrap();
        recent.push_back((now_ms(), text.to_string()));
        while recent.len() > RECENT_SPOKEN_ENTRIES {
            recent.pop_front();
        }
    }

    /// Sentences any phone had spoken around `at_ms`: what a recording that ended then may have heard.
    pub fn spoken_near(&self, at_ms: i64) -> Vec<String> {
        let recent = self.recent_spoken.lock().unwrap();
        let (from, to) = (at_ms - ECHO_WINDOW_BEFORE_MS, at_ms + ECHO_WINDOW_AFTER_MS);
        recent
            .iter()
            .filter(|(t, _)| (from..=to).contains(t))
            .map(|(_, text)| text.clone())
            .collect()
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
    motion: Option<Value>, // the phone's motion estimate (contracts.md, "Motion"), passed on to nav-api as sent
}

impl Meta {
    fn step_count(&self) -> Option<i64> {
        self.motion.as_ref()?["stepCount"].as_i64()
    }
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

/// The navigation loop's state (contracts.md section 2, "Navigation loop"): locating while there is no path,
/// otherwise following `path[hop]`. nav-api is stateless; this is the only place the user's progress lives.
#[derive(Clone, Default)]
struct Nav {
    node: Option<String>, // the user's node: the last one reached, carried into the next session
    node_steps: Option<i64>, // the phone's stepCount when they reached it (localize's previous_step_count)
    path: Option<Vec<Value>>, // the route's hops, kept from the one `route` call
    hop: usize,
    belief: Belief,                // where the user is, per node (pipeline::Belief)
    last_steps: Option<i64>, // the phone's stepCount at the last localization: metres walked since
    no_route_from: Option<String>, // the node nav-api had no route from: `route` is asked again only from another one
}

impl Nav {
    /// A fresh start from the same place: locate again, keep the node and the belief.
    fn relocate(&self) -> Nav {
        Nav {
            node: self.node.clone(),
            node_steps: self.node_steps,
            belief: self.belief.clone(),
            last_steps: self.last_steps,
            ..Nav::default()
        }
    }

    /// The `localize` question: the hop being followed, or (locating) the last node, so nav-api can mark the nodes
    /// the user can't have walked to yet.
    fn ask(&self) -> Ask {
        let hop = self.path.as_ref().map(|p| &p[self.hop]);
        let field = |k: &str| hop.and_then(|h| h[k].as_str()).map(String::from);
        Ask {
            previous: if hop.is_some() {
                field("source")
            } else {
                self.node.clone()
            },
            expected: field("target"),
            previous_step_count: self.node_steps,
        }
    }
}

/// One spoken action: guidance to one destination.
struct Session {
    id: String,
    destination: Destination,
    nav: Nav,
    frames: VecDeque<(Meta, Vec<u8>)>, // last `nav_frames` accepted frames, oldest first
    sent_upto: i64, // sequence of the newest frame sent to localize: each frame goes once
    pending: Option<Meta>, // newest frame not evaluated yet
    previous: Option<Output>, // the worker compares each output with this one
    last_spoken_at: i64,
    arrived: bool,
    nav_failures: u32,
}

impl Session {
    fn new(destination: Destination, nav: Nav) -> Self {
        Session {
            id: uuid::Uuid::new_v4().to_string(),
            destination,
            nav,
            frames: VecDeque::new(),
            sent_upto: -1,
            pending: None,
            previous: None,
            last_spoken_at: 0,
            arrived: false,
            nav_failures: 0,
        }
    }

    fn unsent(&self) -> usize {
        self.frames
            .iter()
            .filter(|(m, _)| m.sequence > self.sent_upto)
            .count()
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
    sse_logs: bool,
    stamp: Value, // which app produced the trace: commit, version, map
    place: Place, // the map the phone chose and the destinations on it
}

pub fn lock(c: &ClientRef) -> MutexGuard<'_, Client> {
    c.lock().unwrap()
}

impl Client {
    fn destination(&self, id: &str) -> Destination {
        self.place
            .destinations
            .iter()
            .find(|d| d.destination_id == id)
            .expect("validated destination")
            .clone()
    }

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
            "routeStepId": self.session.as_ref().map(|s| &s.nav.node),
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

    /// A different action: new session, empty frame buffer, no previous output. It locates the user again, from
    /// the last node reached.
    fn start_session(&mut self, destination: Destination) {
        self.reset();
        self.stopped = false;
        let nav = self
            .session
            .as_ref()
            .map(|s| s.nav.relocate())
            .unwrap_or_default();
        self.session = Some(Session::new(destination, nav));
    }

    fn log(&mut self, request_id: Option<&str>, kind: &str, fields: Value) {
        let entry = merge(
            &json!({
                "at": now_ms(), "clientId": self.id,
                "sessionId": self.session.as_ref().map(|s| &s.id), "generation": self.generation,
                "requestId": request_id, "kind": kind, "phase": self.phase(), "clientRouteStepId": null,
                "routeStepId": self.session.as_ref().and_then(|s| s.nav.node.as_ref()),
                "destinationId": self.session.as_ref().map(|s| &s.destination.destination_id),
                "transcript": null, "command": null, "engine": null, "framesSent": null,
                "action": null, "confidence": null, "observation": null, "spoke": false,
                "text": null, "timingsMs": {}, "dropped": null, "error": null,
                "localize": null, "route": null, "quietReason": null, "jev": null,
            }),
            merge(&self.stamp, fields),
        );
        if self.trace_stdout {
            println!("{}", for_cloud_logging(&entry)); // one JSON line: Cloud Logging stores it as a structured entry
        }
        if let Some(path) = &self.trace_path
            && let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
        {
            let _ = writeln!(f, "{entry}");
        }
        if self.sse_logs {
            self.emit("log", request_id, json!({"kind": kind, "entry": entry}));
        }
        self.entries.push(entry);
    }

    fn accepted(&mut self, request_id: &str) -> Value {
        let body = json!({"requestId": request_id, "accepted": true});
        self.responses.insert(request_id.to_string(), body.clone());
        body
    }
}

/// Every accepted frame joins the session's buffer. An evaluation sends `localize` the frames it has not sent before,
/// so a frame that arrives during a call goes with the next one.
fn submit_frame(app: &App, cref: &ClientRef, c: &mut Client, meta: Meta, frame: Vec<u8>) {
    let Some(s) = c.session.as_mut() else { return };
    s.frames.push_back((meta.clone(), frame));
    while s.frames.len() > app.limits.nav_frames {
        s.frames.pop_front();
    }
    s.pending = Some(meta);
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

/// nav-api didn't answer: worth trying again.
pub fn navigation_unavailable(message: impl Into<String>) -> ApiError {
    ApiError {
        retryable: true,
        ..api_error(503, "navigation_unavailable", message)
    }
}

pub fn bad_request(message: impl ToString) -> ApiError {
    api_error(400, "bad_request", message.to_string())
}

pub type ApiResult<T> = Result<T, ApiError>;

/// A trace entry with the `message` and `severity` fields Cloud Logging shows on the summary line.
///
/// `{kind: "input", clientId: "c2cd8407-…", transcript: "take me to the coffee", command: "counter"}`
/// gets `message: "[c2cd8407] input · “take me to the coffee” → counter"`.
fn for_cloud_logging(entry: &Value) -> Value {
    let s = |k: &str| entry[k].as_str().unwrap_or("");
    let what = if !s("error").is_empty() {
        format!("error: {}", s("error"))
    } else if !s("dropped").is_empty() {
        format!("{} dropped: {}", s("action"), s("dropped"))
    } else if s("kind") == "input" {
        format!("“{}” → {}", s("transcript"), s("command"))
    } else if entry["spoke"] == true {
        format!("{} → “{}”", s("action"), s("text"))
    } else {
        s("action").to_string()
    };
    let step = match s("routeStepId") {
        "" => String::new(),
        step => format!(" @{step}"),
    };
    let id: String = s("clientId").chars().take(8).collect();
    merge(
        entry,
        json!({
            "message": format!("[{id}] {}{step} · {what}", s("kind")).trim_end_matches(" · ").to_string(),
            "severity": if s("error").is_empty() { "INFO" } else { "ERROR" },
        }),
    )
}

/// Debug lines the phone posts to `POST /v1/logs`. Each becomes one structured entry on stdout,
/// next to the server's own trace, so one Cloud Logging query shows both sides of a session.
/// Failures before a client exists (permissions, no camera) are only visible this way.
pub fn client_logs(app: &App, body: &[u8]) -> ApiResult<Vec<Value>> {
    const MAX_BODY: usize = 16 * 1024;
    const MAX_ENTRIES: usize = 50;
    if body.len() > MAX_BODY {
        return Err(api_error(
            413,
            "payload_too_large",
            "Log batch is too large.",
        ));
    }
    let v: Value = serde_json::from_slice(body).map_err(|_| bad_request("Body is not JSON."))?;
    let entries = v
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| bad_request("Missing entries."))?;
    if entries.len() > MAX_ENTRIES {
        return Err(bad_request("Too many entries."));
    }
    let text = |v: &Value, key: &str, max: usize| -> Option<String> {
        v.get(key)
            .and_then(Value::as_str)
            .map(|s| s.chars().take(max).collect())
    };
    let device_id = text(&v, "deviceId", 64);
    let client_id = text(&v, "clientId", 64);
    let received_at = now_ms();
    Ok(entries
        .iter()
        .map(|e| {
            let level = e
                .get("level")
                .and_then(Value::as_str)
                .filter(|l| ["info", "warn", "error"].contains(l))
                .unwrap_or("info");
            let event = text(e, "event", 64);
            let detail = text(e, "detail", 1000);
            let id: String = client_id.as_deref().or(device_id.as_deref()).unwrap_or("").chars().take(8).collect();
            json!({
                "message": format!("[{id}] web · {} {}", event.as_deref().unwrap_or(""), detail.as_deref().unwrap_or("")).trim_end(),
                "severity": match level { "warn" => "WARNING", "error" => "ERROR", _ => "INFO" },
                "kind": "client_log", "source": "web", "level": level,
                "at": e.get("at").and_then(Value::as_i64), "receivedAt": received_at,
                "deviceId": device_id, "clientId": client_id,
                "event": event, "detail": detail,
                "commit": app.commit, "version": env!("CARGO_PKG_VERSION"),
            })
        })
        .collect())
}

/// A new client with its token, on the place it chose. The response body is the contract's `POST /v1/clients` reply.
pub fn create_client(app: &App, id: String, place: Place) -> (ClientRef, Value) {
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
        sse_logs: app.debug_page,
        stamp: json!({
            "commit": app.commit, "version": env!("CARGO_PKG_VERSION"),
            "mapId": place.map_id.clone(),
        }),
        place,
    };
    c.log(None, "client", json!({}));
    let destinations: Vec<Value> = c
        .place
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
        "route": {"routeId": c.place.map_id, "destinations": destinations},
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

/// The multipart parts of an input or frame upload, each already checked with `check_part`.
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

fn ask_again(destinations: &[Destination], reason: &str) -> String {
    match reason {
        "empty" => "I didn't hear anything. Where would you like to go?".to_string(),
        "unsupported" => {
            // Only here are the options useful: the user asked for somewhere we cannot guide to.
            let places = destinations
                .iter()
                .map(|d| d.label.as_str())
                .collect::<Vec<_>>()
                .join(" or the ");
            format!(
                "I can't guide you there yet. I can take you to the {places}. Where would you like to go?"
            )
        }
        _ => "Sorry, I didn't understand. Where would you like to go?".to_string(),
    }
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

pub fn input(app: &App, cref: &ClientRef, parts: Parts) -> ApiResult<Value> {
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
    accept(app, &mut c, &m, "input")?;
    spawn(handle_input(
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
        s.nav.belief = Belief::default();
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
    app.note_spoken(&text);
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

async fn handle_input(
    app: App,
    cref: ClientRef,
    m: Meta,
    (audio, content_type): (Vec<u8>, String),
    frame: Option<Vec<u8>>,
) {
    let (started_gen, phase, destinations) = {
        let c = lock(&cref);
        (c.generation, c.phase(), c.place.destinations.clone())
    };
    let rid = Some(m.request_id.as_str());
    let mut timings = serde_json::Map::new();
    let t = now_ms();
    let transcript = match app
        .pipeline
        .transcribe(audio, &content_type, &destinations)
        .await
    {
        Ok(text) => text.trim().to_string(),
        Err(e) => {
            let mut c = lock(&cref);
            if c.generation == started_gen {
                c.log(rid, "input", json!({"error": format!("stt: {e}")}));
                let data = json!({"code": "upstream_unavailable", "stage": "stt", "text": UNAVAILABLE, "retryable": true});
                c.halt("error", rid, data);
            }
            return;
        }
    };
    timings.insert("stt".into(), json!(now_ms() - t));
    let t = now_ms();
    // The phone heard the app (its own speaker, or another phone's) rather than a person: ask again.
    let echo = pipeline::is_echo(&transcript, &app.spoken_near(m.captured_at));
    let (cmd, jev) = if transcript.is_empty() || echo {
        (Command::Empty, Value::Null)
    } else {
        app.pipeline
            .command(&transcript, phase, &destinations)
            .await
    };
    timings.insert("command".into(), json!(now_ms() - t));
    timings.insert("total".into(), json!(now_ms() - m.captured_at));

    let mut c = lock(&cref);
    if c.generation != started_gen {
        c.log(
            rid,
            "input",
            json!({"transcript": transcript, "dropped": "stale_generation"}),
        );
        return;
    }
    let mut fields = json!({"transcript": transcript, "command": cmd.name()});
    fields["timingsMs"] = json!(timings);
    fields["jev"] = jev;
    if echo {
        fields["dropped"] = json!("echo");
    }
    c.log(rid, "input", fields);
    match cmd {
        Command::Start(id) => {
            let same_action = c.phase() == "navigating"
                && c.session
                    .as_ref()
                    .is_some_and(|s| s.destination.destination_id == id);
            if !same_action {
                let destination = c.destination(&id);
                c.start_session(destination);
            } else if let Some(s) = c.session.as_mut() {
                s.previous = None;
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
                json!({"reason": reason, "text": ask_again(&destinations, reason)}),
            );
        }
    }
}

/// One worker per client and generation: evaluates the frames not sent to `localize` yet, once there are
/// `NAV_BURST` of them. A worker from an older generation stops at its next turn and leaves the slot to the new one.
async fn worker(app: App, cref: ClientRef, generation: i64) {
    let burst = app.pipeline.nav_loop.burst.max(1);
    loop {
        let job = {
            let mut c = lock(&cref);
            let pending = if c.generation == generation {
                c.session
                    .as_mut()
                    .filter(|s| s.unsent() >= burst)
                    .and_then(|s| s.pending.take())
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

/// A failed call to the navigation engine. Three in a row end guidance with an error.
fn nav_failed(cref: &ClientRef, started_gen: i64, rid: Option<&str>, base: &Value, error: String) {
    let mut c = lock(cref);
    if c.generation != started_gen {
        return;
    }
    let failures = c.session.as_mut().map_or(0, |s| {
        s.nav_failures += 1;
        s.nav_failures
    });
    c.log(rid, "frame", merge(base, json!({"error": error})));
    if failures >= NAV_FAILURES_BEFORE_ERROR {
        let data = json!({"code": "upstream_unavailable", "stage": "navigate", "text": UNAVAILABLE, "retryable": true});
        c.halt("error", rid, data);
    }
}

/// What one localization did to the navigation loop.
#[derive(Debug, PartialEq)]
enum Move {
    Stay,
    Located(String),
    Reached(String),
    Lost,
}

/// One localization through the navigation loop (contracts.md section 2): the steps walked since the last one move
/// the belief along the map's links, the result reweighs it (pipeline::Belief). Locating, the most likely node is
/// where the user is once it is `reach` likely; following, the most likely node ahead on the route (the hop's target or
/// one further on: the user walked past a node the camera missed) is reached once `reach` likely, and the route goes
/// on from there; a node off the route `leave` likely starts over. Nothing is reset on a move: the belief carries
/// over, so a weak lead that keeps up adds up, and a node next to the last one needs less evidence than one across
/// the map. `steps` is the newest frame's stepCount. Returns the most likely node, its probability, and the move.
fn advance(
    nav: &mut Nav,
    found: &Value,
    graph: &Graph,
    steps: Option<i64>,
    cfg: &pipeline::NavLoop,
) -> (String, f64, Move) {
    let walked = match (steps, nav.last_steps) {
        (Some(s), Some(last)) if s >= last => {
            (s - last) as f64 * graph.step_length_m * cfg.walk_scale
        }
        (Some(_), None) => 0.0,
        _ => cfg.walk_m_per_call, // no steps, or the phone restarted counting
    };
    nav.last_steps = steps;
    nav.belief.predict(graph, walked, cfg);
    nav.belief.update(found, cfg);
    let Some((best, p)) = nav.belief.best().map(|(n, p)| (n.to_string(), p)) else {
        return (String::new(), 0.0, Move::Stay);
    };
    let Some(path) = &nav.path else {
        if p < cfg.reach {
            return (best, p, Move::Stay);
        }
        (nav.node, nav.node_steps) = (Some(best.clone()), steps);
        return (best.clone(), p, Move::Located(best));
    };
    let node_of = |hop: &Value, end: &str| hop[end].as_str().unwrap_or("").to_string();
    let source = node_of(&path[nav.hop], "source");
    let ahead: Vec<String> = path[nav.hop..]
        .iter()
        .map(|h| node_of(h, "target"))
        .collect();
    let target = ahead
        .iter()
        .fold(&ahead[0], |a, b| {
            if nav.belief.prob(b) > nav.belief.prob(a) {
                b
            } else {
                a
            }
        })
        .clone();
    if nav.belief.prob(&target) >= cfg.reach {
        // Reached the target: the route goes on from there, past any node the camera missed.
        let reached = nav.hop + ahead.iter().position(|a| *a == target).unwrap_or(0);
        (nav.node, nav.node_steps) = (Some(target.clone()), steps);
        nav.hop = reached + 1;
        if nav.hop >= path.len() {
            nav.path = None;
        }
        return (best, p, Move::Reached(target));
    }
    if best != source && !ahead.contains(&best) && p >= cfg.leave {
        *nav = nav.relocate();
        return (best, p, Move::Lost);
    }
    (best, p, Move::Stay)
}

/// The frames not sent yet go to nav-api's `localize` as one vote in the navigation loop; once the user is located,
/// one `route` to the destination; then the current hop becomes the output, and Jev says whether a change is worth
/// saying.
async fn evaluate(app: &App, cref: &ClientRef, m: Meta) {
    let (started_gen, dest, mut nav, frames, steps, previous, last_spoken_at, caller, graph) = {
        let mut c = lock(cref);
        let (generation, client, map) = (c.generation, c.id.clone(), c.place.map_id.clone());
        let graph = c.place.graph.clone();
        let Some(s) = c.session.as_mut() else { return };
        let unsent: Vec<&(Meta, Vec<u8>)> = s
            .frames
            .iter()
            .filter(|(f, _)| f.sequence > s.sent_upto)
            .collect();
        let Some((newest, _)) = unsent.last() else {
            return;
        };
        let steps = newest.step_count();
        let sent_upto = newest.sequence;
        let frames: Vec<(Option<Value>, Vec<u8>)> = unsent
            .iter()
            .map(|(f, data)| (f.motion.clone(), data.clone()))
            .collect();
        s.sent_upto = sent_upto;
        let caller = Caller {
            map,
            client,
            session: Some(s.id.clone()),
            request: m.request_id.clone(),
        };
        (
            generation,
            s.destination.clone(),
            s.nav.clone(),
            frames,
            steps,
            s.previous.clone(),
            s.last_spoken_at,
            caller,
            graph,
        )
    };
    let goal = dest.destination_id.as_str();
    let rid = Some(m.request_id.as_str());
    let mut timings = serde_json::Map::new();
    timings.insert("upload".into(), json!((now_ms() - m.captured_at).max(0)));
    let mut base = json!({
        "clientRouteStepId": m.client_route_step_id, "engine": app.pipeline.engine, "framesSent": frames.len(),
    });
    let t = now_ms();
    let ask = nav.ask();
    let doing = match (&ask.expected, &ask.previous) {
        (Some(target), Some(source)) => format!("{source} → {target}"),
        _ => "locating".to_string(),
    };
    let found = match app.pipeline.locate(frames, &ask, &caller).await {
        Ok(found) => found,
        Err(e) => return nav_failed(cref, started_gen, rid, &base, format!("localize: {e}")),
    };
    timings.insert("localize".into(), json!(now_ms() - t));
    let cfg = app.pipeline.nav_loop;
    let (best, p, mv) = advance(&mut nav, &found, &graph, steps, &cfg);
    let moved = match &mv {
        Move::Stay => String::new(),
        Move::Located(n) => format!(" → located at {n}"),
        Move::Reached(n) => format!(" → reached {n}"),
        Move::Lost => " → lost: locating again".to_string(),
    };
    let observation = format!(
        "{doing}: {best} {p:.2} likely{moved} · {}: {}",
        found["status"].as_str().unwrap_or(""),
        found["reason"].as_str().unwrap_or("")
    );
    base = merge(
        &base,
        json!({"confidence": found["candidates"][0]["score"], "observation": observation, "localize": found}),
    );
    let arrived = matches!(&mv, Move::Located(n) | Move::Reached(n) if n == goal);
    if let Move::Located(n) = &mv
        && !arrived
        && nav.no_route_from.as_ref() != Some(n)
    {
        let t = now_ms();
        match app.pipeline.path(n, goal, &caller).await {
            Ok(route) => {
                timings.insert("route".into(), json!(now_ms() - t));
                let hops = route["hops"].as_array().cloned().unwrap_or_default();
                if route["found"] == true && hops.first().is_some_and(|h| h["source"] == n.as_str())
                {
                    (nav.path, nav.hop) = (Some(hops), 0);
                } else {
                    nav.no_route_from = Some(n.clone());
                }
                base = merge(&base, json!({"route": route}));
            }
            Err(e) => return nav_failed(cref, started_gen, rid, &base, format!("route: {e}")),
        }
    }
    let output = match &nav.path {
        _ if arrived => Output::arrived(goal),
        Some(path) => app
            .pipeline
            .hop_output(&path[nav.hop], nav.node.as_deref().unwrap_or("")),
        None if nav.no_route_from.is_some() && nav.no_route_from == nav.node => {
            Output::no_route(nav.node.as_deref().unwrap_or(""), &dest.label)
        }
        None => Output::wait(nav.node.as_deref(), true), // locating: "Please hold still"
    };

    let now = now_ms();
    let (mut speak, mut reason) = app.pipeline.should_speak(&output, previous.as_ref());
    let mut jev_answer = Value::Null;
    if reason == "changed" && output.action != "arrived" && !app.pipeline.jev_api_key.is_empty() {
        let t = now_ms();
        let state = json!({
            "destination": dest.label, "previous": previous, "new": output,
            "msSinceLastSpoken": now - last_spoken_at,
        });
        let (worth, answer) = app.pipeline.worth_saying(state).await;
        jev_answer = answer;
        if !worth {
            (speak, reason) = (false, "not_worth_saying");
        }
        timings.insert("jev".into(), json!(now_ms() - t));
    }
    let text = speak.then(|| {
        output.instruction.clone().unwrap_or_else(|| {
            pipeline::template(
                &output.action,
                output.direction.as_deref(),
                output.uncertain,
                &dest.label,
            )
        })
    });

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
    s.nav = nav;
    s.arrived = arrived;
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
                "direction": output.direction, "routeStepId": output.step, "nextRouteStepId": output.next,
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
            json!({"action": output.action, "spoke": speak, "text": text, "timingsMs": timings, "quietReason": (!speak).then_some(reason), "jev": jev_answer}),
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    /// A result listing every node of `graph()`: `best` at 0.4, the others `margin` below.
    fn found(best: &str, margin: f64) -> Value {
        let mut candidates = vec![json!({"node": best, "score": 0.4})];
        candidates.extend(
            ["n1", "n2", "n3", "n4", "n9"]
                .iter()
                .filter(|n| **n != best)
                .map(|n| json!({"node": n, "score": 0.4 - margin})),
        );
        json!({"status": "uncertain", "best": best, "margin": margin, "candidates": candidates})
    }

    /// n1 - n2 - n3 - n4 along the route, n9 off it next to n2; 3 m each, 0.3 m per phone step.
    fn graph() -> Graph {
        let link = |a: &str, b: &str| json!({"source": a, "target": b, "distance_m": 3.0});
        let nodes = ["n1", "n2", "n3", "n4", "n9"].map(|n| json!({"id": n}));
        Graph::from_map(&json!({
            "nodes": nodes,
            "links": [link("n1", "n2"), link("n2", "n3"), link("n3", "n4"), link("n2", "n9")],
            "frame": {"live_step_length_m": 0.3},
        }))
    }

    fn path() -> Vec<Value> {
        ["n1", "n2", "n3", "n4"]
            .windows(2)
            .map(|w| json!({"source": w[0], "target": w[1], "steps": [{"action": "straight"}]}))
            .collect()
    }

    fn cfg() -> pipeline::NavLoop {
        Pipeline::from_vars(&|_| None).nav_loop
    }

    /// Feeds `f` until something moves (at most `n` times), the steps going up by `step` each time from `steps`.
    fn until_move(nav: &mut Nav, f: &Value, steps: i64, step: i64, n: usize) -> (usize, Move) {
        for i in 0..n {
            let (_, _, mv) = advance(nav, f, &graph(), Some(steps + step * i as i64), &cfg());
            if mv != Move::Stay {
                return (i, mv);
            }
        }
        (n, Move::Stay)
    }

    #[test]
    fn should_locate_once_one_node_is_likely_enough_not_on_the_first_weak_result() {
        let mut nav = Nav::default();
        let ask = nav.ask();
        assert_eq!((ask.previous, ask.expected), (None, None));
        let (i, mv) = until_move(&mut nav, &found("n1", 0.08), 3, 0, 30);
        assert!(i > 0 && mv == Move::Located("n1".into()), "{i} {mv:?}");
        assert_eq!((nav.node.as_deref(), nav.node_steps), (Some("n1"), Some(3)));
    }

    #[test]
    fn should_reach_a_node_further_on_and_end_the_path_at_its_last_hop() {
        let mut nav = Nav::default();
        until_move(&mut nav, &found("n1", 0.1), 0, 0, 30);
        nav.path = Some(path());
        let ask = nav.ask();
        assert_eq!(
            (ask.previous.as_deref(), ask.expected.as_deref()),
            (Some("n1"), Some("n2"))
        );
        // the camera never shows n2: n3 leading while walking (a step a frame spreads the belief over 3 m links faster
        // than a 0.1 lead gathers it), then while pausing there, is reached; the route goes on from there
        assert_eq!(
            until_move(&mut nav, &found("n3", 0.1), 10, 1, 8).1,
            Move::Stay
        );
        let (_, mv) = until_move(&mut nav, &found("n3", 0.1), 18, 0, 30);
        assert_eq!(mv, Move::Reached("n3".into()));
        assert_eq!((nav.hop, nav.ask().expected.as_deref()), (2, Some("n4")));
        until_move(&mut nav, &found("n4", 0.1), 50, 1, 4);
        let (_, mv) = until_move(&mut nav, &found("n4", 0.1), 54, 0, 30);
        assert_eq!(mv, Move::Reached("n4".into()));
        assert!(nav.path.is_none() && nav.node.as_deref() == Some("n4"));
    }

    #[test]
    fn should_not_move_to_the_goal_seen_from_afar_a_few_times_while_standing() {
        let mut nav = Nav::default();
        until_move(&mut nav, &found("n1", 0.1), 5, 0, 30);
        nav.path = Some(path());
        let mut afar = found("n4", 0.1);
        afar["candidates"][0]["plausible"] = json!(false);
        assert_eq!(until_move(&mut nav, &afar, 5, 0, 4).1, Move::Stay);
    }

    #[test]
    fn should_start_over_from_the_last_node_when_a_node_off_the_route_stays_ahead_keeping_the_belief()
     {
        let mut nav = Nav::default();
        until_move(&mut nav, &found("n2", 0.1), 0, 0, 30);
        (nav.path, nav.hop) = (Some(path()), 1);
        until_move(&mut nav, &found("n9", 0.1), 0, 1, 3);
        let (_, mv) = until_move(&mut nav, &found("n9", 0.1), 3, 0, 30);
        assert_eq!(mv, Move::Lost);
        assert!(nav.path.is_none() && nav.belief.best().unwrap().0 == "n9");
        assert_eq!(nav.ask().previous.as_deref(), Some("n2"));
        // locating again starts from what it knows: n9 at once
        assert_eq!(
            until_move(&mut nav, &found("n9", 0.1), 3, 0, 1).1,
            Move::Located("n9".into())
        );
    }

    /// The walk back on Glories Basement, 2026-10-10 (tests/fixtures/glories-walk-back.json), over the localize
    /// answers nav-api gave live (top two candidates only; the orchestrator now asks for all): the belief places the
    /// user only where the live votes did in that session, in the order walked, back towards the entrance. It places
    /// them in 3 of the 5 sessions where the votes did in all: slower on frames that barely tell nodes apart.
    #[test]
    fn should_place_the_user_on_the_live_walk_back_only_where_the_live_orchestrator_did() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/glories-walk-back.json")).unwrap();
        let graph = Graph::from_map(&fixture["map"]);
        let (mut walked, mut sessions) = (Vec::new(), 0);
        for s in fixture["sessions"].as_array().unwrap() {
            let frames = s["frames"].as_array().unwrap();
            let live: Vec<&str> = frames
                .iter()
                .filter_map(|f| f["located"].as_str())
                .collect();
            let mut nav = Nav::default();
            let mut placed = false;
            for (i, f) in frames.iter().enumerate() {
                if let (_, _, Move::Located(n)) =
                    advance(&mut nav, &f["localize"], &graph, None, &cfg())
                {
                    assert!(
                        live.contains(&n.as_str()),
                        "session {} frame {i}: {n}, live {live:?}",
                        s["client"]
                    );
                    if walked.last() != Some(&n) {
                        walked.push(n);
                    }
                    placed = true;
                }
            }
            sessions += placed as usize;
        }
        assert_eq!(walked, ["n13", "n10"]);
        assert_eq!(sessions, 3);
    }

    /// Itnig, 2026-10-10 (tests/fixtures/itnig-following-restarts.json, on tests/fixtures/itnig-map.json): every live
    /// hop the votes ended because another node won, with nav-api's answers up to that restart. The belief never moves
    /// the user to a node off the route or behind it, nor drops a route whose next nodes were the ones winning. Those
    /// answers stop where the votes decided, three results in: the belief, needing more, mostly hasn't moved yet.
    #[test]
    fn should_never_move_wrong_where_live_dropped_the_route() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/itnig-following-restarts.json"
        ))
        .unwrap();
        let graph = Graph::from_map(
            &serde_json::from_str(include_str!("../tests/fixtures/itnig-map.json")).unwrap(),
        );
        for h in fixture["hops"].as_array().unwrap() {
            let route: Vec<&str> = h["route"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_str().unwrap())
                .collect();
            let hop = h["hop"].as_u64().unwrap() as usize;
            let mut nav = Nav {
                node: Some(route[hop].into()),
                belief: Belief::at(route[hop]),
                path: Some(
                    route
                        .windows(2)
                        .map(|w| json!({"source": w[0], "target": w[1]}))
                        .collect(),
                ),
                hop,
                ..Nav::default()
            };
            let mv = h["frames"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| advance(&mut nav, f, &graph, None, &cfg()).2)
                .find(|mv| *mv != Move::Stay);
            let label = format!("{} hop {}: {}", h["session"], hop, h["live"]);
            match mv {
                None => {}
                Some(Move::Reached(n)) => assert!(
                    h["where"] == "further on the route" && h["elsewhere"] == n.as_str(),
                    "{label}"
                ),
                Some(Move::Lost) => assert!(h["where"] != "further on the route", "{label}"),
                Some(other) => panic!("{label}: {other:?}"),
            }
        }
    }

    #[test]
    fn should_tell_the_phone_its_frame_pace_and_size() {
        let limits = |get: Vars| serde_json::to_value(Limits::from_vars(get)).unwrap();
        let default = limits(&|_| None);
        assert_eq!(
            (&default["frameGapMs"], &default["maxFrameEdgePx"]),
            (&json!(100), &json!(640))
        );
        let tuned = limits(&|k| (k == "FRAME_GAP_MS").then(|| "250".to_string()));
        assert_eq!(tuned["frameGapMs"], 250);
    }

    fn place() -> Place {
        Place::from_map(&json!({"map_id": "glories-basement", "nodes": [{"id": "n1", "name": "Main entrance"}]}))
            .unwrap()
    }

    #[test]
    fn should_tell_the_phone_the_places_on_its_map() {
        let app = Arc::new(AppState::new(Pipeline::from_vars(&|_| None), &|_| None));
        let (_, response) = create_client(&app, "c1".into(), place());
        assert_eq!(
            response["route"],
            json!({"routeId": "glories-basement", "destinations": [{"destinationId": "n1", "label": "Main entrance"}]})
        );
    }

    #[test]
    fn should_stamp_every_trace_entry_with_the_app_version() {
        let get = |k: &str| (k == "GIT_SHA").then(|| "abc123".to_string());
        let app = Arc::new(AppState::new(Pipeline::from_vars(&get), &get));
        let (cref, _) = create_client(&app, "c1".into(), place());
        let entry = &trace(&cref)["entries"][0];
        assert_eq!(
            (&entry["commit"], &entry["version"], &entry["mapId"]),
            (
                &json!("abc123"),
                &json!(env!("CARGO_PKG_VERSION")),
                &json!("glories-basement")
            )
        );
    }

    #[test]
    fn should_stream_trace_entries_as_log_events_only_with_the_debug_page() {
        for (debug, expected) in [(true, Some("log")), (false, None)] {
            let get = |k: &str| (debug && k == "DEBUG_PAGE").then(|| "1".to_string());
            let app = Arc::new(AppState::new(Pipeline::from_vars(&get), &get));
            let (cref, _) = create_client(&app, "c1".into(), place());
            let mut c = lock(&cref);
            let mut rx = c.events.subscribe();
            c.log(Some("r1"), "stop", json!({}));
            let got = rx.try_recv().ok();
            assert_eq!(got.as_ref().map(|e| e["type"].as_str().unwrap()), expected);
            if let Some(ev) = got {
                assert_eq!(
                    (&ev["kind"], &ev["entry"]["kind"]),
                    (&json!("stop"), &json!("stop"))
                );
                assert_eq!(ev["requestId"], "r1");
                assert_eq!(ev["entry"]["phase"], "awaiting_destination");
            }
        }
    }

    #[test]
    fn should_summarise_trace_entries_for_cloud_logging() {
        let msg = |e: Value| {
            let out = for_cloud_logging(&e);
            (
                out["message"].as_str().unwrap().to_string(),
                out["severity"].as_str().unwrap().to_string(),
            )
        };
        let id = json!("c2cd8407-c53d");
        assert_eq!(
            msg(json!({"clientId": id, "kind": "input", "transcript": "take me to the coffee", "command": "counter"})).0,
            "[c2cd8407] input · “take me to the coffee” → counter"
        );
        assert_eq!(
            msg(json!({"clientId": id, "kind": "frame", "routeStepId": "corridor", "action": "turn", "spoke": true, "text": "Turn left."})).0,
            "[c2cd8407] frame @corridor · turn → “Turn left.”"
        );
        assert_eq!(
            msg(json!({"clientId": id, "kind": "frame", "action": "turn", "dropped": "stale_generation"})).0,
            "[c2cd8407] frame · turn dropped: stale_generation"
        );
        assert_eq!(
            msg(json!({"clientId": id, "kind": "frame", "error": "navigate: timeout"})),
            (
                "[c2cd8407] frame · error: navigate: timeout".into(),
                "ERROR".into()
            )
        );
        assert_eq!(
            msg(json!({"clientId": id, "kind": "client"})).0,
            "[c2cd8407] client"
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
    fn should_turn_a_client_log_batch_into_structured_entries() {
        let get = |k: &str| (k == "GIT_SHA").then(|| "abc123".to_string());
        let app = Arc::new(AppState::new(Pipeline::from_vars(&get), &get));
        let body = br#"{"deviceId":"d1","clientId":"c1","entries":[
            {"at":5,"level":"error","event":"permission_denied","detail":"NotAllowedError"},
            {"at":6,"level":"nonsense","event":"x"}]}"#;
        let out = client_logs(&app, body).ok().unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["kind"], "client_log");
        assert!(out[0]["message"].as_str().unwrap().contains(" web · "));
        assert_eq!(out[0]["level"], "error");
        assert_eq!(out[0]["detail"], "NotAllowedError");
        assert_eq!(out[0]["commit"], "abc123");
        assert_eq!(out[1]["level"], "info");
    }

    #[test]
    fn should_reject_oversized_or_malformed_client_logs() {
        let get = |_: &str| None;
        let app = Arc::new(AppState::new(Pipeline::from_vars(&get), &get));
        assert_eq!(
            client_logs(&app, &vec![b' '; 20_000]).unwrap_err().status,
            413
        );
        assert_eq!(client_logs(&app, b"nope").unwrap_err().status, 400);
        assert_eq!(
            client_logs(&app, br#"{"entries":"x"}"#).unwrap_err().status,
            400
        );
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
