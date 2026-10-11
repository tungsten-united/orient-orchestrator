//! Model calls behind the orchestrator: speech (ElevenLabs), command (TypeSafe Jev), navigation, the worker's
//! comparison rule, and the sentence templates.
//!
//! Every model output is validated here. Nothing in this module changes session state.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use regex::Regex;
use reqwest::multipart::{Form, Part};
use serde::Serialize;
use serde_json::{Value, json};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// MP3 bytes as they arrive.
pub type Audio = BoxStream<'static, Result<Bytes, BoxError>>;

const CANCEL_WORDS: [&str; 3] = ["cancel", "stop", "never mind"];
const MAX_INSTRUCTION_CHARS: usize = 240;
const MAX_LOCALIZE_FRAMES: usize = 4;

/// The place a phone chose: a map published on nav-api, and the spots on it the user can ask for.
#[derive(Clone)]
pub struct Place {
    pub map_id: String,
    pub destinations: Vec<Destination>,
    pub graph: Arc<Graph>,
}

/// A spot the user can ask for: a node of the map, by its name.
#[derive(Clone)]
pub struct Destination {
    pub destination_id: String,
    pub label: String,
}

impl Place {
    /// Every node of a nav-api map (`GET /maps/{map_id}`) is a destination, by its name.
    pub fn from_map(map: &Value) -> Option<Place> {
        let destinations = map["nodes"]
            .as_array()?
            .iter()
            .filter_map(|n| {
                Some(Destination {
                    destination_id: n["id"].as_str()?.to_string(),
                    label: n["name"].as_str()?.to_string(),
                })
            })
            .collect();
        Some(Place {
            map_id: map["map_id"].as_str()?.to_string(),
            destinations,
            graph: Arc::new(Graph::from_map(map)),
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Start(String),
    Cancel,
    Unsupported,
    Unclear,
    Empty,
}

impl Command {
    pub fn name(&self) -> &'static str {
        match self {
            Command::Start(_) => "start",
            Command::Cancel => "cancel",
            Command::Unsupported => "unsupported",
            Command::Unclear => "unclear",
            Command::Empty => "empty",
        }
    }
}

/// A validated navigation output. The worker compares each one with the session's previous one.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Output {
    pub action: String,
    pub direction: Option<String>,
    pub step: Option<String>,
    pub next: Option<String>,
    pub instruction: Option<String>,
    pub uncertain: bool,
}

impl Output {
    pub fn wait(step: Option<&str>, uncertain: bool) -> Self {
        Output {
            action: "wait".into(),
            direction: None,
            step: step.map(String::from),
            next: None,
            instruction: None,
            uncertain,
        }
    }

    /// Located, but nav-api has no route from here: said once, not "hold still", which would blame the camera.
    pub fn no_route(step: &str, destination_label: &str) -> Self {
        Output {
            instruction: Some(format!(
                "I can't find a way to the {destination_label} from here."
            )),
            ..Output::wait(Some(step), false)
        }
    }

    pub fn arrived(step: &str) -> Self {
        Output {
            action: "arrived".into(),
            ..Output::wait(Some(step), false)
        }
    }
}

/// The navigation loop's settings (contracts.md section 2, "Navigation loop"): the belief's. The defaults come from
/// nav-engine's `nav map tune itnig` (2026-10-11, its `BeliefParams`): on episodes stitched from the Itnig walks, each
/// matched against the other walks' references, 293 of 966 nodes reached with no wrong move, where the votes this
/// replaced reached 289 with 430 wrong. Except the temperature: live frames separate nodes less than replayed ones
/// (Itnig's median lead 0.05 live, 0.068 replayed), so the tuned 0.25 is scaled by that ratio to 0.18. On replays
/// 0.18 is past the edge (203 wrong of 966): raise NAV_BELIEF_TEMPERATURE if staging shows wrong moves, lower it if
/// users wait too long to be located.
#[derive(Clone, Copy, Debug)]
pub struct NavLoop {
    pub burst: usize, // NAV_BURST: frames not sent to localize yet that start an evaluation
    pub temperature: f64, // NAV_BELIEF_TEMPERATURE: score difference that makes a node e times less likely than the leader
    pub gate: f64,        // NAV_BELIEF_GATE: likelihood of a node nav-api marks farther than walked
    pub jump: f64,        // NAV_BELIEF_JUMP: belief spread over every node each localization
    pub walk_scale: f64, // NAV_BELIEF_WALK_SCALE: metres walked = phone steps x the map's live step length x this
    pub walk_m_per_call: f64, // NAV_BELIEF_WALK_M_PER_CALL: metres walked per localization without steps
    pub reach: f64,           // NAV_BELIEF_REACH: locate at, or reach a node ahead, this likely
    pub leave: f64,           // NAV_BELIEF_LEAVE: drop the route for a node off it this likely
}

/// What a `localize` call says besides the frames: the hop being followed, or the last node when locating again.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ask {
    pub previous: Option<String>,
    pub expected: Option<String>,
    pub previous_step_count: Option<i64>,
}

/// Who a nav-api call is for: sent as X-Client-Id, X-Session-Id and X-Request-Id, so nav-api's log joins the trace.
#[derive(Clone, Debug, Default)]
pub struct Caller {
    pub map: String, // the client's map
    pub client: String,
    pub session: Option<String>,
    pub request: String,
}

const UNKNOWN_LENGTH_M: f64 = 10.0; // an edge without a distance, as nav-engine's route.py

/// The map as the belief walks it (`GET /maps/{map_id}`): its nodes, metres between linked nodes either way (over
/// `links`, the connections before cleaning, as nav-api's walked-distance gate; else `edges`), and metres per
/// phone step (`frame.live_step_length_m`).
#[derive(Clone, Debug, Default)]
pub struct Graph {
    pub nodes: Vec<String>,
    links: HashMap<String, Vec<(String, f64)>>,
    pub step_length_m: f64,
}

impl Graph {
    pub fn from_map(map: &Value) -> Graph {
        let nodes = map["nodes"].as_array().into_iter().flatten();
        let nodes = nodes
            .filter_map(|n| Some(n["id"].as_str()?.to_string()))
            .collect();
        let links = map["links"].as_array().filter(|l| !l.is_empty());
        let mut out: HashMap<String, Vec<(String, f64)>> = HashMap::new();
        for e in links.or(map["edges"].as_array()).into_iter().flatten() {
            let (Some(a), Some(b)) = (e["source"].as_str(), e["target"].as_str()) else {
                continue;
            };
            let length = e["distance_m"]
                .as_f64()
                .unwrap_or(UNKNOWN_LENGTH_M)
                .max(0.5);
            for (x, y) in [(a, b), (b, a)] {
                let next = out.entry(x.to_string()).or_default();
                match next.iter_mut().find(|(n, _)| n == y) {
                    Some(l) => l.1 = l.1.min(length),
                    None if x != y => next.push((y.to_string(), length)),
                    None => {}
                }
            }
        }
        let frame = &map["frame"];
        let step = frame["live_step_length_m"]
            .as_f64()
            .or(frame["step_length_m"].as_f64());
        Graph {
            nodes,
            links: out,
            step_length_m: step.unwrap_or(0.7),
        }
    }
}

/// Where the user is, as a probability per node (nav-engine's `navigation/belief.py`, the same rules):
///
/// - walking w metres, the user leaves node a for a linked node b at a rate w / metres(a, b); a little of the belief
///   (`jump`) spreads over every node each time (a move the camera and the steps missed, a wrong start);
/// - a `localize` result with node scores s multiplies node n by exp((s_n - s_best) / temperature), and a node nav-api
///   marks not plausible (farther than walked) by `gate`. A node it doesn't list scores as the lowest listed, at
///   most the leader minus the margin. Live scores are low; their differences are what tells.
///
/// Empty: nothing known yet, uniform over the map's nodes at the first update. A node nav-api names that the map
/// doesn't have joins with the share `jump` gives every node.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Belief(BTreeMap<String, f64>);

impl Belief {
    /// Sure the user is at `node`.
    pub fn at(node: &str) -> Belief {
        Belief(BTreeMap::from([(node.to_string(), 1.0)]))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn prob(&self, node: &str) -> f64 {
        self.0.get(node).copied().unwrap_or(0.0)
    }

    /// The most likely node and its probability.
    pub fn best(&self) -> Option<(&str, f64)> {
        self.0
            .iter()
            .fold(None, |best: Option<(&str, f64)>, (n, &p)| match best {
                Some((_, q)) if q >= p => best,
                _ => Some((n, p)),
            })
    }

    fn add(&mut self, node: &str, cfg: &NavLoop) {
        if !self.0.contains_key(node) {
            let share = if self.0.is_empty() {
                1.0
            } else {
                cfg.jump / self.0.len() as f64
            };
            self.0.insert(node.to_string(), share);
        }
    }

    fn normalize(&mut self) {
        let total: f64 = self.0.values().sum();
        let n = self.0.len() as f64;
        for p in self.0.values_mut() {
            *p = if total > 0.0 { *p / total } else { 1.0 / n };
        }
    }

    /// Walking `walked_m` since the last result.
    pub fn predict(&mut self, graph: &Graph, walked_m: f64, cfg: &NavLoop) {
        if self.0.is_empty() {
            self.0 = graph.nodes.iter().map(|n| (n.clone(), 1.0)).collect(); // uniform, once normalized
        }
        let mut next: BTreeMap<String, f64> = self.0.keys().map(|n| (n.clone(), 0.0)).collect();
        for (a, &p) in &self.0 {
            let rates: Vec<(&String, f64)> = graph
                .links
                .get(a)
                .into_iter()
                .flatten()
                .map(|(b, length)| (b, walked_m.max(0.0) / length))
                .collect();
            let total: f64 = rates.iter().map(|(_, r)| r).sum();
            let leave = 1.0 - (-total).exp();
            *next.entry(a.clone()).or_default() += p * (1.0 - leave);
            for (b, r) in rates.into_iter().filter(|_| total > 0.0) {
                *next.entry(b.clone()).or_default() += p * leave * r / total;
            }
        }
        let share = cfg.jump / next.len().max(1) as f64;
        for p in next.values_mut() {
            *p = (1.0 - cfg.jump) * *p + share;
        }
        self.0 = next;
        self.normalize();
    }

    /// A `localize` result.
    pub fn update(&mut self, found: &Value, cfg: &NavLoop) {
        let listed: Vec<(&str, f64, bool)> = found["candidates"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| {
                Some((
                    c["node"].as_str()?,
                    c["score"].as_f64()?,
                    c["plausible"] == false,
                ))
            })
            .collect();
        let Some(best) = listed.iter().map(|c| c.1).reduce(f64::max) else {
            return;
        };
        let margin = found["margin"].as_f64().unwrap_or(0.0);
        let floor = listed.iter().map(|c| c.1).fold(best - margin, f64::min);
        listed.iter().for_each(|(n, _, _)| self.add(n, cfg));
        for (n, p) in self.0.iter_mut() {
            let (score, too_far) = listed
                .iter()
                .find(|c| c.0 == n)
                .map_or((floor, false), |c| (c.1, c.2));
            *p *= ((score - best) / cfg.temperature).exp() * if too_far { cfg.gate } else { 1.0 };
        }
        self.normalize();
    }
}

pub struct Pipeline {
    http: reqwest::Client,
    pub nav_url: String,
    pub default_map: String, // NAV_MAP_ID: the map of a phone that doesn't choose one
    nav_token: String,
    nav_trust: String,
    pub nav_loop: NavLoop,
    pub engine: String,
    eleven: ElevenLabs,
    jev_url: String,
    pub jev_api_key: String,
    jev_model: String,
    jev_min_confidence: f64,
}

const ELEVENLABS_URL: &str = "https://api.elevenlabs.io";

/// ElevenLabs settings for both directions (contracts.md section 4).
struct ElevenLabs {
    url: String, // ELEVENLABS_URL: the real API, or the fakes
    key: String,
    stt_model: String,
    tts_model: String,
    voice_id: String, // no default: picked by the output owner (S06)
    language: String,
}

/// Settings lookup: the process environment.
pub type Vars<'a> = &'a dyn Fn(&str) -> Option<String>;

pub fn var(get: Vars, name: &str, default: &str) -> String {
    get(name).unwrap_or_else(|| default.to_string())
}

fn says(text: &str, phrase: &str) -> bool {
    Regex::new(&format!(r"\b{}\b", regex::escape(&phrase.to_lowercase())))
        .is_ok_and(|re| re.is_match(text))
}

/// Jev Choice question for the Command step. Option keys are destination IDs plus `cancel` and `unsupported`.
pub fn command_question(destinations: &[Destination]) -> Value {
    let mut criteria = serde_json::Map::new();
    for d in destinations {
        criteria.insert(
            d.destination_id.clone(),
            json!(format!("Wants to go to the {}.", d.label)),
        );
    }
    criteria.insert(
        "cancel".into(),
        json!("Wants to stop, cancel or end the guidance."),
    );
    criteria.insert(
        "unsupported".into(),
        json!("Wants something else: another place, a question, or nothing clear."),
    );
    json!({
        "type": "choice",
        "instructions": "What is the blind user asking for in this spoken request? \
    They are being guided indoors and can only be taken to the listed places.",
        "criteria": criteria,
    })
}

/// What the trace keeps of a Jev Choice answer.
fn jev_trace(answer: &Value) -> Value {
    json!({
        "source": "jev",
        "choice": answer["choice"],
        "confidence": answer["confidence"],
        "probabilities": answer["probabilities"],
    })
}

/// Low confidence means "ask again", never a guess.
pub fn parse_command(answer: &Value, destinations: &[Destination], min_confidence: f64) -> Command {
    if !answer["confidence"]
        .as_f64()
        .is_some_and(|c| c >= min_confidence)
    {
        return Command::Unclear;
    }
    match answer["choice"].as_str() {
        Some("cancel") => Command::Cancel,
        Some("unsupported") => Command::Unsupported,
        Some(id) if destinations.iter().any(|d| d.destination_id == id) => {
            Command::Start(id.into())
        }
        _ => Command::Unclear,
    }
}

/// Keyword fallback used when no TypeSafe key is configured.
pub fn match_command(transcript: &str, destinations: &[Destination]) -> Command {
    let text = transcript.to_lowercase();
    if CANCEL_WORDS.iter().any(|w| says(&text, w)) {
        return Command::Cancel;
    }
    destinations
        .iter()
        .find(|d| says(&text, &d.label))
        .map_or(Command::Unsupported, |d| {
            Command::Start(d.destination_id.clone())
        })
}

/// Jev Choice question for the worker: is a new direction worth saying out loud?
pub fn speak_question() -> Value {
    json!({
        "type": "choice",
        "instructions": "A blind user is being guided indoors by voice. The route planner gave a new direction. \
    Should it be spoken now? Every spoken sentence interrupts the user, so only speak what they need to act on.",
        "criteria": {
            "speak": "The new direction changes what the user must do now: a turn, a new stretch, a correction, \
    or something that tells them they are on track after a long silence.",
            "quiet": "The new direction repeats or only rephrases what was last said, \
    and does not change what the user should do.",
        },
    })
}

/// Only a confident "quiet" keeps a changed direction unsaid.
pub fn parse_speak(answer: &Value, min_confidence: f64) -> bool {
    !(answer["choice"] == "quiet"
        && answer["confidence"]
            .as_f64()
            .is_some_and(|c| c >= min_confidence))
}

/// The instruction, or as many of its first sentences as fit in a spoken message.
///
/// `fit_instruction("Go through the door. Bear right …")` gives `Some("Go through the door.")` when the whole is too long.
pub fn fit_instruction(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if text.chars().count() <= MAX_INSTRUCTION_CHARS {
        return Some(text.into());
    }
    let head: String = text.chars().take(MAX_INSTRUCTION_CHARS).collect();
    head.rfind(". ").map(|i| head[..=i].to_string())
}

pub fn template(
    action: &str,
    direction: Option<&str>,
    uncertain: bool,
    destination_label: &str,
) -> String {
    match (action, direction) {
        ("turn", Some("around")) => "Turn around.".into(),
        ("turn", Some(d)) => format!("Turn {d}."),
        ("continue", _) => "Keep going straight.".into(),
        ("arrived", _) => format!("You have arrived at the {destination_label}."),
        ("stop", _) => "Stop.".into(),
        _ if uncertain => "Please hold still, I need a clearer view.".into(),
        _ => "Wait a moment.".into(),
    }
}

impl Pipeline {
    pub fn from_vars(get: Vars) -> Self {
        let env = |name, default| var(get, name, default);
        Pipeline {
            http: reqwest::Client::new(),
            nav_url: env("NAV_URL", "http://localhost:8001"),
            default_map: env("NAV_MAP_ID", "itnig"),
            nav_token: env("NAV_API_TOKEN", ""),
            nav_trust: env("NAV_TRUST", "any"),
            nav_loop: NavLoop {
                burst: env("NAV_BURST", "1").parse().expect("NAV_BURST"),
                temperature: env("NAV_BELIEF_TEMPERATURE", "0.18")
                    .parse()
                    .expect("NAV_BELIEF_TEMPERATURE"),
                gate: env("NAV_BELIEF_GATE", "0.8")
                    .parse()
                    .expect("NAV_BELIEF_GATE"),
                jump: env("NAV_BELIEF_JUMP", "0.1")
                    .parse()
                    .expect("NAV_BELIEF_JUMP"),
                walk_scale: env("NAV_BELIEF_WALK_SCALE", "3.7")
                    .parse()
                    .expect("NAV_BELIEF_WALK_SCALE"),
                walk_m_per_call: env("NAV_BELIEF_WALK_M_PER_CALL", "1.5")
                    .parse()
                    .expect("NAV_BELIEF_WALK_M_PER_CALL"),
                reach: env("NAV_BELIEF_REACH", "0.7")
                    .parse()
                    .expect("NAV_BELIEF_REACH"),
                leave: env("NAV_BELIEF_LEAVE", "0.7")
                    .parse()
                    .expect("NAV_BELIEF_LEAVE"),
            },
            engine: env("NAV_ENGINE", "nav-engine"),
            eleven: ElevenLabs {
                url: env("ELEVENLABS_URL", ELEVENLABS_URL),
                key: env("ELEVENLABS_API_KEY", ""),
                stt_model: env("ELEVENLABS_STT_MODEL", "scribe_v2"),
                tts_model: env("ELEVENLABS_TTS_MODEL", "eleven_flash_v2_5"),
                voice_id: env("ELEVENLABS_VOICE_ID", ""),
                language: env("SPEECH_LANGUAGE", "en"),
            },
            jev_url: env("JEV_URL", "https://api.typesafe.ai/v1/systemone"),
            jev_api_key: env("TYPESAFE_API_KEY", ""),
            jev_model: env("JEV_MODEL", "jev-latest"),
            jev_min_confidence: env("JEV_MIN_CONFIDENCE", "0.5")
                .parse()
                .expect("JEV_MIN_CONFIDENCE"),
        }
    }

    /// One TypeSafe System One call: a state plus typed questions, answers keyed like the questions.
    async fn jev(
        &self,
        state: Value,
        questions: Value,
        timeout_ms: u64,
    ) -> Result<Value, BoxError> {
        let body = json!({"model": self.jev_model, "state": state, "questions": questions});
        let r: Value = self
            .http
            .post(&self.jev_url)
            .bearer_auth(&self.jev_api_key)
            .json(&body)
            .timeout(Duration::from_millis(timeout_ms))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(r["answers"].clone())
    }

    /// Speech needs a key, unless ELEVENLABS_URL points at fakes.
    pub fn speech_configured(&self) -> bool {
        !self.eleven.key.is_empty() || self.eleven.url != ELEVENLABS_URL
    }

    /// ElevenLabs Scribe, batch: the phone's audio as received, no transcoding, biased towards the destination names.
    pub async fn transcribe(
        &self,
        audio: Vec<u8>,
        content_type: &str,
        destinations: &[Destination],
    ) -> Result<String, BoxError> {
        let e = &self.eleven;
        let file = Part::bytes(audio)
            .file_name("audio")
            .mime_str(content_type)?;
        let mut form = Form::new()
            .text("model_id", e.stt_model.clone())
            .text("language_code", e.language.clone())
            .text("tag_audio_events", "false")
            .part("file", file);
        for d in destinations {
            form = form.text("keyterms", d.label.clone());
        }
        let r: Value = self
            .http
            .post(format!("{}/v1/speech-to-text", e.url))
            .header("xi-api-key", &e.key)
            .multipart(form)
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(r["text"].as_str().ok_or("no text")?.to_string())
    }

    /// ElevenLabs Flash, streamed. Fails if the stream has not started within 3 s.
    pub async fn speak(&self, text: &str) -> Result<Audio, BoxError> {
        let e = &self.eleven;
        if e.voice_id.is_empty() {
            return Err("ELEVENLABS_VOICE_ID is not set".into());
        }
        let request = self
            .http
            .post(format!(
                "{}/v1/text-to-speech/{}/stream?output_format=mp3_44100_64",
                e.url, e.voice_id
            ))
            .header("xi-api-key", &e.key)
            .json(&json!({"text": text, "model_id": e.tts_model, "language_code": e.language}))
            .send();
        let r = tokio::time::timeout(Duration::from_secs(3), request)
            .await??
            .error_for_status()?;
        Ok(r.bytes_stream().map_err(BoxError::from).boxed())
    }

    /// The command and Jev's answer behind it, as a small trace: its choice, confidence and probabilities.
    pub async fn command(
        &self,
        transcript: &str,
        phase: &str,
        destinations: &[Destination],
    ) -> (Command, Value) {
        if self.jev_api_key.is_empty() {
            let command = match_command(transcript, destinations);
            return (command, json!({"source": "keywords"}));
        }
        let state = json!({"spokenRequest": transcript, "sessionPhase": phase});
        let questions = json!({"command": command_question(destinations)});
        match self.jev(state, questions, 3000).await {
            Ok(answers) => {
                let answer = &answers["command"];
                let command = parse_command(answer, destinations, self.jev_min_confidence);
                (command, jev_trace(answer))
            }
            Err(e) => (
                Command::Unclear,
                json!({"source": "jev", "error": e.to_string()}),
            ),
        }
    }

    /// Asks Jev whether a changed direction is worth saying. Any failure says it.
    /// The second value is Jev's answer for the trace.
    pub async fn worth_saying(&self, state: Value) -> (bool, Value) {
        let questions = json!({"speak": speak_question()});
        match self.jev(state, questions, 2000).await {
            Ok(answers) => {
                let answer = &answers["speak"];
                (
                    parse_speak(answer, self.jev_min_confidence),
                    jev_trace(answer),
                )
            }
            Err(e) => (true, json!({"source": "jev", "error": e.to_string()})),
        }
    }

    /// A request to nav-api for the caller's map, with the caller's ids and the token when one is set.
    fn nav(&self, path: &str, caller: &Caller) -> reqwest::RequestBuilder {
        let mut r = self
            .http
            .post(format!("{}/maps/{}/{path}", self.nav_url, caller.map))
            .header("X-Client-Id", &caller.client)
            .header("X-Request-Id", &caller.request);
        if let Some(session) = &caller.session {
            r = r.header("X-Session-Id", session);
        }
        self.authed(r)
    }

    fn authed(&self, r: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.nav_token.is_empty() {
            r
        } else {
            r.bearer_auth(&self.nav_token)
        }
    }

    /// `GET /maps/{map_id}`: the place a phone chose, or None when nav-api has no such map.
    pub async fn place(&self, map_id: &str) -> Result<Option<Place>, BoxError> {
        let r = self
            .authed(self.http.get(format!("{}/maps/{map_id}", self.nav_url)))
            .timeout(Duration::from_secs(5))
            .send()
            .await?;
        if r.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let map: Value = r.error_for_status()?.json().await?;
        Ok(Some(
            Place::from_map(&map).ok_or("map: no map_id or nodes")?,
        ))
    }

    /// `GET /maps`: the published maps, as `{mapId, place}` for the phone to choose from, the default map first.
    pub async fn maps(&self) -> Result<Vec<Value>, BoxError> {
        let mut maps: Vec<Value> = self
            .authed(self.http.get(format!("{}/maps", self.nav_url)))
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        maps.sort_by_key(|m| m["map_id"] != self.default_map.as_str()); // stable: nav-api's order otherwise
        Ok(maps
            .iter()
            .map(|m| json!({"mapId": m["map_id"], "place": m["place"]}))
            .collect())
    }

    /// `POST /maps/{map}/localize`: which node the frames show (at most 4, oldest first, each with the phone's motion
    /// as it sent it), with the hop being followed or the last node (`ask`).
    pub async fn locate(
        &self,
        frames: Vec<(Option<Value>, Vec<u8>)>,
        ask: &Ask,
        caller: &Caller,
    ) -> Result<Value, BoxError> {
        let skip = frames.len().saturating_sub(MAX_LOCALIZE_FRAMES);
        let frames: Vec<_> = frames.into_iter().skip(skip).collect();
        // every node's score (nav-api's maximum): the belief weighs them all
        let mut form = Form::new().text("top_k", "20");
        if let Some(node) = &ask.previous {
            form = form.text("previous", node.clone());
        }
        if let Some(node) = &ask.expected {
            form = form.text("expected", node.clone());
        }
        let heading = frames
            .last()
            .and_then(|(m, _)| m.as_ref()?["headingDeg"].as_f64());
        if let Some(h) = heading {
            form = form.text("heading_deg", h.to_string());
        }
        if frames.iter().any(|(m, _)| m.is_some()) {
            // nav-api wants one motion part per image, `null` for a frame without one.
            for (m, _) in &frames {
                form = form.text("motion", m.as_ref().map_or("null".into(), Value::to_string));
            }
            if let (Some(_), Some(steps)) = (&ask.previous, ask.previous_step_count) {
                form = form.text("previous_step_count", steps.to_string());
            }
        }
        for (i, (_, frame)) in frames.into_iter().enumerate() {
            let part = Part::bytes(frame)
                .file_name(format!("frame{i}.jpg"))
                .mime_str("image/jpeg")?;
            form = form.part("images", part);
        }
        let out: Value = self
            .nav("localize", caller)
            .multipart(form)
            .timeout(Duration::from_secs(4))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if !out.is_object() {
            return Err("localize: not a JSON object".into());
        }
        Ok(out)
    }

    /// `POST /maps/{map}/route`: the route from `start` to `goal` over edges trusted as `NAV_TRUST`.
    pub async fn path(&self, start: &str, goal: &str, caller: &Caller) -> Result<Value, BoxError> {
        let out: Value = self
            .nav("route", caller)
            .json(&json!({"start": start, "goal": goal, "trust": self.nav_trust}))
            .timeout(Duration::from_secs(2))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if !out.is_object() {
            return Err("route: not a JSON object".into());
        }
        Ok(out)
    }

    /// Turn a route from `node` into an Output: its first hop (`hop_output`), or `wait` when nav-api found none.
    pub fn validate(&self, route: &Value, node: &str) -> Output {
        if route["found"] != true {
            return Output::wait(Some(node), false);
        }
        self.hop_output(&route["hops"][0], node)
    }

    /// The output for walking `hop` from `node`: `turn` when its first step is a turn, otherwise `continue`, with the
    /// hop's instruction cut to whole sentences; `wait` when the hop doesn't start at `node`.
    pub fn hop_output(&self, hop: &Value, node: &str) -> Output {
        let next = hop["target"].as_str().filter(|n| !n.is_empty());
        if hop["source"] != node || next.is_none() {
            return Output::wait(Some(node), false);
        }
        let direction = match hop["steps"][0]["action"].as_str() {
            Some("turn_left") => Some("left"),
            Some("turn_right") => Some("right"),
            Some("turn_around") => Some("around"),
            _ => None,
        };
        Output {
            action: if direction.is_some() {
                "turn"
            } else {
                "continue"
            }
            .into(),
            direction: direction.map(String::from),
            step: Some(node.into()),
            next: next.map(String::from),
            instruction: hop["instruction"].as_str().and_then(fit_instruction),
            uncertain: false,
        }
    }

    /// The worker's rule: speak only when the output differs from the session's previous output.
    pub fn should_speak(&self, output: &Output, previous: Option<&Output>) -> (bool, &'static str) {
        match previous {
            None => (true, "first"),
            Some(p) if p != output => (true, "changed"),
            _ => (false, "unchanged"),
        }
    }
}

/// Words in a sentence, lowercased and without punctuation.
fn words(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !(c.is_alphanumeric() || c == '\''))
        .filter(|w| !w.is_empty())
        .map(str::to_string)
        .collect()
}

/// Length of the longest run of consecutive words that both lists share.
fn longest_common_run(a: &[String], b: &[String]) -> usize {
    let mut best = 0;
    let mut row = vec![0usize; b.len() + 1];
    for x in a {
        let mut diagonal = 0;
        for (j, y) in b.iter().enumerate() {
            let above = row[j + 1];
            row[j + 1] = if x == y { diagonal + 1 } else { 0 };
            best = best.max(row[j + 1]);
            diagonal = above;
        }
    }
    best
}

/// Words in a row a transcript must share with a spoken sentence to count as that sentence heard again.
const ECHO_RUN_WORDS: usize = 5;

/// True when `transcript` is a sentence the app itself just had spoken, picked up by a microphone: the
/// user's own phone, or another phone in the room. Short sentences must match exactly. Longer ones match
/// when five words in a row are the same, which a natural answer such as "I would like to go to the
/// kitchen" does not reach even when it reuses the question's words.
pub fn is_echo(transcript: &str, spoken: &[String]) -> bool {
    let heard = words(transcript);
    if heard.len() < 2 {
        return false;
    }
    spoken.iter().any(|sentence| {
        let said = words(sentence);
        match said.len() {
            0 | 1 => false,
            2..=4 => heard == said,
            _ => longest_common_run(&heard, &said) >= ECHO_RUN_WORDS,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn out(action: &str, direction: Option<&str>, step: &str, uncertain: bool) -> Output {
        Output {
            action: action.into(),
            direction: direction.map(String::from),
            step: Some(step.into()),
            next: None,
            instruction: None,
            uncertain,
        }
    }

    #[test]
    fn should_read_settings_from_the_lookup_when_given() {
        let p = Pipeline::from_vars(&|k| (k == "NAV_URL").then(|| "http://nav".to_string()));
        assert_eq!(p.nav_url, "http://nav");
    }

    const CFG: NavLoop = NavLoop {
        burst: 1,
        temperature: 0.03,
        gate: 0.2,
        jump: 0.01,
        walk_scale: 1.0,
        walk_m_per_call: 0.0,
        reach: 0.7,
        leave: 0.8,
    };

    /// n1 - n2 (1.4 m), n2 - n4 (2 m), n2 - n3 (3.5 m), n2 - n5 (3 m); n7 linked to nothing.
    fn graph() -> Graph {
        let nodes = ["n1", "n2", "n3", "n4", "n5", "n7"].map(|n| json!({"id": n, "name": n}));
        Graph::from_map(&json!({
            "nodes": nodes,
            "links": [{"source": "n1", "target": "n2", "distance_m": 1.4}, {"source": "n2", "target": "n4", "distance_m": 2.0},
                      {"source": "n3", "target": "n2", "distance_m": 3.5}, {"source": "n2", "target": "n5", "distance_m": 3.0}],
            "edges": [{"source": "n1", "target": "n7", "distance_m": 1.0}],
            "frame": {"live_step_length_m": 0.31},
        }))
    }

    /// Every node listed: the named ones with their scores, the others at 0.2.
    fn scores(named: &[(&str, f64)], too_far: &[&str]) -> Value {
        let mut all: Vec<(&str, f64)> = ["n1", "n2", "n3", "n4", "n5", "n7"]
            .iter()
            .map(|n| (*n, named.iter().find(|x| x.0 == *n).map_or(0.2, |x| x.1)))
            .collect();
        all.sort_by(|a, b| b.1.total_cmp(&a.1));
        let candidates: Vec<Value> = all
            .iter()
            .map(|(n, s)| {
                json!({"node": n, "score": s,
            "plausible": if too_far.contains(n) { json!(false) } else { Value::Null }})
            })
            .collect();
        json!({"status": "uncertain", "best": all[0].0, "margin": all[0].1 - all[1].1, "candidates": candidates})
    }

    #[test]
    fn should_read_the_graph_from_the_links_before_the_edges_and_the_live_step_length() {
        let g = graph();
        assert_eq!(g.nodes.len(), 6);
        assert_eq!(g.step_length_m, 0.31);
        assert!(g.links["n2"].contains(&("n3".into(), 3.5)) && !g.links.contains_key("n7"));
        let edges_only =
            Graph::from_map(&json!({"nodes": [], "edges": [{"source": "a", "target": "b"}]}));
        assert_eq!(
            edges_only.links["b"],
            vec![("a".to_string(), UNKNOWN_LENGTH_M)]
        );
        assert_eq!(edges_only.step_length_m, 0.7);
    }

    #[test]
    fn should_add_up_a_small_lead_that_keeps_up_where_one_result_is_not_enough() {
        let (g, mut b) = (graph(), Belief::default());
        b.predict(&g, 0.0, &CFG);
        b.update(&scores(&[("n2", 0.33), ("n3", 0.31)], &[]), &CFG);
        let (best, p) = b.best().unwrap();
        assert!(best == "n2" && p < 0.7, "{p}");
        for _ in 0..4 {
            b.predict(&g, 0.0, &CFG);
            b.update(&scores(&[("n2", 0.33), ("n3", 0.31)], &[]), &CFG);
        }
        let (best, p) = b.best().unwrap();
        assert!(best == "n2" && p > 0.7, "{p}");
    }

    #[test]
    fn should_move_belief_to_linked_nodes_the_nearer_sooner_when_walking_and_keep_it_standing() {
        let (g, cfg) = (graph(), NavLoop { jump: 0.0, ..CFG });
        let mut b = Belief(BTreeMap::from([("n2".to_string(), 1.0)]));
        b.predict(&g, 0.0, &cfg);
        assert_eq!(b.prob("n2"), 1.0);
        b.predict(&g, 1.0, &cfg);
        assert!(
            b.prob("n1") > b.prob("n4")
                && b.prob("n4") > b.prob("n5")
                && b.prob("n5") > b.prob("n3")
        );
        assert_eq!(b.prob("n7"), 0.0);
        assert!((b.0.values().sum::<f64>() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn should_weigh_less_a_node_nav_api_marks_farther_than_walked() {
        let g = graph();
        let (mut a, mut b) = (Belief::default(), Belief::default());
        a.predict(&g, 0.0, &CFG);
        b.predict(&g, 0.0, &CFG);
        a.update(&scores(&[("n5", 0.4)], &[]), &CFG);
        b.update(&scores(&[("n5", 0.4)], &["n5"]), &CFG);
        assert!(b.prob("n5") < a.prob("n5"));
    }

    #[test]
    fn should_learn_from_one_candidate_through_the_margin_and_take_in_nodes_the_map_lacks() {
        let g = graph();
        let mut b = Belief::default();
        b.predict(&g, 0.0, &CFG);
        b.update(
            &json!({"best": "n2", "margin": 0.1, "candidates": [{"node": "n2", "score": 0.4}]}),
            &CFG,
        );
        assert!(b.prob("n2") > 3.0 * b.prob("n3"));
        b.update(
            &json!({"best": "x9", "margin": 0.1, "candidates": [{"node": "x9", "score": 0.5}]}),
            &CFG,
        );
        assert!(b.prob("x9") > 0.0 && (b.0.values().sum::<f64>() - 1.0).abs() < 1e-9);
        let mut nothing = b.clone();
        nothing.update(&json!({"status": "lost", "candidates": []}), &CFG);
        assert_eq!(nothing, b);
    }

    #[test]
    fn should_turn_a_route_into_its_first_hop_or_wait() {
        let p = Pipeline::from_vars(&|_| None);
        let route = |action: &str| {
            json!({"found": true, "hops": [
                {"edge": "e2", "source": "n2", "target": "n3", "instruction": "Turn left at the bar.",
                 "steps": [{"action": action}]},
                {"edge": "e3", "source": "n3", "target": "n4", "instruction": "Bear right.", "steps": []}]})
        };
        let got = p.validate(&route("turn_left"), "n2");
        assert_eq!(
            (
                got.action.as_str(),
                got.direction.as_deref(),
                got.next.as_deref(),
                got.instruction.as_deref()
            ),
            (
                "turn",
                Some("left"),
                Some("n3"),
                Some("Turn left at the bar.")
            )
        );
        let got = p.validate(&route("bear_left"), "n2");
        assert_eq!((got.action.as_str(), got.direction), ("continue", None));
        let wait = out("wait", None, "n2", false);
        assert_eq!(p.validate(&json!({"found": false, "hops": []}), "n2"), wait);
        assert_eq!(p.validate(&json!({"found": true, "hops": []}), "n2"), wait);
        assert_eq!(
            p.validate(&route("straight"), "n1"),
            out("wait", None, "n1", false)
        );
        // the next hop, once the user reached n3
        let got = p.hop_output(&route("turn_left")["hops"][1], "n3");
        assert_eq!(
            (
                got.action.as_str(),
                got.next.as_deref(),
                got.instruction.as_deref()
            ),
            ("continue", Some("n4"), Some("Bear right."))
        );
    }

    #[test]
    fn should_cut_a_long_instruction_at_the_last_sentence_that_fits() {
        assert_eq!(
            fit_instruction("  Bear right.  "),
            Some("Bear right.".into())
        );
        assert_eq!(fit_instruction(" "), None);
        let long = format!("Go through the glass door. {}", "a".repeat(240));
        assert_eq!(
            fit_instruction(&long),
            Some("Go through the glass door.".into())
        );
        assert_eq!(fit_instruction(&"a".repeat(241)), None);
    }

    /// Itnig, 2026-10-10 (session 9a90bd61): nav-engine's cleaner joined two hops into one, Drinks area -> Corridor
    /// end, with this 263-character instruction. The phone said only its first sentence, the user stopped at the
    /// tables, 4 m short, and the hop never confirmed. nav-engine no longer joins hops and flags any instruction over
    /// 240 characters; this keeps the phone's side of that contract on record.
    #[test]
    fn should_say_only_what_fits_of_the_live_263_character_instruction() {
        let live = "Bear right slightly and keep down the narrow aisle for about 3 metres until a group of tables is \
                    on your right. Then, bear right and continue about 4 metres down the aisle until it opens out into \
                    a wider space; a fire extinguisher hangs on the wall on your right.";
        assert_eq!(live.chars().count(), 263);
        assert_eq!(
            fit_instruction(live).as_deref(),
            Some(
                "Bear right slightly and keep down the narrow aisle for about 3 metres until a group of tables \
                 is on your right."
            )
        );
    }

    #[test]
    fn should_say_a_changed_direction_unless_jev_is_sure_it_is_not_worth_it() {
        let answer = |choice: &str, confidence: f64| json!({"type": "choice", "choice": choice, "confidence": confidence});
        assert!(!parse_speak(&answer("quiet", 0.8), 0.5));
        assert!(parse_speak(&answer("quiet", 0.3), 0.5));
        assert!(parse_speak(&answer("speak", 0.9), 0.5));
        assert!(parse_speak(&json!(null), 0.5));
        let q = speak_question();
        let options: Vec<&String> = q["criteria"].as_object().unwrap().keys().collect();
        assert_eq!(options, ["quiet", "speak"]);
    }

    #[test]
    fn speaks_only_when_output_differs_from_previous() {
        let p = Pipeline::from_vars(&|_| None);
        let prev = out("turn", Some("left"), "corridor", false);
        assert_eq!(p.should_speak(&prev, None), (true, "first"));
        assert_eq!(p.should_speak(&prev, Some(&prev)), (false, "unchanged"));
        let right = out("turn", Some("right"), "corridor", false);
        assert_eq!(p.should_speak(&right, Some(&prev)), (true, "changed"));
        let unsure = out("wait", None, "corridor", true);
        assert!(
            p.should_speak(&unsure, Some(&out("wait", None, "corridor", false)))
                .0
        );
    }

    /// A small map as nav-api serves it (`GET /maps/{map_id}`), with the fields `Place` reads.
    fn place() -> Place {
        Place::from_map(&json!({"map_id": "itnig", "place": "Itnig", "nodes": [
            {"id": "n2", "name": "drinks area", "kind": "landmark"},
            {"id": "n7", "name": "kitchen", "kind": "room"},
            {"id": "n8", "name": "stage", "kind": "destination"},
        ]}))
        .unwrap()
    }

    #[test]
    fn every_node_of_the_map_is_a_destination_by_its_name() {
        let p = place();
        assert_eq!(p.map_id, "itnig");
        let names: Vec<(&str, &str)> = p
            .destinations
            .iter()
            .map(|d| (d.destination_id.as_str(), d.label.as_str()))
            .collect();
        assert_eq!(
            names,
            [("n2", "drinks area"), ("n7", "kitchen"), ("n8", "stage")]
        );
        assert!(Place::from_map(&json!({"detail": "not found"})).is_none());
    }

    #[test]
    fn match_command_uses_whole_words() {
        let d = &place().destinations;
        assert_eq!(
            match_command("Take me to the drinks area", d),
            Command::Start("n2".into())
        );
        assert_eq!(
            match_command("where is the stage?", d),
            Command::Start("n8".into())
        );
        assert_eq!(match_command("cancel please", d), Command::Cancel);
        assert_eq!(match_command("barcelona", d), Command::Unsupported);
    }

    #[test]
    fn jev_command_is_one_choice_and_low_confidence_asks_again() {
        let d = &place().destinations;
        let q = command_question(d);
        let options: Vec<&String> = q["criteria"].as_object().unwrap().keys().collect();
        assert_eq!(options, ["cancel", "n2", "n7", "n8", "unsupported"]);
        assert_eq!(q["criteria"]["n7"], "Wants to go to the kitchen.");
        // Answer shape from https://docs.typesafe.ai/introduction/quickstart
        let answer = |choice: &str, confidence: f64| json!({"type": "choice", "choice": choice, "confidence": confidence, "probabilities": {}});
        assert_eq!(
            parse_command(&answer("n2", 0.8), d, 0.5),
            Command::Start("n2".into())
        );
        assert_eq!(
            parse_command(&answer("cancel", 0.9), d, 0.5),
            Command::Cancel
        );
        assert_eq!(parse_command(&answer("n2", 0.3), d, 0.5), Command::Unclear);
        assert_eq!(
            parse_command(&answer("kitchen", 0.9), d, 0.5),
            Command::Unclear
        );
        assert_eq!(parse_command(&json!(null), d, 0.5), Command::Unclear);
    }

    #[test]
    fn should_recognise_the_app_s_own_sentence_heard_back() {
        let spoken = vec!["Please hold still, I need a clearer view.".to_string()];
        let garbled = "Please hold still. I need to clear the UI. Que mas va a estar?";
        assert!(is_echo(garbled, &spoken));
        assert!(is_echo("please hold still i need a clearer view", &spoken));
        let prompt = vec!["Where would you like to go?".to_string()];
        assert!(is_echo("Where would you like to go?", &prompt));
    }

    #[test]
    fn should_not_mistake_a_natural_answer_for_an_echo() {
        let prompt = vec!["Where would you like to go?".to_string()];
        assert!(!is_echo("I would like to go to the kitchen", &prompt));
        assert!(!is_echo("the kitchen", &prompt));
        let places = "I can take you to the drinks area or the kitchen or the stage.";
        let reask = vec![format!(
            "I can't guide you there yet. {places} Where would you like to go?"
        )];
        assert!(!is_echo("I want to go to the kitchen", &reask));
        assert!(!is_echo("take me to the stage", &reask));
    }

    #[test]
    fn should_match_short_sentences_only_exactly() {
        let spoken = vec!["Turn left.".to_string()];
        assert!(is_echo("turn left", &spoken));
        assert!(!is_echo("turn left at the kitchen", &spoken));
        assert!(!is_echo("kitchen", &spoken));
        assert!(!is_echo("", &spoken));
    }

    #[test]
    fn should_keep_jev_choice_confidence_and_probabilities_for_the_trace() {
        let answer = json!({
            "type": "choice", "choice": "n8", "confidence": 0.9,
            "probabilities": {"n8": 0.9, "unsupported": 0.1},
        });
        let kept = jev_trace(&answer);
        assert_eq!(kept["source"], "jev");
        assert_eq!(kept["choice"], "n8");
        assert_eq!(kept["confidence"], 0.9);
        assert_eq!(kept["probabilities"]["unsupported"], 0.1);
        assert!(kept.get("type").is_none());
    }
}
