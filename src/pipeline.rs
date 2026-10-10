//! Model calls behind the orchestrator: speech (ElevenLabs), command (TypeSafe Jev), navigation, the worker's
//! comparison rule, and the sentence templates.
//!
//! Every model output is validated here. Nothing in this module changes session state.

use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use regex::Regex;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// MP3 bytes as they arrive.
pub type Audio = BoxStream<'static, Result<Bytes, BoxError>>;

const CANCEL_WORDS: [&str; 3] = ["cancel", "stop", "never mind"];
const MAX_INSTRUCTION_CHARS: usize = 240;
const MAX_LOCALIZE_FRAMES: usize = 4;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    pub route_id: String,
    pub start_step_id: String,
    pub destinations: Vec<Destination>,
}

/// A place the user can ask for. `destination_id` is its node on the navigation engine's map.
#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Destination {
    pub destination_id: String,
    pub label: String,
    pub aliases: Vec<String>,
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

    pub fn arrived(step: &str) -> Self {
        Output {
            action: "arrived".into(),
            ..Output::wait(Some(step), false)
        }
    }
}

/// The navigation loop's settings (contracts.md section 2, "Navigation loop"). The defaults come from replaying the
/// Itnig walks through the same loop (nav-engine's `nav map orchestrator-replay`).
#[derive(Clone, Copy, Debug)]
pub struct NavLoop {
    pub burst: usize, // NAV_BURST: frames not sent to localize yet that start an evaluation
    pub k: usize,     // NAV_VOTE_K: votes needed ...
    pub n: usize,     // NAV_VOTE_N: ... among the last n localizations
    pub margin: f64, // NAV_MARGIN: a result votes only when its best node leads the second by this much
    pub lost_calls: u32, // NAV_LOST_CALLS: `lost` results in a row, while following, that start a new navigation
}

/// One localization's vote in the navigation loop.
#[derive(Clone, Debug, PartialEq)]
pub enum Vote {
    /// Locating: the user seems to be at this node.
    At(String),
    /// Following: the hop's target leads.
    Target,
    /// Following: a node that is neither the hop's source nor its target leads.
    Elsewhere,
    Abstain,
}

impl Vote {
    /// How the trace shows it.
    pub fn name(&self) -> String {
        match self {
            Vote::At(node) => node.clone(),
            Vote::Target => "target".into(),
            Vote::Elsewhere => "elsewhere".into(),
            Vote::Abstain => "none".into(),
        }
    }
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
    pub client: String,
    pub session: Option<String>,
    pub request: String,
}

/// The best node of a localization, when it leads the second by `margin` and the user can be there: the result
/// isn't `lost`, and nav-api doesn't mark it as farther than they walked (`plausible: false`).
fn leader(found: &Value, margin: f64) -> Option<&str> {
    let best = found["best"].as_str().filter(|n| !n.is_empty())?;
    let top = &found["candidates"][0];
    let too_far = top["node"] == best && top["plausible"] == false;
    let lead = found["margin"].as_f64().unwrap_or(0.0);
    (found["status"] != "lost" && lead >= margin && !too_far).then_some(best)
}

/// Locating: the leading node gets a vote.
pub fn locate_vote(found: &Value, margin: f64) -> Vote {
    leader(found, margin).map_or(Vote::Abstain, |n| Vote::At(n.into()))
}

/// Following the hop `source -> target`: has the user reached the target, or are they somewhere else?
pub fn follow_vote(found: &Value, source: &str, target: &str, margin: f64) -> Vote {
    match leader(found, margin) {
        Some(n) if n == target => Vote::Target,
        Some(n) if n != source => Vote::Elsewhere,
        _ => Vote::Abstain,
    }
}

/// Adds `vote` to the last `n` votes and counts the ones equal to it (none for an abstention).
pub fn tally(votes: &mut VecDeque<Vote>, vote: Vote, n: usize) -> usize {
    votes.push_back(vote.clone());
    while votes.len() > n.max(1) {
        votes.pop_front();
    }
    if vote == Vote::Abstain {
        0
    } else {
        votes.iter().filter(|v| **v == vote).count()
    }
}

pub struct Pipeline {
    http: reqwest::Client,
    pub nav_url: String,
    nav_map: String,
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
        let examples = d.aliases.join(", ");
        criteria.insert(
            d.destination_id.clone(),
            json!(format!(
                "Wants to go to the {}, for example: {examples}.",
                d.label
            )),
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
        .find(|d| says(&text, &d.label) || d.aliases.iter().any(|a| says(&text, a)))
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
            nav_map: env("NAV_MAP_ID", "itnig"),
            nav_token: env("NAV_API_TOKEN", ""),
            nav_trust: env("NAV_TRUST", "observed"),
            nav_loop: NavLoop {
                burst: env("NAV_BURST", "1").parse().expect("NAV_BURST"),
                k: env("NAV_VOTE_K", "3").parse().expect("NAV_VOTE_K"),
                n: env("NAV_VOTE_N", "4").parse().expect("NAV_VOTE_N"),
                margin: env("NAV_MARGIN", "0.04").parse().expect("NAV_MARGIN"),
                lost_calls: env("NAV_LOST_CALLS", "10").parse().expect("NAV_LOST_CALLS"),
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
            for term in std::iter::once(&d.label).chain(&d.aliases) {
                form = form.text("keyterms", term.clone());
            }
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

    /// A request to nav-api for the venue's map, with the caller's ids and the token when one is set.
    fn nav(&self, path: &str, caller: &Caller) -> reqwest::RequestBuilder {
        let mut r = self
            .http
            .post(format!("{}/maps/{}/{path}", self.nav_url, self.nav_map))
            .header("X-Client-Id", &caller.client)
            .header("X-Request-Id", &caller.request);
        if let Some(session) = &caller.session {
            r = r.header("X-Session-Id", session);
        }
        if self.nav_token.is_empty() {
            r
        } else {
            r.bearer_auth(&self.nav_token)
        }
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
        let mut form = Form::new();
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

    /// The node a localization puts the user at, only when nav-api confirmed it.
    pub fn located(&self, found: &Value) -> Option<String> {
        (found["status"] == "confirmed")
            .then(|| {
                found["best"]
                    .as_str()
                    .filter(|n| !n.is_empty())
                    .map(String::from)
            })
            .flatten()
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

    #[test]
    fn should_locate_only_on_a_confirmed_node() {
        let p = Pipeline::from_vars(&|_| None);
        let at =
            |status: &str| json!({"status": status, "best": "n4", "margin": 0.1, "candidates": []});
        assert_eq!(p.located(&at("confirmed")), Some("n4".into()));
        assert_eq!(p.located(&at("uncertain")), None);
        assert_eq!(p.located(&at("lost")), None);
        assert_eq!(
            p.located(&json!({"status": "confirmed", "best": null})),
            None
        );
    }

    #[test]
    fn should_vote_on_the_leading_node_only_when_it_clearly_leads() {
        let found = |status: &str, best: &str, margin: f64| {
            json!({"status": status, "best": best, "margin": margin,
                   "candidates": [{"node": best, "score": 0.4, "plausible": null}]})
        };
        // low scores are fine: what counts is which node leads
        assert_eq!(
            locate_vote(&found("uncertain", "n4", 0.05), 0.04),
            Vote::At("n4".into())
        );
        assert_eq!(
            locate_vote(&found("uncertain", "n4", 0.02), 0.04),
            Vote::Abstain
        );
        assert_eq!(locate_vote(&found("lost", "n4", 0.2), 0.04), Vote::Abstain);
        assert_eq!(
            locate_vote(&json!({"status": "lost", "candidates": []}), 0.04),
            Vote::Abstain
        );
        let mut too_far = found("uncertain", "n5", 0.1);
        too_far["candidates"][0]["plausible"] = json!(false);
        assert_eq!(locate_vote(&too_far, 0.04), Vote::Abstain);
        assert_eq!(follow_vote(&too_far, "n4", "n5", 0.04), Vote::Abstain);
        assert_eq!(
            follow_vote(&found("uncertain", "n5", 0.05), "n4", "n5", 0.04),
            Vote::Target
        );
        assert_eq!(
            follow_vote(&found("confirmed", "n4", 0.1), "n4", "n5", 0.04),
            Vote::Abstain
        );
        // the goal seen from afar is not the next node
        assert_eq!(
            follow_vote(&found("uncertain", "n8", 0.1), "n4", "n5", 0.04),
            Vote::Elsewhere
        );
    }

    #[test]
    fn should_count_votes_among_the_last_n() {
        let mut votes = VecDeque::new();
        assert_eq!(tally(&mut votes, Vote::Target, 4), 1);
        assert_eq!(tally(&mut votes, Vote::Abstain, 4), 0);
        assert_eq!(tally(&mut votes, Vote::Target, 4), 2);
        assert_eq!(tally(&mut votes, Vote::Elsewhere, 4), 1);
        assert_eq!(tally(&mut votes, Vote::Target, 4), 2); // the first Target fell out of the window
        assert_eq!(votes.len(), 4);
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

    #[test]
    fn match_command_uses_whole_words() {
        let route: Route = serde_json::from_str(include_str!("route.json")).unwrap();
        let d = &route.destinations;
        assert_eq!(
            match_command("Take me to the coffee", d),
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
        let route: Route = serde_json::from_str(include_str!("route.json")).unwrap();
        let d = &route.destinations;
        let q = command_question(d);
        let options: Vec<&String> = q["criteria"].as_object().unwrap().keys().collect();
        assert_eq!(options, ["cancel", "n2", "n7", "n8", "unsupported"]);
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
