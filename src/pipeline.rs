//! Model calls behind the orchestrator: speech (ElevenLabs), command (TypeSafe Jev), navigation, the worker's
//! comparison rule, and the sentence templates.
//!
//! Every model output is validated here. Nothing in this module changes session state.

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

pub struct Pipeline {
    http: reqwest::Client,
    pub nav_url: String,
    nav_map: String,
    nav_token: String,
    nav_trust: String,
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

    pub async fn command(
        &self,
        transcript: &str,
        phase: &str,
        destinations: &[Destination],
    ) -> Command {
        if self.jev_api_key.is_empty() {
            return match_command(transcript, destinations);
        }
        let state = json!({"spokenRequest": transcript, "sessionPhase": phase});
        let questions = json!({"command": command_question(destinations)});
        match self.jev(state, questions, 3000).await {
            Ok(answers) => {
                parse_command(&answers["command"], destinations, self.jev_min_confidence)
            }
            Err(_) => Command::Unclear,
        }
    }

    /// Asks Jev whether a changed direction is worth saying. Any failure says it.
    pub async fn worth_saying(&self, state: Value) -> bool {
        let questions = json!({"speak": speak_question()});
        match self.jev(state, questions, 2000).await {
            Ok(answers) => parse_speak(&answers["speak"], self.jev_min_confidence),
            Err(_) => true,
        }
    }

    /// A request to nav-api for the venue's map, with its token when one is set.
    fn nav(&self, path: &str) -> reqwest::RequestBuilder {
        let r = self
            .http
            .post(format!("{}/maps/{}/{path}", self.nav_url, self.nav_map));
        if self.nav_token.is_empty() {
            r
        } else {
            r.bearer_auth(&self.nav_token)
        }
    }

    /// `POST /maps/{map}/localize`: which node the session's last frames show (at most 4, oldest first),
    /// with the phone's compass heading when it has one.
    pub async fn locate(
        &self,
        frames: Vec<Vec<u8>>,
        previous: Option<&str>,
        heading_deg: Option<f64>,
    ) -> Result<Value, BoxError> {
        let skip = frames.len().saturating_sub(MAX_LOCALIZE_FRAMES);
        let mut form = Form::new();
        if let Some(node) = previous {
            form = form.text("previous", node.to_string());
        }
        if let Some(h) = heading_deg {
            form = form.text("heading_deg", h.to_string());
        }
        for (i, frame) in frames.into_iter().skip(skip).enumerate() {
            let part = Part::bytes(frame)
                .file_name(format!("frame{i}.jpg"))
                .mime_str("image/jpeg")?;
            form = form.part("images", part);
        }
        let out: Value = self
            .nav("localize")
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
    pub async fn path(&self, start: &str, goal: &str) -> Result<Value, BoxError> {
        let out: Value = self
            .nav("route")
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

    /// Turn a route from `node` into an Output: its first hop, with the turn its first step starts with.
    pub fn validate(&self, route: &Value, node: &str) -> Output {
        let hop = &route["hops"][0];
        let next = hop["target"].as_str().filter(|n| !n.is_empty());
        if route["found"] != true || hop["source"] != node || next.is_none() {
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
}
