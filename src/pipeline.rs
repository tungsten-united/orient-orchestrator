//! Model calls behind the orchestrator: STT, command (TypeSafe Jev), navigation, the worker's
//! comparison rule, and the sentence templates.
//!
//! Every model output is validated here. Nothing in this module changes session state.

use std::collections::HashMap;
use std::time::Duration;

use regex::Regex;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use serde_json::{Value, json};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ACTIONS: [&str; 5] = ["wait", "turn", "continue", "arrived", "stop"];
const DIRECTIONS: [&str; 3] = ["left", "right", "around"];
const CANCEL_WORDS: [&str; 3] = ["cancel", "stop", "never mind"];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    pub route_id: String,
    pub start_step_id: String,
    pub steps: HashMap<String, Step>,
    pub destinations: Vec<Destination>,
}

#[derive(Deserialize)]
pub struct Step {
    pub hint: String,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Destination {
    pub destination_id: String,
    pub label: String,
    pub aliases: Vec<String>,
    pub steps: Vec<String>,
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
#[derive(Clone, Debug, PartialEq)]
pub struct Output {
    pub action: String,
    pub direction: Option<String>,
    pub step: String,
    pub uncertain: bool,
}

pub struct Pipeline {
    http: reqwest::Client,
    pub nav_url: String,
    pub engine: String,
    pub stt_url: String,
    jev_url: String,
    pub jev_api_key: String,
    jev_model: String,
    jev_min_confidence: f64,
    pub min_confidence: f64,
    pub repeat_ms: i64,
}

pub fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
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
    pub fn from_env() -> Self {
        Pipeline {
            http: reqwest::Client::new(),
            nav_url: env("NAV_URL", "http://localhost:8001"),
            engine: env("NAV_ENGINE", "vla"),
            stt_url: env("STT_URL", ""),
            jev_url: env("JEV_URL", "https://api.typesafe.ai/v1/systemone"),
            jev_api_key: env("TYPESAFE_API_KEY", ""),
            jev_model: env("JEV_MODEL", "jev-latest"),
            jev_min_confidence: env("JEV_MIN_CONFIDENCE", "0.5")
                .parse()
                .expect("JEV_MIN_CONFIDENCE"),
            min_confidence: env("MIN_CONFIDENCE", "0.5")
                .parse()
                .expect("MIN_CONFIDENCE"),
            repeat_ms: env("REPEAT_MS", "7000").parse().expect("REPEAT_MS"),
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

    pub async fn transcribe(&self, audio: Vec<u8>, content_type: &str) -> Result<String, BoxError> {
        // ponytail: open point 1. Any HTTP STT that returns {"transcript": ...}; Jev only takes text and JSON state.
        let part = Part::bytes(audio)
            .file_name("audio")
            .mime_str(content_type)?;
        let r: Value = self
            .http
            .post(&self.stt_url)
            .multipart(Form::new().part("audio", part))
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(r["transcript"].as_str().ok_or("no transcript")?.to_string())
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

    /// Sends the session's last frames, oldest first, as repeated `frames` parts.
    pub async fn navigate(&self, frames: Vec<Vec<u8>>, meta: Value) -> Result<Value, BoxError> {
        let mut form = Form::new().text("meta", meta.to_string());
        for (i, frame) in frames.into_iter().enumerate() {
            let part = Part::bytes(frame)
                .file_name(format!("frame{i}.jpg"))
                .mime_str("image/jpeg")?;
            form = form.part("frames", part);
        }
        let out: Value = self
            .http
            .post(format!("{}/v1/navigate", self.nav_url))
            .multipart(form)
            .timeout(Duration::from_secs(4))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if !out.is_object() {
            return Err("navigation output is not a JSON object".into());
        }
        Ok(out)
    }

    /// Turn a VLA answer into an Output. Only the route can move the step.
    pub fn validate(&self, nav: &Value, step: &str, path: &[String]) -> Output {
        let next = path
            .iter()
            .position(|p| p == step)
            .and_then(|i| path.get(i + 1));
        let action = nav["action"].as_str().unwrap_or("");
        let direction = nav["direction"].as_str();
        let proposed = nav["proposedNextStepId"].as_str().unwrap_or(step);
        let wait = |uncertain| Output {
            action: "wait".into(),
            direction: None,
            step: step.into(),
            uncertain,
        };

        if !ACTIONS.contains(&action)
            || (proposed != step && next.map(String::as_str) != Some(proposed))
        {
            return wait(false);
        }
        if !nav["confidence"]
            .as_f64()
            .is_some_and(|c| c >= self.min_confidence)
        {
            return wait(true);
        }
        if action == "arrived" && path.last().map(String::as_str) != Some(proposed) {
            return wait(false);
        }
        if action == "turn" && !direction.is_some_and(|d| DIRECTIONS.contains(&d)) {
            return wait(false);
        }
        Output {
            action: action.into(),
            direction: if action == "turn" {
                direction.map(String::from)
            } else {
                None
            },
            step: proposed.into(),
            uncertain: false,
        }
    }

    /// The worker's rule: speak when the output differs from the session's previous output,
    /// or as a reminder after `repeat_ms` of the same output.
    pub fn should_speak(
        &self,
        output: &Output,
        previous: Option<&Output>,
        last_spoken_at: i64,
        now: i64,
    ) -> (bool, &'static str) {
        match previous {
            None => (true, "first"),
            Some(p) if p != output => (true, "changed"),
            _ if now - last_spoken_at >= self.repeat_ms => (true, "repeat_interval"),
            _ => (false, "unchanged"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> Vec<String> {
        ["start", "corridor", "counter"].map(String::from).to_vec()
    }

    fn out(action: &str, direction: Option<&str>, step: &str, uncertain: bool) -> Output {
        Output {
            action: action.into(),
            direction: direction.map(String::from),
            step: step.into(),
            uncertain,
        }
    }

    #[test]
    fn validate_only_moves_along_the_route() {
        let p = Pipeline::from_env();
        let ok = json!({"action": "continue", "proposedNextStepId": "corridor", "confidence": 0.9});
        assert_eq!(
            p.validate(&ok, "start", &path()),
            out("continue", None, "corridor", false)
        );
        let skip =
            json!({"action": "continue", "proposedNextStepId": "counter", "confidence": 0.9});
        assert_eq!(p.validate(&skip, "start", &path()).action, "wait");
        let unsure =
            json!({"action": "continue", "proposedNextStepId": "corridor", "confidence": 0.2});
        assert_eq!(
            p.validate(&unsure, "start", &path()),
            out("wait", None, "start", true)
        );
        let early =
            json!({"action": "arrived", "proposedNextStepId": "corridor", "confidence": 0.9});
        assert_eq!(p.validate(&early, "start", &path()).action, "wait");
        let turn = json!({"action": "turn", "direction": "up", "confidence": 0.9});
        assert_eq!(p.validate(&turn, "start", &path()).action, "wait");
    }

    #[test]
    fn speaks_only_when_output_differs_from_previous() {
        let p = Pipeline::from_env();
        let prev = out("turn", Some("left"), "corridor", false);
        assert_eq!(p.should_speak(&prev, None, 0, 1000), (true, "first"));
        assert_eq!(
            p.should_speak(&prev, Some(&prev), 0, 1000),
            (false, "unchanged")
        );
        let right = out("turn", Some("right"), "corridor", false);
        assert_eq!(
            p.should_speak(&right, Some(&prev), 0, 1000),
            (true, "changed")
        );
        let unsure = out("wait", None, "corridor", true);
        assert!(
            p.should_speak(
                &unsure,
                Some(&out("wait", None, "corridor", false)),
                0,
                1000
            )
            .0
        );
        assert_eq!(
            p.should_speak(&prev, Some(&prev), 0, p.repeat_ms),
            (true, "repeat_interval")
        );
    }

    #[test]
    fn match_command_uses_whole_words() {
        let route: Route = serde_json::from_str(include_str!("route.json")).unwrap();
        let d = &route.destinations;
        assert_eq!(
            match_command("Take me to the coffee", d),
            Command::Start("counter".into())
        );
        assert_eq!(
            match_command("where is the toilet?", d),
            Command::Start("bathroom".into())
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
        assert_eq!(options, ["bathroom", "cancel", "counter", "unsupported"]);
        // Answer shape from https://docs.typesafe.ai/introduction/quickstart
        let answer = |choice: &str, confidence: f64| json!({"type": "choice", "choice": choice, "confidence": confidence, "probabilities": {}});
        assert_eq!(
            parse_command(&answer("counter", 0.8), d, 0.5),
            Command::Start("counter".into())
        );
        assert_eq!(
            parse_command(&answer("cancel", 0.9), d, 0.5),
            Command::Cancel
        );
        assert_eq!(
            parse_command(&answer("counter", 0.3), d, 0.5),
            Command::Unclear
        );
        assert_eq!(
            parse_command(&answer("kitchen", 0.9), d, 0.5),
            Command::Unclear
        );
        assert_eq!(parse_command(&json!(null), d, 0.5), Command::Unclear);
    }
}
