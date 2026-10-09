//! Model calls behind the orchestrator: STT, command, navigation, decider, writer.
//!
//! Every model output is validated here. Nothing in this module changes session state.

use std::collections::HashMap;
use std::time::Duration;

use regex::Regex;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ACTIONS: [&str; 5] = ["wait", "turn", "continue", "arrived", "stop"];
const DIRECTIONS: [&str; 3] = ["left", "right", "around"];
const CANCEL_WORDS: [&str; 3] = ["cancel", "stop", "never mind"];
const MAX_TEXT: usize = 240;

const COMMAND_PROMPT: &str = "You turn a blind user's spoken request into a navigation command. \
Reply with JSON only: {\"command\": \"start\" | \"cancel\" | \"unsupported\", \"destinationId\": string | null, \
\"confidence\": number}. Only use a destinationId from the input list.";
const WRITER_PROMPT: &str = "You write one short spoken navigation instruction for a blind user, at most 20 words, \
naming the landmark when it helps. No preamble. Reply with JSON only: {\"text\": string}.";

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

#[derive(Clone)]
pub struct Spoken {
    pub action: String,
    pub route_step_id: String,
    pub uncertain: bool,
    pub at: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WriterInput {
    pub action: String,
    pub direction: Option<String>,
    pub route_step_id: String,
    pub destination_label: String,
    pub step_hint: String,
    pub uncertain: bool,
}

pub struct Pipeline {
    http: reqwest::Client,
    pub nav_url: String,
    pub engine: String,
    pub stt_url: String,
    pub jev_base_url: String,
    jev_api_key: String,
    jev_model: String,
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

/// Keyword fallback used when no Jev API is configured.
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
            jev_base_url: env("JEV_BASE_URL", ""),
            jev_api_key: env("JEV_API_KEY", ""),
            jev_model: env("JEV_MODEL", ""),
            min_confidence: env("MIN_CONFIDENCE", "0.5")
                .parse()
                .expect("MIN_CONFIDENCE"),
            repeat_ms: env("REPEAT_MS", "7000").parse().expect("REPEAT_MS"),
        }
    }

    async fn jev(&self, prompt: &str, payload: Value, timeout_ms: u64) -> Result<Value, BoxError> {
        // ponytail: assumes an OpenAI-compatible chat completions API with JSON mode.
        // Jev's wire format isn't documented yet; only this function changes when it is.
        let body = json!({
            "model": self.jev_model,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": payload.to_string()},
            ],
        });
        let r: Value = self
            .http
            .post(format!("{}/chat/completions", self.jev_base_url))
            .bearer_auth(&self.jev_api_key)
            .json(&body)
            .timeout(Duration::from_millis(timeout_ms))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let content = r["choices"][0]["message"]["content"]
            .as_str()
            .ok_or("no content")?;
        let out: Value = serde_json::from_str(content)?;
        if !out.is_object() {
            return Err("Jev output is not a JSON object".into());
        }
        Ok(out)
    }

    pub async fn transcribe(&self, audio: Vec<u8>, content_type: &str) -> Result<String, BoxError> {
        // ponytail: open point 1. Any HTTP STT that returns {"transcript": ...}; fold into jev() if Jev takes audio.
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
        if self.jev_base_url.is_empty() {
            return match_command(transcript, destinations);
        }
        let public: Vec<Value> = destinations
            .iter()
            .map(|d| json!({"destinationId": d.destination_id, "label": d.label, "aliases": d.aliases}))
            .collect();
        let payload = json!({"transcript": transcript, "phase": phase, "destinations": public});
        let Ok(out) = self.jev(COMMAND_PROMPT, payload, 3000).await else {
            return Command::Unclear;
        };
        match out["command"].as_str() {
            Some("start") => match out["destinationId"].as_str() {
                Some(id) if destinations.iter().any(|d| d.destination_id == id) => {
                    Command::Start(id.into())
                }
                _ => Command::Unsupported,
            },
            Some("cancel") => Command::Cancel,
            Some("unsupported") => Command::Unsupported,
            _ => Command::Unclear,
        }
    }

    pub async fn navigate(&self, frame: Vec<u8>, meta: Value) -> Result<Value, BoxError> {
        let form = Form::new().text("meta", meta.to_string()).part(
            "frame",
            Part::bytes(frame)
                .file_name("frame.jpg")
                .mime_str("image/jpeg")?,
        );
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

    /// Turn a VLA answer into (action, direction, new step, uncertain). Only the route can move the step.
    pub fn validate(
        &self,
        nav: &Value,
        step: &str,
        path: &[String],
    ) -> (String, Option<String>, String, bool) {
        let next = path
            .iter()
            .position(|p| p == step)
            .and_then(|i| path.get(i + 1));
        let action = nav["action"].as_str().unwrap_or("");
        let direction = nav["direction"].as_str();
        let proposed = nav["proposedNextStepId"].as_str().unwrap_or(step);
        let wait = |uncertain| ("wait".to_string(), None, step.to_string(), uncertain);

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
        let direction = if action == "turn" {
            direction.map(String::from)
        } else {
            None
        };
        (action.to_string(), direction, proposed.to_string(), false)
    }

    /// Utterance decider, rules version. Same contract if it moves to the Jev API.
    pub fn decide(
        &self,
        action: &str,
        step: &str,
        uncertain: bool,
        last: Option<&Spoken>,
        now: i64,
    ) -> (bool, &'static str) {
        if action == "stop" || action == "arrived" {
            return (true, if action == "stop" { "stop" } else { "arrived" });
        }
        let Some(last) = last else {
            return (true, "first");
        };
        if action != last.action {
            (true, "action_changed")
        } else if step != last.route_step_id {
            (true, "step_changed")
        } else if uncertain && !last.uncertain {
            (true, "uncertain")
        } else if now - last.at >= self.repeat_ms {
            (true, "repeat_interval")
        } else {
            (false, "unchanged")
        }
    }

    /// One short sentence. Falls back to a template so guidance never stops because the writer failed.
    pub async fn write(&self, input: &WriterInput) -> String {
        if !self.jev_base_url.is_empty()
            && let Ok(out) = self.jev(WRITER_PROMPT, json!(input), 2000).await
            && let Some(text) = out["text"].as_str().map(str::trim)
            && !text.is_empty()
            && text.chars().count() <= MAX_TEXT
        {
            return text.to_string();
        }
        template(
            &input.action,
            input.direction.as_deref(),
            input.uncertain,
            &input.destination_label,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> Vec<String> {
        ["start", "corridor", "counter"].map(String::from).to_vec()
    }

    #[test]
    fn validate_only_moves_along_the_route() {
        let p = Pipeline::from_env();
        let ok = json!({"action": "continue", "proposedNextStepId": "corridor", "confidence": 0.9});
        assert_eq!(
            p.validate(&ok, "start", &path()),
            ("continue".into(), None, "corridor".into(), false)
        );
        let skip =
            json!({"action": "continue", "proposedNextStepId": "counter", "confidence": 0.9});
        assert_eq!(p.validate(&skip, "start", &path()).0, "wait");
        let unsure =
            json!({"action": "continue", "proposedNextStepId": "corridor", "confidence": 0.2});
        assert_eq!(
            p.validate(&unsure, "start", &path()),
            ("wait".into(), None, "start".into(), true)
        );
        let early =
            json!({"action": "arrived", "proposedNextStepId": "corridor", "confidence": 0.9});
        assert_eq!(p.validate(&early, "start", &path()).0, "wait");
        let turn = json!({"action": "turn", "direction": "up", "confidence": 0.9});
        assert_eq!(p.validate(&turn, "start", &path()).0, "wait");
    }

    #[test]
    fn decide_speaks_on_change_or_after_interval() {
        let p = Pipeline::from_env();
        let last = Spoken {
            action: "continue".into(),
            route_step_id: "corridor".into(),
            uncertain: false,
            at: 0,
        };
        assert_eq!(
            p.decide("continue", "corridor", false, Some(&last), 1000),
            (false, "unchanged")
        );
        assert!(p.decide("turn", "corridor", false, Some(&last), 1000).0);
        assert!(p.decide("continue", "counter", false, Some(&last), 1000).0);
        assert!(
            p.decide("continue", "corridor", false, Some(&last), p.repeat_ms)
                .0
        );
        assert!(p.decide("arrived", "corridor", false, Some(&last), 1000).0);
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
}
