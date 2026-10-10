//! Fake upstreams for local runs and staging: nav-api (`localize`, `route`) and ElevenLabs speech-to-text
//! on one port (`BIND`, default 127.0.0.1:8001), because Cloud Run gives each service a single port.
//!
//! The fake Scribe treats the audio bytes as the transcript, so the debug page can "speak" by sending text.
//! Text to speech is not faked: `GET /speech` answers 503 and the phone or debug page uses browser TTS.
//! The navigation answers follow a preset chosen through `POST /control`, which the debug page drives.
//!
//!   cargo run --example fakes
//!   ELEVENLABS_URL=http://localhost:8001 DEBUG_PAGE=1 TRACE_PATH=trace.jsonl cargo run
//!   open http://localhost:8000/debug

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower_http::cors::CorsLayer;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Control {
    /// continue | advance | left | right | arrived | unsure | off_route | fail
    preset: String,
    delay_ms: u64,
    #[serde(skip)]
    goal: String,
}

type Shared = Arc<Mutex<Control>>;

/// ElevenLabs Scribe's answer shape; only `text` is used.
async fn stt(mut mp: Multipart) -> Json<Value> {
    let mut text = String::new();
    while let Ok(Some(field)) = mp.next_field().await {
        if field.name() == Some("file") {
            text = String::from_utf8_lossy(&field.bytes().await.unwrap_or_default()).into();
        }
    }
    Json(json!({"language_code": "en", "language_probability": 1.0, "text": text, "words": []}))
}

/// The fake walk: start, corridor, then the destination.
fn after(node: &str, goal: &str) -> String {
    match node {
        "start" => "corridor".into(),
        _ => goal.into(),
    }
}

/// nav-api's `POST /maps/{map}/localize`.
async fn localize(State(ctl): State<Shared>, mut mp: Multipart) -> Result<Json<Value>, StatusCode> {
    let (mut previous, mut frames) = (None, 0);
    while let Ok(Some(field)) = mp.next_field().await {
        match field.name() {
            Some("previous") => previous = field.text().await.ok(),
            Some("images") => frames += 1,
            _ => {}
        }
    }
    let Control {
        preset,
        delay_ms,
        goal,
    } = ctl.lock().unwrap().clone();
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    let here = previous.clone().unwrap_or_else(|| "start".into());
    let (status, best) = match preset.as_str() {
        "fail" => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        "unsure" => ("uncertain", here),
        "advance" if previous.is_none() => ("confirmed", here),
        "advance" => ("confirmed", after(&here, &goal)),
        "arrived" if !goal.is_empty() => ("confirmed", goal),
        _ => ("confirmed", here),
    };
    Ok(Json(json!({
        "status": status, "reason": format!("fake {preset}, {frames} frames"), "best": best, "margin": 0.1,
        "candidates": [{"node": best, "name": best, "score": 0.7, "refs": []}], "previous": previous,
    })))
}

/// nav-api's `POST /maps/{map}/route`: one hop toward the goal.
async fn route(State(ctl): State<Shared>, Json(req): Json<Value>) -> Json<Value> {
    let (start, goal) = (
        req["start"].as_str().unwrap_or(""),
        req["goal"].as_str().unwrap_or(""),
    );
    let preset = {
        let mut c = ctl.lock().unwrap();
        c.goal = goal.into();
        c.preset.clone()
    };
    let step = match preset.as_str() {
        "left" => "turn_left",
        "right" => "turn_right",
        _ => "straight",
    };
    let hop = json!({"edge": "e1", "source": start, "target": after(start, goal), "forward": true, "length_m": 5.0,
                     "bearing_deg": null, "instruction": null, "steps": [{"action": step}], "status": "observed"});
    Json(match preset.as_str() {
        "off_route" => json!({"start": start, "goal": goal, "found": false, "hops": []}),
        _ => json!({"start": start, "goal": goal, "found": true, "hops": [hop]}),
    })
}

async fn get_control(State(ctl): State<Shared>) -> Json<Control> {
    Json(ctl.lock().unwrap().clone())
}

async fn set_control(State(ctl): State<Shared>, Json(c): Json<Control>) -> Json<Control> {
    *ctl.lock().unwrap() = c.clone();
    Json(c)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let ctl = Arc::new(Mutex::new(Control {
        preset: "continue".into(),
        delay_ms: 300,
        goal: String::new(),
    }));
    let fakes = Router::new()
        .route("/maps/{map}/localize", post(localize))
        .route("/maps/{map}/route", post(route))
        .route("/control", get(get_control).post(set_control))
        .route("/v1/speech-to-text", post(stt))
        .layer(CorsLayer::permissive())
        .with_state(ctl);
    let addr = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8001".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("fake navigation engine on http://{addr} (POST /control to change the answer)");
    println!("fake ElevenLabs speech-to-text on http://{addr} (audio bytes = transcript)");
    axum::serve(listener, fakes).await
}
