//! Fake upstreams for local runs and staging: the navigation engine and ElevenLabs speech-to-text
//! on one port (`BIND`, default 127.0.0.1:8001), because Cloud Run gives each service a single port.
//!
//! The fake Scribe treats the audio bytes as the transcript, so the debug page can "speak" by sending text.
//! The navigation answer is a preset chosen through `POST /control`, which the debug page drives.
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

async fn navigate(State(ctl): State<Shared>, mut mp: Multipart) -> Result<Json<Value>, StatusCode> {
    let (mut meta, mut frames) = (json!({}), 0);
    while let Ok(Some(field)) = mp.next_field().await {
        match field.name() {
            Some("meta") => {
                meta = serde_json::from_str(&field.text().await.unwrap_or_default())
                    .unwrap_or_default()
            }
            Some("frames") => frames += 1,
            _ => {}
        }
    }
    let Control { preset, delay_ms } = ctl.lock().unwrap().clone();
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    let step = meta["routeStepId"].as_str().unwrap_or("").to_string();
    let next = meta["allowedNextStepIds"][0]
        .as_str()
        .unwrap_or(&step)
        .to_string();
    let answer = |action: &str, direction: Option<&str>, proposed: &str, confidence: f64| {
        json!({
            "action": action, "direction": direction, "proposedNextStepId": proposed,
            "confidence": confidence, "observation": format!("fake {preset}, {frames} frames"),
        })
    };
    Ok(Json(match preset.as_str() {
        "advance" => answer("continue", None, &next, 0.9),
        "left" => answer("turn", Some("left"), &step, 0.9),
        "right" => answer("turn", Some("right"), &step, 0.9),
        "arrived" => answer("arrived", None, &next, 0.9),
        "unsure" => answer("continue", None, &step, 0.2),
        "off_route" => answer("continue", None, "nowhere", 0.9),
        "fail" => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        _ => answer("continue", None, &step, 0.9),
    }))
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
    }));
    let fakes = Router::new()
        .route("/v1/navigate", post(navigate))
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
