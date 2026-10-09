//! End to end over HTTP and SSE, against the native server (default) or a running deployment.
//!
//!   cargo test --test e2e                                  # in-process server, fakes on random ports
//!   E2E_BASE=http://localhost:8787 cargo test --test e2e   # e.g. `wrangler dev`; fakes on :8101 (nav) and :8102 (stt)

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::Multipart;
use axum::routing::post;
use axum::{Json, Router};
use futures::{Stream, StreamExt};
use reqwest::multipart::{Form, Part};
use serde_json::{Value, json};

use orient_orchestrator::client::{AppState, now_ms};
use orient_orchestrator::pipeline::{Pipeline, Route};
use orient_orchestrator::server;

#[derive(Default)]
struct FakeNav {
    answer: Value,
    delay_ms: u64,
    frames_seen: usize,
}

struct Harness {
    base: String,
    http: reqwest::Client,
    nav: Arc<Mutex<FakeNav>>,
}

async fn serve(router: Router, port: u16) -> String {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

async fn harness() -> Harness {
    let nav = Arc::new(Mutex::new(FakeNav::default()));
    let fake = nav.clone();
    let fake_vla = Router::new().route(
        "/v1/navigate",
        post(move |mut mp: Multipart| {
            let fake = fake.clone();
            async move {
                let mut frames = 0;
                while let Some(f) = mp.next_field().await.unwrap() {
                    frames += usize::from(f.name() == Some("frames"));
                }
                let (answer, delay) = {
                    let mut n = fake.lock().unwrap();
                    n.frames_seen = frames;
                    (n.answer.clone(), n.delay_ms)
                };
                tokio::time::sleep(Duration::from_millis(delay)).await;
                Json(answer)
            }
        }),
    );
    // Fake STT: the "audio" bytes are the transcript.
    let fake_stt = Router::new().route(
        "/stt",
        post(|mut mp: Multipart| async move {
            let audio = mp
                .next_field()
                .await
                .unwrap()
                .unwrap()
                .bytes()
                .await
                .unwrap();
            Json(json!({"transcript": String::from_utf8_lossy(&audio)}))
        }),
    );
    let base = match std::env::var("E2E_BASE") {
        Ok(base) => {
            serve(fake_vla, 8101).await;
            serve(fake_stt, 8102).await;
            base
        }
        Err(_) => {
            let mut pipeline = Pipeline::from_vars(&|_| None);
            pipeline.nav_url = serve(fake_vla, 0).await;
            pipeline.stt_url = format!("{}/stt", serve(fake_stt, 0).await);
            let route: Route = serde_json::from_str(include_str!("../src/route.json")).unwrap();
            let app = Arc::new(AppState::new(route, pipeline, &|_| None));
            serve(server::router(app), 0).await
        }
    };
    Harness {
        base,
        http: reqwest::Client::new(),
        nav,
    }
}

/// Reads the SSE stream. Timer heartbeats (no quietReason) are skipped: the test does not control them.
struct Events {
    stream: Pin<Box<dyn Stream<Item = reqwest::Result<axum::body::Bytes>> + Send>>,
    buf: String,
}

impl Events {
    async fn open(http: &reqwest::Client, url: String) -> Self {
        let r = http.get(url).send().await.unwrap();
        assert_eq!(r.status(), 200);
        Events {
            stream: Box::pin(r.bytes_stream()),
            buf: String::new(),
        }
    }

    async fn recv(&mut self, wait: Duration) -> Option<Value> {
        loop {
            if let Some(i) = self.buf.find("\n\n") {
                let block: String = self.buf.drain(..i + 2).collect();
                let Some(data) = block.lines().find_map(|l| l.strip_prefix("data:")) else {
                    continue;
                };
                let ev: Value = serde_json::from_str(data.trim()).unwrap();
                if ev["type"] == "heartbeat" && ev["quietReason"].is_null() {
                    continue;
                }
                return Some(ev);
            }
            match tokio::time::timeout(wait, self.stream.next()).await {
                Ok(Some(Ok(chunk))) => self.buf.push_str(&String::from_utf8_lossy(&chunk)),
                _ => return None,
            }
        }
    }

    async fn next(&mut self) -> Value {
        self.recv(Duration::from_secs(5)).await.expect("event")
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

fn audio(text: &str) -> Part {
    Part::bytes(text.as_bytes().to_vec())
        .file_name("a.webm")
        .mime_str("audio/webm")
        .unwrap()
}

#[tokio::test]
async fn sessions_frames_worker_and_stop() {
    let h = harness().await;
    let r = h
        .http
        .post(format!("{}/v1/clients", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 201);
    let c: Value = r.json().await.unwrap();
    let (cid, token) = (
        c["clientId"].as_str().unwrap(),
        c["clientToken"].as_str().unwrap(),
    );
    let url = |p: &str| format!("{}/v1/clients/{cid}/{p}", h.base);
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
    let say = |rid: &str, generation: i64, seq: i64, text: &str| {
        let form = Form::new()
            .text("meta", meta(rid, generation, seq, now_ms()))
            .part("audio", audio(text));
        h.http
            .post(url("utterances"))
            .bearer_auth(token)
            .multipart(form)
            .send()
    };
    let set_nav = |answer: Value, delay_ms: u64| {
        let mut n = h.nav.lock().unwrap();
        n.answer = answer;
        n.delay_ms = delay_ms;
    };
    let frames_seen = || h.nav.lock().unwrap().frames_seen;

    // Unknown client: 404. No token: 401, also on the event stream.
    let unknown = format!("{}/v1/clients/nope/trace", h.base);
    assert_eq!(
        h.http
            .get(unknown)
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(h.http.get(url("trace")).send().await.unwrap().status(), 401);
    assert_eq!(
        h.http.get(url("events")).send().await.unwrap().status(),
        401
    );

    // The stream opens with a state snapshot.
    let mut rx = Events::open(&h.http, format!("{}?token={token}", url("events"))).await;
    let ev = rx.next().await;
    assert_eq!(
        (ev["type"].as_str(), ev["phase"].as_str()),
        (Some("state"), Some("awaiting_destination"))
    );

    // Transcript instead of audio: 400.
    let form = Form::new()
        .text("meta", meta("t0", 1, 0, now_ms()))
        .text("transcript", "coffee");
    let r = h
        .http
        .post(url("utterances"))
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // Audio with the first frame starts session 1.
    set_nav(
        json!({"action": "continue", "proposedNextStepId": "corridor", "confidence": 0.9}),
        0,
    );
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("take me to the coffee"))
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
    let ev = rx.next().await;
    assert_eq!(
        (ev["type"].as_str(), ev["phase"].as_str()),
        (Some("state"), Some("navigating"))
    );
    let session1 = ev["sessionId"].as_str().unwrap().to_string();
    let ev = rx.next().await;
    assert_eq!(
        (ev["type"].as_str(), ev["text"].as_str()),
        (Some("guidance"), Some("Keep going straight."))
    );
    assert_eq!(frames_seen(), 1);

    // Replays, old sequences and old captures are rejected without side effects.
    assert_eq!(say("u1", 2, 0, "bathroom").await.unwrap().status(), 202);
    assert_eq!(
        post_frame("f0", 2, 0, now_ms()).await.unwrap().status(),
        409
    );
    assert_eq!(
        post_frame("f1", 2, 1, now_ms() - 60_000)
            .await
            .unwrap()
            .status(),
        422
    );

    // Same output as before: the worker stays quiet. The buffer caps at 5 frames.
    for seq in 2..8 {
        assert_eq!(
            post_frame(&format!("f{seq}"), 2, seq, now_ms())
                .await
                .unwrap()
                .status(),
            202
        );
        let ev = rx.next().await;
        assert_eq!(
            (ev["type"].as_str(), ev["quietReason"].as_str()),
            (Some("heartbeat"), Some("unchanged"))
        );
    }
    assert_eq!(frames_seen(), 5);

    // A different output is spoken.
    set_nav(
        json!({"action": "turn", "direction": "left", "proposedNextStepId": "corridor", "confidence": 0.9}),
        0,
    );
    assert_eq!(
        post_frame("f8", 2, 8, now_ms()).await.unwrap().status(),
        202
    );
    assert_eq!(rx.next().await["text"], "Turn left.");

    // Same action again: same session, same generation.
    assert_eq!(
        say("u2", 2, 9, "the coffee please").await.unwrap().status(),
        202
    );
    let ev = rx.next().await;
    assert_eq!(
        (ev["sessionId"].as_str(), ev["generation"].as_i64()),
        (Some(session1.as_str()), Some(2))
    );

    // Different action: new session, new generation, empty buffer and no previous output.
    assert_eq!(
        say("u3", 2, 10, "where is the toilet")
            .await
            .unwrap()
            .status(),
        202
    );
    let ev = rx.next().await;
    let session2 = ev["sessionId"].as_str().unwrap().to_string();
    assert_ne!(session2, session1);
    assert_eq!(
        (ev["generation"].as_i64(), ev["destinationId"].as_str()),
        (Some(3), Some("bathroom"))
    );
    assert_eq!(
        post_frame("f11", 2, 11, now_ms()).await.unwrap().status(),
        409
    );
    assert_eq!(
        post_frame("f12", 3, 12, now_ms()).await.unwrap().status(),
        202
    );
    assert_eq!(rx.next().await["text"], "Turn left.");
    assert_eq!(frames_seen(), 1);

    // Stop while the navigation model is still thinking: the late answer is never emitted.
    set_nav(
        json!({"action": "turn", "direction": "right", "proposedNextStepId": "bathroom", "confidence": 0.9}),
        300,
    );
    assert_eq!(
        post_frame("f13", 3, 13, now_ms()).await.unwrap().status(),
        202
    );
    let r: Value = h
        .http
        .post(url("stop"))
        .bearer_auth(token)
        .json(&json!({"requestId": "s1", "generation": 3}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (r["generation"].as_i64(), r["phase"].as_str()),
        (Some(4), Some("stopped"))
    );
    assert_eq!(rx.next().await["type"], "stop");
    assert!(
        rx.recv(Duration::from_millis(800)).await.is_none(),
        "no event after stop"
    );

    // Retry from the last confirmed step keeps the session and its step.
    let r: Value = h
        .http
        .post(url("retry"))
        .bearer_auth(token)
        .json(&json!({"requestId": "r1", "generation": 4, "from": "last_confirmed_step"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (
            r["phase"].as_str(),
            r["sessionId"].as_str(),
            r["routeStepId"].as_str(),
            r["generation"].as_i64()
        ),
        (
            Some("navigating"),
            Some(session2.as_str()),
            Some("corridor"),
            Some(5)
        )
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
    assert!(t.contains("\"framesSent\":5") && !t.contains(token));
}
