//! End to end over HTTP and SSE, against the native server (default) or a running deployment.
//!
//!   cargo test --test e2e                                  # in-process server, fakes on random ports
//!   E2E_BASE=https://… cargo test --test e2e   # fakes on :8101 (nav) and :8102 (ElevenLabs);
//!                                             # point NAV_URL and ELEVENLABS_URL there

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Multipart, Path, Query};
use axum::http::HeaderMap;
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
    found: Value,
    route: Value,
    delay_ms: u64,
    frames_seen: usize,
    previous: Option<String>,
    route_request: Value,
    auth: Option<String>,
}

/// What the fake ElevenLabs received.
#[derive(Default)]
struct FakeEleven {
    stt_request: Value,
    tts_request: Value,
    tts_calls: usize,
}

struct Harness {
    base: String,
    http: reqwest::Client,
    nav: Arc<Mutex<FakeNav>>,
    eleven: Arc<Mutex<FakeEleven>>,
    jev_states: Arc<Mutex<Vec<Value>>>,
}

async fn serve(router: Router, port: u16) -> String {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

/// `with_jev`: a fake TypeSafe that picks the coffee and keeps every changed direction quiet.
async fn harness_with(with_jev: bool) -> Harness {
    let nav = Arc::new(Mutex::new(FakeNav::default()));
    let fake = nav.clone();
    let fake_nav = Router::new().route(
        "/maps/itnig/localize",
        post(move |headers: HeaderMap, mut mp: Multipart| {
            let fake = fake.clone();
            async move {
                let (mut frames, mut previous) = (0, None);
                while let Some(f) = mp.next_field().await.unwrap() {
                    match f.name() {
                        Some("images") => frames += 1,
                        Some("previous") => previous = Some(f.text().await.unwrap()),
                        _ => {}
                    }
                }
                let (answer, delay) = {
                    let mut n = fake.lock().unwrap();
                    n.frames_seen = frames;
                    n.previous = previous;
                    n.auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    (n.found.clone(), n.delay_ms)
                };
                tokio::time::sleep(Duration::from_millis(delay)).await;
                Json(answer)
            }
        }),
    );
    let fake = nav.clone();
    let fake_nav = fake_nav.route(
        "/maps/itnig/route",
        post(move |Json(req): Json<Value>| {
            let fake = fake.clone();
            async move {
                let mut n = fake.lock().unwrap();
                n.route_request = req;
                Json(n.route.clone())
            }
        }),
    );
    let jev_states = Arc::new(Mutex::new(Vec::new()));
    let states = jev_states.clone();
    let fake_jev = Router::new().route(
        "/jev",
        post(move |Json(body): Json<Value>| {
            let states = states.clone();
            async move {
                let answer =
                    |choice: &str| json!({"type": "choice", "choice": choice, "confidence": 0.9});
                if body["questions"]["speak"].is_object() {
                    states.lock().unwrap().push(body["state"].clone());
                    return Json(json!({"answers": {"speak": answer("quiet")}}));
                }
                Json(json!({"answers": {"command": answer("n2")}}))
            }
        }),
    );
    // Fake ElevenLabs Scribe: the "audio" bytes are the transcript.
    let eleven = Arc::new(Mutex::new(FakeEleven::default()));
    let fake = eleven.clone();
    let fake_eleven = Router::new().route(
        "/v1/speech-to-text",
        post(move |headers: HeaderMap, mut mp: Multipart| {
            let fake = fake.clone();
            async move {
                let mut request = json!({"key": headers.get("xi-api-key").and_then(|v| v.to_str().ok())});
                let mut text = String::new();
                while let Some(f) = mp.next_field().await.unwrap() {
                    let name = f.name().unwrap_or("").to_string();
                    if name == "file" {
                        request["fileType"] = json!(f.content_type());
                        text = String::from_utf8_lossy(&f.bytes().await.unwrap()).into();
                    } else {
                        request[name] = json!(f.text().await.unwrap());
                    }
                }
                fake.lock().unwrap().stt_request = request;
                Json(json!({"language_code": "en", "language_probability": 0.98, "text": text, "words": []}))
            }
        }),
    );
    // Fake ElevenLabs Flash: the "MP3" is the text, and the text "fail" fails.
    let fake = eleven.clone();
    let fake_eleven = fake_eleven.route(
        "/v1/text-to-speech/{voice}/stream",
        post(
            move |Path(voice): Path<String>,
                  Query(q): Query<std::collections::HashMap<String, String>>,
                  Json(body): Json<Value>| {
                let fake = fake.clone();
                async move {
                    let mut f = fake.lock().unwrap();
                    f.tts_calls += 1;
                    f.tts_request = json!({"voice": voice, "outputFormat": q.get("output_format"), "body": body});
                    if body["text"] == "fail" {
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                    Ok((
                        [(axum::http::header::CONTENT_TYPE, "audio/mpeg")],
                        format!("mp3:{}", body["text"].as_str().unwrap()),
                    ))
                }
            },
        ),
    );
    let base = match std::env::var("E2E_BASE") {
        Ok(base) => {
            serve(fake_nav, 8101).await;
            serve(fake_eleven, 8102).await;
            base
        }
        Err(_) => {
            let mut vars = vec![
                ("NAV_URL", serve(fake_nav, 0).await),
                ("ELEVENLABS_URL", serve(fake_eleven, 0).await),
                ("ELEVENLABS_API_KEY", "test-key".into()),
                ("ELEVENLABS_VOICE_ID", "test-voice".into()),
                ("NAV_API_TOKEN", "nav-token".into()),
            ];
            if with_jev {
                vars.push(("JEV_URL", format!("{}/jev", serve(fake_jev, 0).await)));
                vars.push(("TYPESAFE_API_KEY", "test-jev-key".into()));
            }
            let get = |k: &str| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone());
            let route: Route = serde_json::from_str(include_str!("../src/route.json")).unwrap();
            let app = Arc::new(AppState::new(route, Pipeline::from_vars(&get), &get));
            serve(server::router(app), 0).await
        }
    };
    Harness {
        base,
        http: reqwest::Client::new(),
        nav,
        eleven,
        jev_states,
    }
}

async fn harness() -> Harness {
    harness_with(false).await
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

fn confirmed(node: &str) -> Value {
    json!({"status": "confirmed", "reason": "clear", "best": node, "margin": 0.1, "candidates": [{"node": node, "score": 0.7}]})
}

fn hop(from: &str, to: &str, step: &str, instruction: Option<&str>) -> Value {
    json!({"found": true, "hops": [{"edge": "e1", "source": from, "target": to, "instruction": instruction,
                                     "steps": [{"action": step}], "status": "observed"}]})
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
            .post(url("inputs"))
            .bearer_auth(token)
            .multipart(form)
            .send()
    };
    let set_nav = |node: &str, route: Value, delay_ms: u64| {
        let mut n = h.nav.lock().unwrap();
        n.found = confirmed(node);
        n.route = route;
        n.delay_ms = delay_ms;
    };
    let continue_to = |from: &str, to: &str| hop(from, to, "straight", None);
    let turn_left = |from: &str, to: &str| hop(from, to, "turn_left", None);
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
        .post(url("inputs"))
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // Audio with the first frame starts session 1.
    set_nav("start", continue_to("start", "corridor"), 0);
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("take me to the coffee"))
        .part("frame", jpeg());
    let r = h
        .http
        .post(url("inputs"))
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
    assert_eq!(
        h.nav.lock().unwrap().route_request,
        json!({"start": "start", "goal": "n2", "trust": "observed"})
    );
    if std::env::var("E2E_BASE").is_err() {
        assert_eq!(
            h.nav.lock().unwrap().auth.as_deref(),
            Some("Bearer nav-token")
        );
    }
    // Scribe got the phone's audio as is, with the contract's settings (contracts.md section 4).
    let mut stt = h.eleven.lock().unwrap().stt_request.clone();
    if std::env::var("E2E_BASE").is_err() {
        assert_eq!(stt["key"], "test-key");
    }
    stt.as_object_mut().unwrap().remove("key");
    assert_eq!(
        stt,
        json!({"model_id": "scribe_v2", "language_code": "en", "tag_audio_events": "false", "fileType": "audio/webm"})
    );

    // Replays, old sequences and old captures are rejected without side effects.
    assert_eq!(say("u1", 2, 0, "kitchen").await.unwrap().status(), 202);
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

    // Same output as before: the worker stays quiet. The buffer caps at 4 frames.
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
    assert_eq!(frames_seen(), 4);

    // A different output is spoken.
    set_nav("corridor", turn_left("corridor", "n2"), 0);
    assert_eq!(
        post_frame("f8", 2, 8, now_ms()).await.unwrap().status(),
        202
    );
    let ev = rx.next().await;
    assert_eq!(
        (
            ev["text"].as_str(),
            ev["routeStepId"].as_str(),
            ev["nextRouteStepId"].as_str()
        ),
        (Some("Turn left."), Some("corridor"), Some("n2"))
    );
    assert_eq!(h.nav.lock().unwrap().previous.as_deref(), Some("start"));

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
        say("u3", 2, 10, "take me to the kitchen")
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
        (Some(3), Some("n7"))
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
    assert_eq!(h.nav.lock().unwrap().route_request["start"], "corridor");

    // Stop while the navigation model is still thinking: the late answer is never emitted.
    set_nav("n7", continue_to("corridor", "n7"), 300);
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
    assert!(t.contains("\"framesSent\":4") && !t.contains(token));
}

async fn local_server(vars: &[(&str, &str)]) -> String {
    let get = |k: &str| {
        vars.iter()
            .find(|(n, _)| *n == k)
            .map(|(_, v)| v.to_string())
    };
    let route: Route = serde_json::from_str(include_str!("../src/route.json")).unwrap();
    let app = Arc::new(AppState::new(route, Pipeline::from_vars(&get), &get));
    serve(server::router(app), 0).await
}

#[tokio::test]
async fn should_serve_debug_page_pointing_at_nav_url_only_when_enabled() {
    let http = reqwest::Client::new();
    let off = local_server(&[]).await;
    assert_eq!(
        http.get(format!("{off}/debug"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );

    let on = local_server(&[("DEBUG_PAGE", "1"), ("NAV_URL", "https://fakes.example")]).await;
    let page = http
        .get(format!("{on}/debug"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(page.contains(r#"id="nav-url" value="https://fakes.example""#));
}

#[tokio::test]
async fn should_report_deployed_commit_in_health() {
    let base = local_server(&[("GIT_SHA", "abc123")]).await;
    let h: Value = reqwest::get(format!("{base}/v1/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(h["commit"], "abc123");
}

#[tokio::test]
async fn should_stream_speech_from_elevenlabs_and_cache_it_by_text() {
    let h = harness().await;
    let c: Value = h
        .http
        .post(format!("{}/v1/clients", h.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let (cid, token) = (
        c["clientId"].as_str().unwrap(),
        c["clientToken"].as_str().unwrap(),
    );
    let speech = |token: &str, text: &str| {
        h.http
            .get(format!("{}/v1/clients/{cid}/speech", h.base))
            .query(&[("token", token), ("text", text)])
            .send()
    };
    let calls = || h.eleven.lock().unwrap().tts_calls;

    assert_eq!(speech("wrong", "Turn left.").await.unwrap().status(), 401);
    assert_eq!(speech(token, "").await.unwrap().status(), 400);
    assert_eq!(speech(token, &"a".repeat(241)).await.unwrap().status(), 400);
    assert_eq!(calls(), 0);

    // First time: streamed from Flash with the contract's request (contracts.md section 4).
    let r = speech(token, "Turn left.").await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "audio/mpeg");
    assert_eq!(r.headers()["x-speech-text"], "Turn left.");
    assert_eq!(r.text().await.unwrap(), "mp3:Turn left.");
    let mut tts = h.eleven.lock().unwrap().tts_request.clone();
    if std::env::var("E2E_BASE").is_ok() {
        tts.as_object_mut().unwrap().remove("voice"); // picked by the deployment
    } else {
        assert_eq!(tts["voice"], "test-voice");
        tts.as_object_mut().unwrap().remove("voice");
    }
    assert_eq!(
        tts,
        json!({"outputFormat": "mp3_44100_64", "body": {"text": "Turn left.", "model_id": "eleven_flash_v2_5", "language_code": "en"}})
    );
    assert_eq!(calls(), 1);

    // Same text again: from the cache, no ElevenLabs call.
    let r = speech(token, "Turn left.").await.unwrap();
    assert_eq!(r.headers()["x-speech-text"], "Turn left.");
    assert_eq!(r.text().await.unwrap(), "mp3:Turn left.");
    assert_eq!(calls(), 1);

    // A failure is a 503 the phone answers with browser TTS, and it is not cached.
    let r = speech(token, "fail").await.unwrap();
    assert_eq!(r.status(), 503);
    let e: Value = r.json().await.unwrap();
    assert_eq!(e["error"]["code"], "upstream_unavailable");
    assert_eq!(speech(token, "fail").await.unwrap().status(), 503);
    assert_eq!(calls(), 3);
}

#[tokio::test]
async fn should_answer_503_for_speech_when_no_voice_is_configured() {
    let base = local_server(&[("ELEVENLABS_API_KEY", "k")]).await;
    let http = reqwest::Client::new();
    let c: Value = http
        .post(format!("{base}/v1/clients"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let r = http
        .get(format!(
            "{base}/v1/clients/{}/speech",
            c["clientId"].as_str().unwrap()
        ))
        .query(&[
            ("token", c["clientToken"].as_str().unwrap()),
            ("text", "Turn left."),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
}

#[tokio::test]
async fn should_keep_a_changed_direction_quiet_when_jev_says_it_is_not_worth_saying() {
    if std::env::var("E2E_BASE").is_ok() {
        return;
    }
    let h = harness_with(true).await;
    let c: Value = h
        .http
        .post(format!("{}/v1/clients", h.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let (cid, token) = (
        c["clientId"].as_str().unwrap(),
        c["clientToken"].as_str().unwrap(),
    );
    let url = |p: &str| format!("{}/v1/clients/{cid}/{p}", h.base);
    let mut rx = Events::open(&h.http, format!("{}?token={token}", url("events"))).await;
    rx.next().await;
    {
        let mut n = h.nav.lock().unwrap();
        n.found = confirmed("start");
        n.route = hop(
            "start",
            "corridor",
            "straight",
            Some("Walk 10 m along the wall."),
        );
    }
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("coffee"))
        .part("frame", jpeg());
    let r = h
        .http
        .post(url("inputs"))
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    assert_eq!(rx.next().await["phase"], "navigating");
    assert_eq!(rx.next().await["text"], "Walk 10 m along the wall.");
    assert!(h.jev_states.lock().unwrap().is_empty());

    h.nav.lock().unwrap().route["hops"][0]["instruction"] = json!("Keep walking along the wall.");
    let form = Form::new()
        .text("meta", meta("f1", 2, 1, now_ms()))
        .part("frame", jpeg());
    let r = h
        .http
        .post(url("frames"))
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    let ev = rx.next().await;
    assert_eq!(
        (ev["type"].as_str(), ev["quietReason"].as_str()),
        (Some("heartbeat"), Some("not_worth_saying"))
    );
    let state = h.jev_states.lock().unwrap()[0].clone();
    assert_eq!(
        state["previous"]["instruction"],
        "Walk 10 m along the wall."
    );
    assert_eq!(state["new"]["instruction"], "Keep walking along the wall.");
}
