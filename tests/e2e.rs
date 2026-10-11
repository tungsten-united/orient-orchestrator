//! End to end over HTTP and SSE, against the native server (default) or a running deployment.
//!
//!   cargo test --test e2e                                  # in-process server, fakes on random ports
//!   E2E_BASE=https://… cargo test --test e2e   # fakes on :8101 (nav) and :8102 (ElevenLabs);
//!                                             # point NAV_URL and ELEVENLABS_URL there

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Multipart, Path, Query};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::{Stream, StreamExt};
use reqwest::multipart::{Form, Part};
use serde_json::{Value, json};

use orient_orchestrator::client::{AppState, now_ms};
use orient_orchestrator::pipeline::Pipeline;
use orient_orchestrator::server;

#[derive(Default)]
struct FakeNav {
    found: Value,
    route: Value,
    delay_ms: u64,
    frames_seen: usize,
    previous: Option<String>,
    expected: Option<String>,
    heading: Option<String>,
    motions: Vec<String>,
    previous_step_count: Option<String>,
    route_request: Value,
    route_calls: usize,
    auth: Option<String>,
    caller: Value,                  // the X-*-Id headers of the last localize
    map: String,                    // the map of the last localize
    script: VecDeque<Value>,        // localize answers to give in order, before `found`
    routes: HashMap<String, Value>, // "start/trust" -> the route answer, before `route`
    route_starts: Vec<String>,      // the start of every route request, in order
}

/// The walk back on Glories Basement, 2026-10-10: nav-api's live answers, per session (see its `about`).
fn walk_back() -> Value {
    serde_json::from_str(include_str!("fixtures/glories-walk-back.json")).unwrap()
}

/// The maps the fake nav-api publishes, as `GET /maps/{map_id}` serves them (the fields the orchestrator reads).
fn fake_map(map_id: &str) -> Option<Value> {
    let nodes = match map_id {
        "itnig" => json!([{"id": "n2", "name": "drinks area"}, {"id": "n7", "name": "kitchen"},
                          {"id": "n8", "name": "stage"}]),
        "glories-basement" => walk_back()["map"]["nodes"].clone(),
        _ => return None,
    };
    Some(json!({"map_id": map_id, "place": map_id, "nodes": nodes, "edges": []}))
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

/// `with_jev`: a fake TypeSafe that picks the drinks area and keeps every changed direction quiet.
async fn harness_with(with_jev: bool) -> Harness {
    harness_vars(with_jev, &[]).await
}

/// `extra`: more settings for the in-process server.
async fn harness_vars(with_jev: bool, extra: &[(&'static str, &str)]) -> Harness {
    let nav = Arc::new(Mutex::new(FakeNav::default()));
    let fake = nav.clone();
    let fake_nav = Router::new()
        .route(
            "/maps",
            get(|| async {
                // newest first, as nav-api lists them
                Json(
                    json!([{"map_id": "glories-basement", "place": "Glories Basement", "nodes": 2},
                            {"map_id": "itnig", "place": "Itnig", "nodes": 3}]),
                )
            }),
        )
        .route(
            "/maps/{map}",
            get(|Path(map): Path<String>| async move {
                fake_map(&map)
                    .map(Json)
                    .ok_or(axum::http::StatusCode::NOT_FOUND)
            }),
        );
    let fake_nav = fake_nav.route(
        "/maps/{map}/localize",
        post(move |Path(map): Path<String>, headers: HeaderMap, mut mp: Multipart| {
            let fake = fake.clone();
            async move {
                let (mut frames, mut previous, mut expected, mut heading) = (0, None, None, None);
                let (mut motions, mut steps) = (Vec::new(), None);
                while let Some(f) = mp.next_field().await.unwrap() {
                    match f.name() {
                        Some("images") => frames += 1,
                        Some("previous") => previous = Some(f.text().await.unwrap()),
                        Some("expected") => expected = Some(f.text().await.unwrap()),
                        Some("heading_deg") => heading = Some(f.text().await.unwrap()),
                        Some("motion") => motions.push(f.text().await.unwrap()),
                        Some("previous_step_count") => steps = Some(f.text().await.unwrap()),
                        _ => {}
                    }
                }
                let header = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).map(String::from);
                let (answer, delay) = {
                    let mut n = fake.lock().unwrap();
                    n.map = map;
                    n.frames_seen = frames;
                    n.previous = previous;
                    n.expected = expected;
                    n.heading = heading;
                    n.motions = motions;
                    n.previous_step_count = steps;
                    n.caller = json!({"client": header("x-client-id"), "session": header("x-session-id"),
                                      "request": header("x-request-id")});
                    n.auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    let answer = n.script.pop_front().unwrap_or_else(|| n.found.clone());
                    (answer, n.delay_ms)
                };
                tokio::time::sleep(Duration::from_millis(delay)).await;
                Json(answer)
            }
        }),
    );
    let fake = nav.clone();
    let fake_nav = fake_nav.route(
        "/maps/{map}/route",
        post(move |Json(req): Json<Value>| {
            let fake = fake.clone();
            async move {
                let mut n = fake.lock().unwrap();
                let start = req["start"].as_str().unwrap_or("").to_string();
                let key = format!("{start}/{}", req["trust"].as_str().unwrap_or(""));
                n.route_request = req;
                n.route_calls += 1;
                n.route_starts.push(start);
                Json(
                    n.routes
                        .get(&key)
                        .cloned()
                        .unwrap_or_else(|| n.route.clone()),
                )
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
                    } else if name == "keyterms" {
                        let term = f.text().await.unwrap();
                        match request[&name].as_array_mut() {
                            Some(terms) => terms.push(json!(term)),
                            None => request[name] = json!([term]),
                        }
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
            vars.extend(extra.iter().map(|(k, v)| (*k, v.to_string())));
            if with_jev {
                vars.push(("JEV_URL", format!("{}/jev", serve(fake_jev, 0).await)));
                vars.push(("TYPESAFE_API_KEY", "test-jev-key".into()));
            }
            let get = |k: &str| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone());
            let app = Arc::new(AppState::new(Pipeline::from_vars(&get), &get));
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

/// A clear answer: the frames show `node`, far ahead of any other (the belief commits to it in one result).
fn confirmed(node: &str) -> Value {
    json!({"status": "confirmed", "reason": "clear", "best": node, "margin": 1.0, "candidates": [{"node": node, "score": 0.7}]})
}

/// A route: (source, target, first step, instruction) per hop.
fn path(hops: &[(&str, &str, &str, Option<&str>)]) -> Value {
    let hops: Vec<Value> = hops
        .iter()
        .map(|(from, to, step, instruction)| {
            json!({"edge": "e1", "source": from, "target": to, "instruction": instruction,
                   "steps": [{"action": step}], "status": "observed"})
        })
        .collect();
    json!({"found": true, "hops": hops})
}

/// Posts frames from `seq` on until an event other than an unchanged heartbeat arrives (the belief may need a few results).
/// Returns it, the next sequence and how many frames it took.
async fn frames_until_news(
    h: &Harness,
    rx: &mut Events,
    url: &str,
    token: &str,
    generation: i64,
    mut seq: i64,
) -> (Value, i64, usize) {
    for sent in 1..=8 {
        let form = Form::new()
            .text("meta", meta(&format!("f{seq}"), generation, seq, now_ms()))
            .part("frame", jpeg());
        let r = h
            .http
            .post(url)
            .bearer_auth(token)
            .multipart(form)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202);
        seq += 1;
        let ev = rx.next().await;
        if !(ev["type"] == "heartbeat" && ev["quietReason"] == "unchanged") {
            return (ev, seq, sent);
        }
    }
    panic!("nothing new after 8 frames");
}

fn meta(rid: &str, generation: i64, sequence: i64, captured_at: i64) -> String {
    json!({"requestId": rid, "generation": generation, "sequence": sequence, "capturedAt": captured_at,
           "motion": {"headingDeg": 90.5, "stepCount": 3}})
    .to_string()
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
    let frames_seen = || h.nav.lock().unwrap().frames_seen;
    let route_calls = || h.nav.lock().unwrap().route_calls;
    let local = std::env::var("E2E_BASE").is_err();

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

    // Audio with the first frame starts session 1. A confirmed result locates the user at once; one route.
    set_nav(
        "start",
        path(&[
            ("start", "corridor", "straight", None),
            ("corridor", "n2", "turn_left", None),
        ]),
        0,
    );
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("take me to the drinks area"))
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
        json!({"start": "start", "goal": "n2", "trust": "any"})
    );
    assert_eq!(route_calls(), 1);
    {
        let n = h.nav.lock().unwrap();
        assert_eq!((n.previous.as_deref(), n.expected.as_deref()), (None, None));
        assert_eq!(
            n.caller,
            json!({"client": cid, "session": session1, "request": "u1"})
        );
        if local {
            assert_eq!(n.auth.as_deref(), Some("Bearer nav-token"));
        }
    }
    // Scribe got the phone's audio as is, with the contract's settings (contracts.md section 4).
    let mut stt = h.eleven.lock().unwrap().stt_request.clone();
    if local {
        assert_eq!(stt["key"], "test-key");
    }
    stt.as_object_mut().unwrap().remove("key");
    assert_eq!(
        stt,
        json!({"model_id": "scribe_v2", "language_code": "en", "tag_audio_events": "false", "fileType": "audio/webm",
            "keyterms": ["drinks area", "kitchen", "stage"]})
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

    // Still at the hop's source: the worker stays quiet. Each frame goes to localize once, with its motion, the
    // hop and the step count from when the user was located.
    for seq in 2..5 {
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
    {
        let n = h.nav.lock().unwrap();
        assert_eq!(n.frames_seen, 1);
        assert_eq!(
            (n.previous.as_deref(), n.expected.as_deref()),
            (Some("start"), Some("corridor"))
        );
        assert_eq!(n.motions.len(), 1);
        assert_eq!(
            serde_json::from_str::<Value>(&n.motions[0]).unwrap()["stepCount"],
            3
        );
        assert_eq!(n.previous_step_count.as_deref(), Some("3"));
        assert_eq!(n.heading.as_deref(), Some("90.5"));
    }

    // The hop's target, clearly: reached, and the next hop is spoken. No new route.
    let route = h.nav.lock().unwrap().route.clone(); // the guard must drop before set_nav locks again
    set_nav("corridor", route, 0);
    let (ev, seq, took) = frames_until_news(&h, &mut rx, &url("frames"), token, 2, 5).await;
    assert_eq!(
        (
            ev["text"].as_str(),
            ev["routeStepId"].as_str(),
            ev["nextRouteStepId"].as_str()
        ),
        (Some("Turn left."), Some("corridor"), Some("n2"))
    );
    if local {
        assert_eq!(took, 1); // a clear answer: the belief commits at once
    }
    assert_eq!(route_calls(), 1);
    assert_eq!(h.nav.lock().unwrap().expected.as_deref(), Some("corridor"));

    // Same action again: same session, same generation, and the next output is spoken.
    assert_eq!(
        say("u2", 2, seq, "the drinks area please")
            .await
            .unwrap()
            .status(),
        202
    );
    let ev = rx.next().await;
    assert_eq!(
        (ev["sessionId"].as_str(), ev["generation"].as_i64()),
        (Some(session1.as_str()), Some(2))
    );
    assert_eq!(
        post_frame("g10", 2, seq + 1, now_ms())
            .await
            .unwrap()
            .status(),
        202
    );
    assert_eq!(rx.next().await["text"], "Turn left.");
    assert_eq!(h.nav.lock().unwrap().expected.as_deref(), Some("n2"));

    // Different action: new session, new generation, empty buffer and no previous output. It locates the user
    // again, from their last node.
    h.nav.lock().unwrap().route = path(&[("corridor", "n7", "turn_left", None)]);
    assert_eq!(
        say("u3", 2, seq + 2, "take me to the kitchen")
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
        post_frame("g12", 2, seq + 3, now_ms())
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        post_frame("g14", 3, seq + 4, now_ms())
            .await
            .unwrap()
            .status(),
        202
    );
    assert_eq!(rx.next().await["text"], "Turn left.");
    assert_eq!(frames_seen(), 1);
    {
        let n = h.nav.lock().unwrap();
        assert_eq!(
            (n.previous.as_deref(), n.expected.as_deref()),
            (Some("corridor"), None)
        );
        assert_eq!(n.route_request["start"], "corridor");
    }
    assert_eq!(route_calls(), 2);

    // Somewhere else, three times out of four: lost. Locate again, and a new route from there.
    set_nav("n5", path(&[("n5", "n7", "straight", None)]), 0);
    let (ev, seq, _) = frames_until_news(&h, &mut rx, &url("frames"), token, 3, seq + 5).await;
    assert_eq!(
        (ev["text"].as_str(), ev["uncertain"].as_bool()),
        (
            Some("Please hold still, I need a clearer view."),
            Some(true)
        )
    );
    let (ev, seq, _) = frames_until_news(&h, &mut rx, &url("frames"), token, 3, seq).await;
    assert_eq!(
        (ev["text"].as_str(), ev["routeStepId"].as_str()),
        (Some("Keep going straight."), Some("n5"))
    );
    assert_eq!(h.nav.lock().unwrap().route_request["start"], "n5");
    assert_eq!(route_calls(), 3);

    // Stop while the navigation model is still thinking: the late answer is never emitted.
    let route = h.nav.lock().unwrap().route.clone(); // the guard must drop before set_nav locks again
    set_nav("n7", route, 300);
    assert_eq!(
        post_frame("g99", 3, seq, now_ms()).await.unwrap().status(),
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
            Some("n5"),
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
    assert!(t.contains("\"framesSent\":1") && t.contains("likely → reached") && !t.contains(token));
}

/// The server alone, with a fake nav-api that only serves the maps (clients need their map's places).
async fn local_server(vars: &[(&str, &str)]) -> String {
    let maps = Router::new().route(
        "/maps/{map}",
        get(|Path(map): Path<String>| async move {
            fake_map(&map)
                .map(Json)
                .ok_or(axum::http::StatusCode::NOT_FOUND)
        }),
    );
    let nav_url = serve(maps, 0).await;
    let get = |k: &str| {
        vars.iter()
            .find(|(n, _)| *n == k)
            .map(|(_, v)| v.to_string())
            .or_else(|| (k == "NAV_URL").then(|| nav_url.clone()))
    };
    let app = Arc::new(AppState::new(Pipeline::from_vars(&get), &get));
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
        n.route = path(&[
            (
                "start",
                "corridor",
                "straight",
                Some("Walk 10 m along the wall."),
            ),
            (
                "corridor",
                "n2",
                "straight",
                Some("Keep walking along the wall."),
            ),
        ]);
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

    // The next hop only rephrases the last one: Jev keeps it quiet.
    h.nav.lock().unwrap().found = confirmed("corridor");
    let (ev, _, _) = frames_until_news(&h, &mut rx, &url("frames"), token, 2, 1).await;
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

#[tokio::test]
async fn should_follow_the_route_hop_by_hop_to_arrival() {
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
    let url = |p: &str| format!("{}/v1/clients/{cid}/{p}", h.base);
    let mut rx = Events::open(&h.http, format!("{}?token={token}", url("events"))).await;
    rx.next().await;
    {
        let mut n = h.nav.lock().unwrap();
        n.found = confirmed("start");
        n.route = path(&[
            ("start", "corridor", "straight", None),
            ("corridor", "n2", "turn_left", None),
        ]);
    }
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("take me to the drinks area"))
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
    assert_eq!(rx.next().await["text"], "Keep going straight.");

    // The destination seen from afar doesn't skip the corridor: it is somewhere else, so the user is located
    // again, and the destination itself is where they arrive.
    h.nav.lock().unwrap().found = confirmed("corridor");
    let (ev, seq, _) = frames_until_news(&h, &mut rx, &url("frames"), token, 2, 1).await;
    assert_eq!(
        (ev["text"].as_str(), ev["routeStepId"].as_str()),
        (Some("Turn left."), Some("corridor"))
    );
    h.nav.lock().unwrap().found = confirmed("n2");
    let (ev, seq, _) = frames_until_news(&h, &mut rx, &url("frames"), token, 2, seq).await;
    assert_eq!(
        (ev["action"].as_str(), ev["text"].as_str()),
        (
            Some("arrived"),
            Some("You have arrived at the drinks area.")
        )
    );
    assert_eq!(h.nav.lock().unwrap().route_calls, 1);
    let form = Form::new()
        .text("meta", meta("late", 2, seq, now_ms()))
        .part("frame", jpeg());
    let r = h
        .http
        .post(url("frames"))
        .bearer_auth(token)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
}

/// Any published map: the phone lists them, picks one, and its places are that map's nodes. Saying one guides there,
/// with every nav-api call on that map; an unknown or malformed map id is refused before a client exists.
#[tokio::test]
async fn should_guide_on_the_map_the_phone_chose() {
    let h = harness().await;
    let maps: Value = h
        .http
        .get(format!("{}/v1/maps", h.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        maps,
        json!([{"mapId": "itnig", "place": "Itnig"}, {"mapId": "glories-basement", "place": "Glories Basement"}])
    );

    let create = |body: Value| {
        h.http
            .post(format!("{}/v1/clients", h.base))
            .json(&body)
            .send()
    };
    assert_eq!(
        create(json!({"mapId": "nowhere"})).await.unwrap().status(),
        404
    );
    assert_eq!(
        create(json!({"mapId": "../itnig"})).await.unwrap().status(),
        400
    );
    let c: Value = create(json!({"mapId": "glories-basement"}))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let nodes = walk_back()["map"]["nodes"].clone();
    let places: Vec<Value> = nodes
        .as_array()
        .unwrap()
        .iter()
        .map(|n| json!({"destinationId": n["id"], "label": n["name"]}))
        .collect();
    assert_eq!(
        c["route"],
        json!({"routeId": "glories-basement", "destinations": places})
    );
    assert_eq!(places.len(), 16);
    let (cid, token) = (
        c["clientId"].as_str().unwrap(),
        c["clientToken"].as_str().unwrap(),
    );
    let url = |p: &str| format!("{}/v1/clients/{cid}/{p}", h.base);
    let mut rx = Events::open(&h.http, format!("{}?token={token}", url("events"))).await;
    rx.next().await;
    {
        let mut n = h.nav.lock().unwrap();
        n.found = confirmed("n1");
        n.route = path(&[(
            "n1",
            "n4",
            "turn_right",
            Some("Turn right and walk to the second escalators."),
        )]);
    }
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("take me to the second escalators"))
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
    assert_eq!(rx.next().await["destinationId"], "n4");
    assert_eq!(
        rx.next().await["text"],
        "Turn right and walk to the second escalators."
    );
    let n = h.nav.lock().unwrap();
    assert_eq!(n.map, "glories-basement");
    assert_eq!(n.route_request["goal"], "n4");
    let stt = h.eleven.lock().unwrap().stt_request.clone();
    let names: Vec<&Value> = nodes
        .as_array()
        .unwrap()
        .iter()
        .map(|n| &n["name"])
        .collect();
    assert_eq!(stt["keyterms"], json!(names));
}

/// Located, but nav-api has no route from there (a map walked one way only): the phone hears that once, not
/// "Please hold still", and `route` is not asked again from the same node; from another node it is.
#[tokio::test]
async fn should_say_once_that_there_is_no_way_from_here_and_ask_again_only_from_elsewhere() {
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
    let url = |p: &str| format!("{}/v1/clients/{cid}/{p}", h.base);
    let mut rx = Events::open(&h.http, format!("{}?token={token}", url("events"))).await;
    rx.next().await;
    {
        let mut n = h.nav.lock().unwrap();
        n.found = confirmed("n7");
        n.route = json!({"found": false, "hops": []});
    }
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("take me to the drinks area"))
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
    let ev = rx.next().await;
    assert_eq!(
        (ev["type"].as_str(), ev["text"].as_str()),
        (
            Some("guidance"),
            Some("I can't find a way to the drinks area from here.")
        )
    );
    // Still at the kitchen, every result confirming it: quiet, and no new route request.
    let mut seq = 1;
    for _ in 0..3 {
        let form = Form::new()
            .text("meta", meta(&format!("f{seq}"), 2, seq, now_ms()))
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
        seq += 1;
        let ev = rx.next().await;
        assert_eq!(
            (ev["type"].as_str(), ev["quietReason"].as_str()),
            (Some("heartbeat"), Some("unchanged"))
        );
    }
    assert_eq!(h.nav.lock().unwrap().route_calls, 1);

    // Somewhere else, there is a way: asked again, and guidance starts.
    {
        let mut n = h.nav.lock().unwrap();
        n.found = confirmed("corridor");
        n.route = path(&[("corridor", "n2", "turn_left", None)]);
    }
    let (ev, _, _) = frames_until_news(&h, &mut rx, &url("frames"), token, 2, seq).await;
    assert_eq!(ev["text"], "Turn left.");
    let n = h.nav.lock().unwrap();
    assert_eq!(
        (n.route_calls, n.route_request["start"].as_str()),
        (2, Some("corridor"))
    );
}

/// Replays one live session of the walk back (`walk_back()`): the phone on Glories Basement asks for the entrance
/// escalator with its first frame, and every frame then gets the localize answer nav-api gave live, in order. Route
/// requests get nav-api's real answers from that place. Returns every guidance text, in order.
async fn replay_walk_back(h: &Harness, client: &str) -> Vec<String> {
    let fixture = walk_back();
    let session = fixture["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["client"] == client)
        .unwrap();
    let frames = session["frames"].as_array().unwrap();
    {
        let mut n = h.nav.lock().unwrap();
        n.script = frames.iter().map(|f| f["localize"].clone()).collect();
        for (start, by_trust) in fixture["routes"].as_object().unwrap() {
            for (trust, answer) in by_trust.as_object().unwrap() {
                n.routes.insert(format!("{start}/{trust}"), answer.clone());
            }
        }
    }
    let c: Value = h
        .http
        .post(format!("{}/v1/clients", h.base))
        .json(&json!({"mapId": "glories-basement"}))
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
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("take me to the entrance escalator"))
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
    assert_eq!(rx.next().await["destinationId"], "n1");
    let mut spoken = Vec::new();
    for seq in 1..=frames.len() as i64 {
        let ev = rx.next().await; // the previous frame's evaluation: guidance or a quiet heartbeat
        if ev["type"] == "guidance" {
            spoken.push(ev["text"].as_str().unwrap().to_string());
        }
        if seq == frames.len() as i64 {
            break;
        }
        let form = Form::new()
            .text("meta", meta(&format!("f{seq}"), 2, seq, now_ms()))
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
    }
    assert!(
        h.nav.lock().unwrap().script.is_empty(),
        "every live answer was used"
    );
    spoken
}

/// Live (Glories Basement, 2026-10-10, session 00f49c7b): placed at Playground, Body Shop junction and Rituals shop on
/// the way back, the orchestrator asked for a route 20 times (none over `observed`: the map was walked the other way)
/// and said "Please hold still, I need a clearer view." throughout. Now "hold still" is only for before the first
/// placement: it asks once from each place and says why it can't guide. On these answers (top two candidates only,
/// the walk back barely telling nodes apart) the belief places the user once, at Rituals shop, near the end.
#[tokio::test]
async fn should_ask_once_per_place_and_say_there_is_no_way_on_the_live_walk_back() {
    let h = harness_vars(false, &[("NAV_TRUST", "observed")]).await;
    let spoken = replay_walk_back(&h, "00f49c7b").await;
    assert_eq!(h.nav.lock().unwrap().route_starts, ["n5"]);
    let no_way = "I can't find a way to the Entrance escalator from here.";
    assert_eq!(
        spoken,
        ["Please hold still, I need a clearer view.", no_way]
    );
}

/// The same walk back, routing over walked connections either way (the default): placed at the bar kiosk, the phone
/// hears the way back as the map has it, the first hop's instruction for that direction, right after "hold still"
/// while it was still locating.
#[tokio::test]
async fn should_guide_back_over_the_walked_connections_on_the_live_walk_back() {
    let h = harness().await;
    let spoken = replay_walk_back(&h, "74515567").await;
    let n = h.nav.lock().unwrap();
    assert_eq!(n.route_starts.first().map(String::as_str), Some("n13"));
    assert_eq!(n.route_request["trust"], "any");
    let first_hop = &walk_back()["routes"]["n13"]["any"]["hops"][0];
    assert_eq!(spoken[0], "Please hold still, I need a clearer view.");
    assert_eq!(Some(spoken[1].as_str()), first_hop["instruction"].as_str());
}

/// The camera misses a node on the way (Itnig, 2026-10-10: Right-side tables, walked past to Corridor end): the next
/// one leading, with nav-api agreeing the user could have walked there, is where they are. The route goes on from
/// there, with no "Please hold still" and no new route; hop by hop only, this dropped the route and located again.
#[tokio::test]
async fn should_move_on_past_a_node_the_camera_missed() {
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
    let url = |p: &str| format!("{}/v1/clients/{cid}/{p}", h.base);
    let mut rx = Events::open(&h.http, format!("{}?token={token}", url("events"))).await;
    rx.next().await;
    {
        let mut n = h.nav.lock().unwrap();
        n.found = confirmed("start");
        n.route = path(&[
            ("start", "corridor", "straight", None),
            ("corridor", "n2", "turn_left", None),
        ]);
    }
    let form = Form::new()
        .text("meta", meta("u1", 1, 0, now_ms()))
        .part("audio", audio("take me to the drinks area"))
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
    assert_eq!(rx.next().await["text"], "Keep going straight.");

    // The corridor is never recognised; the drinks area leads, not confirmed, and could have been walked to.
    h.nav.lock().unwrap().found = json!({"status": "uncertain", "reason": "best score 0.41 < 0.45", "best": "n2",
        "margin": 0.2, "candidates": [{"node": "n2", "score": 0.41, "plausible": true}]});
    let (ev, _, sent) = frames_until_news(&h, &mut rx, &url("frames"), token, 2, 1).await;
    assert_eq!(
        (ev["action"].as_str(), ev["text"].as_str()),
        (
            Some("arrived"),
            Some("You have arrived at the drinks area.")
        )
    );
    assert!(sent > 1, "{sent}"); // not on one result that isn't clear
    assert_eq!(h.nav.lock().unwrap().route_calls, 1);
}
