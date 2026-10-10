//! Native HTTP server (axum): parses requests, calls `client`, writes responses.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde_json::{Value, json};
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::client::{
    self, ApiError, ApiResult, App, ClientRef, Limits, Parts, api_error, bad_request,
};
use crate::pipeline::var;

struct Server {
    app: App,
    // ponytail: in-memory clients in one process, never expired. Fine for one demo phone; add a store if we scale out.
    clients: Mutex<HashMap<String, ClientRef>>,
    // Start of the current minute and the log batches received in it. Debug logs are unauthenticated,
    // because the failures worth seeing happen before a client exists, so they get a global cap.
    log_window: Mutex<(i64, u32)>,
}

const LOG_BATCHES_PER_MINUTE: u32 = 120;

type Srv = Arc<Server>;

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(self.body())).into_response()
    }
}

fn get_client(srv: &Srv, id: &str, token: &str) -> ApiResult<ClientRef> {
    let cref = srv.clients.lock().unwrap().get(id).cloned();
    let cref = cref.ok_or_else(client::client_not_found)?;
    client::authorize(&cref, token)?;
    Ok(cref)
}

fn bearer(headers: &HeaderMap) -> &str {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map_or("", |v| v.trim_start_matches("Bearer "))
}

async fn read_parts(limits: &Limits, mut mp: Multipart) -> ApiResult<Parts> {
    let mp_error = |e: axum::extract::multipart::MultipartError| match e.status() {
        StatusCode::PAYLOAD_TOO_LARGE => api_error(413, "payload_too_large", e.body_text()),
        _ => bad_request(e.body_text()),
    };
    let mut p = Parts::default();
    while let Some(field) = mp.next_field().await.map_err(mp_error)? {
        let name = field.name().unwrap_or("").to_string();
        let content_type = field.content_type().unwrap_or("").to_string();
        match name.as_str() {
            "meta" => p.meta = Some(field.text().await.map_err(mp_error)?),
            "audio" | "frame" => {
                client::check_part(limits, &name, &content_type, 0)?;
                let data = field.bytes().await.map_err(mp_error)?.to_vec();
                client::check_part(limits, &name, &content_type, data.len())?;
                if name == "audio" {
                    p.audio = Some((data, content_type));
                } else {
                    p.frame = Some(data);
                }
            }
            _ => {}
        }
    }
    Ok(p)
}

async fn health(State(srv): State<Srv>) -> Json<Value> {
    Json(json!({"status": "ok", "version": env!("CARGO_PKG_VERSION"), "commit": srv.app.commit}))
}

/// The debug page, with its fake navigation URL defaulting to this server's NAV_URL.
fn debug_page(nav_url: &str) -> Html<String> {
    let default = r#"id="nav-url" value="http://localhost:8001""#;
    Html(include_str!("debug.html").replace(default, &format!(r#"id="nav-url" value="{nav_url}""#)))
}

async fn create_client(State(srv): State<Srv>) -> (StatusCode, Json<Value>) {
    let id = uuid::Uuid::new_v4().to_string();
    let (cref, response) = client::create_client(&srv.app, id.clone());
    srv.clients.lock().unwrap().insert(id, cref);
    (StatusCode::CREATED, Json(response))
}

async fn events(
    State(srv): State<Srv>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    let cref = get_client(&srv, &id, q.get("token").map_or("", String::as_str))?;
    let stream = client::events(&srv.app, &cref).map(Ok::<_, Infallible>);
    Ok((
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        Body::from_stream(stream),
    )
        .into_response())
}

const SPEECH_TEXT: HeaderName = HeaderName::from_static("x-speech-text");

/// Percent-encodes UTF-8 text for a header value, readable with `decodeURIComponent`.
fn percent_encode(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b' ' | b'!'..=b'~' if b != b'%' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Token in the query, like the event stream, so the phone can use it as an `<audio>` src.
async fn speech(
    State(srv): State<Srv>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    get_client(&srv, &id, q.get("token").map_or("", String::as_str))?;
    let text = q.get("text").cloned().unwrap_or_default();
    let spoken = percent_encode(&text);
    let audio = client::speech(&srv.app, text).await?;
    Ok((
        [
            (header::CONTENT_TYPE, "audio/mpeg".to_string()),
            (SPEECH_TEXT, spoken),
        ],
        Body::from_stream(audio),
    )
        .into_response())
}

async fn input(
    State(srv): State<Srv>,
    Path(id): Path<String>,
    headers: HeaderMap,
    mp: Multipart,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let cref = get_client(&srv, &id, bearer(&headers))?;
    let parts = read_parts(&srv.app.limits, mp).await?;
    let r = client::input(&srv.app, &cref, parts)?;
    Ok((StatusCode::ACCEPTED, Json(r)))
}

async fn frames(
    State(srv): State<Srv>,
    Path(id): Path<String>,
    headers: HeaderMap,
    mp: Multipart,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let cref = get_client(&srv, &id, bearer(&headers))?;
    let parts = read_parts(&srv.app.limits, mp).await?;
    let r = client::frames(&srv.app, &cref, parts)?;
    Ok((StatusCode::ACCEPTED, Json(r)))
}

async fn stop(
    State(srv): State<Srv>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let cref = get_client(&srv, &id, bearer(&headers))?;
    Ok(Json(client::stop(&cref, &body)?))
}

async fn retry(
    State(srv): State<Srv>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Value>> {
    let cref = get_client(&srv, &id, bearer(&headers))?;
    Ok(Json(client::retry(&cref, &body)?))
}

/// Phone debug logs. No token: a denied permission happens before `POST /v1/clients`.
async fn logs(State(srv): State<Srv>, body: Bytes) -> ApiResult<StatusCode> {
    {
        let mut window = srv.log_window.lock().unwrap();
        let now = client::now_ms();
        if now - window.0 >= 60_000 {
            *window = (now, 0);
        }
        window.1 += 1;
        if window.1 > LOG_BATCHES_PER_MINUTE {
            return Err(api_error(429, "rate_limited", "Too many log batches."));
        }
    }
    for line in client::client_logs(&srv.app, &body)? {
        println!("{line}"); // one JSON line: Cloud Logging stores it as a structured entry
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn trace(
    State(srv): State<Srv>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let cref = get_client(&srv, &id, bearer(&headers))?;
    Ok(Json(client::trace(&cref)))
}

pub fn router(app: App) -> Router {
    let get_env = |k: &str| std::env::var(k).ok();
    let origins = var(&get_env, "ALLOW_ORIGINS", "*");
    let allow_origin = if origins == "*" {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(
            origins
                .split(',')
                .map(|o| HeaderValue::from_str(o.trim()).expect("ALLOW_ORIGINS")),
        )
    };
    let cors = CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
        .expose_headers([SPEECH_TEXT]);
    let mut routes = Router::new();
    if app.debug_page {
        // Local runs and staging only: drives the API and shows events and the trace live. See examples/fakes.rs.
        let page = debug_page(&app.pipeline.nav_url);
        routes = routes.route("/debug", get(|| async { page }));
    }
    let srv = Arc::new(Server {
        app,
        clients: Mutex::default(),
        log_window: Mutex::default(),
    });
    routes
        .route("/v1/health", get(health))
        .route("/v1/logs", post(logs))
        .route("/v1/clients", post(create_client))
        .route("/v1/clients/{id}/events", get(events))
        .route("/v1/clients/{id}/speech", get(speech))
        .route("/v1/clients/{id}/inputs", post(input))
        .route("/v1/clients/{id}/frames", post(frames))
        .route("/v1/clients/{id}/stop", post(stop))
        .route("/v1/clients/{id}/retry", post(retry))
        .route("/v1/clients/{id}/trace", get(trace))
        .layer(cors)
        .with_state(srv)
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_percent_encode_non_ascii_and_percent_only() {
        assert_eq!(super::percent_encode("Turn left."), "Turn left.");
        assert_eq!(super::percent_encode("Café 100%"), "Caf%C3%A9 100%25");
    }
}
