//! Cloudflare: a stateless Worker routes `/v1/clients/{id}/*` to one Durable Object per client.
//! The Durable Object holds that client in memory, like one entry of the native server's map,
//! and its open SSE request keeps it alive while the phone is connected.

use std::cell::RefCell;
use std::sync::Arc;

use futures::StreamExt;
use serde_json::{Value, json};
use worker::*;

use crate::client::{
    self, ApiError, ApiResult, App, AppState, ClientRef, Limits, Parts, api_error, bad_request,
};
use crate::pipeline::{Pipeline, Route, var};

const BINDING: &str = "CLIENTS";

/// Worker vars and secrets: both are read with `Env::var`.
fn lookup(env: &Env) -> impl Fn(&str) -> Option<String> + '_ {
    move |k| env.var(k).ok().map(|v| v.to_string())
}

fn internal(e: Error) -> ApiError {
    api_error(500, "internal", e.to_string())
}

fn respond(status: u16, body: &Value) -> Result<Response> {
    Ok(Response::from_json(body)?.with_status(status))
}

fn cors(env: &Env, origin: Option<String>, mut resp: Response) -> Result<Response> {
    let allowed = var(&lookup(env), "ALLOW_ORIGINS", "*");
    let origin = if allowed == "*" {
        Some(allowed.clone())
    } else {
        origin.filter(|o| allowed.split(',').any(|a| a.trim() == o))
    };
    if let Some(o) = origin {
        let h = resp.headers_mut();
        h.set("Access-Control-Allow-Origin", &o)?;
        h.set("Access-Control-Allow-Methods", "GET, POST")?;
        h.set(
            "Access-Control-Allow-Headers",
            "authorization, content-type",
        )?;
        h.set("Vary", "Origin")?;
    }
    Ok(resp)
}

fn stub(env: &Env, client_id: &str) -> Result<Stub> {
    env.durable_object(BINDING)?
        .id_from_name(client_id)?
        .get_stub()
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let origin = req.headers().get("Origin")?;
    if req.method() == Method::Options {
        return cors(&env, origin, Response::empty()?.with_status(204));
    }
    let path = req.path();
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let resp = match (req.method(), segments.as_slice()) {
        (Method::Get, ["v1", "health"]) => respond(
            200,
            &json!({"status": "ok", "version": env!("CARGO_PKG_VERSION")}),
        )?,
        (Method::Post, ["v1", "clients"]) => {
            let id = uuid::Uuid::new_v4().to_string();
            let create = Request::new(&format!("https://client/create/{id}"), Method::Post)?;
            return stub(&env, &id)?.fetch_with_request(create).await;
        }
        // Only IDs this Worker could have issued reach a Durable Object.
        (_, ["v1", "clients", id, _]) if uuid::Uuid::parse_str(id).is_ok() => {
            return stub(&env, id)?.fetch_with_request(req).await;
        }
        (_, ["v1", "clients", _, _]) => {
            let e = client::client_not_found();
            respond(e.status, &e.body())?
        }
        _ => respond(
            404,
            &json!({"error": {"code": "not_found", "message": "No such route.", "retryable": false}}),
        )?,
    };
    cors(&env, origin, resp)
}

// ponytail: the client lives in memory only. If Cloudflare evicts the object (no open stream for
// a while, or a deploy), the phone gets 404 and starts a new client. Persist to `state.storage()` if that bites.
#[durable_object(fetch)]
pub struct ClientObject {
    app: App,
    env: Env,
    client: RefCell<Option<ClientRef>>,
}

impl DurableObject for ClientObject {
    fn new(_state: State, env: Env) -> Self {
        // ponytail: built-in route only; ROUTE_PATH has no filesystem to read from here.
        let route: Route =
            serde_json::from_str(include_str!("route.json")).expect("route definition");
        let app = {
            let get = lookup(&env);
            Arc::new(AppState::new(route, Pipeline::from_vars(&get), &get))
        };
        ClientObject {
            app,
            env,
            client: RefCell::new(None),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let origin = req.headers().get("Origin")?;
        let resp = match self.handle(req).await {
            Ok(resp) => resp,
            Err(e) => respond(e.status, &e.body())?,
        };
        cors(&self.env, origin, resp)
    }
}

impl ClientObject {
    async fn handle(&self, mut req: Request) -> ApiResult<Response> {
        let path = req.path();
        let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
        let op = match segments.as_slice() {
            ["create", id] => {
                let (cref, body) = client::create_client(&self.app, id.to_string());
                *self.client.borrow_mut() = Some(cref);
                return respond(201, &body).map_err(internal);
            }
            ["v1", "clients", _, op] => op.to_string(),
            _ => return Err(api_error(404, "not_found", "No such route.")),
        };
        let cref = self
            .client
            .borrow()
            .clone()
            .ok_or_else(client::client_not_found)?;
        let token = if op == "events" {
            let url = req.url().map_err(internal)?;
            url.query_pairs()
                .find(|(k, _)| k == "token")
                .map(|(_, v)| v.into_owned())
        } else {
            req.headers()
                .get("Authorization")
                .map_err(internal)?
                .map(|v| v.trim_start_matches("Bearer ").to_string())
        };
        client::authorize(&cref, &token.unwrap_or_default())?;

        let app = &self.app;
        let (status, body) = match (req.method(), op.as_str()) {
            (Method::Get, "events") => {
                let stream = client::events(app, &cref).map(Ok::<_, Error>);
                let mut resp = Response::from_stream(stream).map_err(internal)?;
                let h = resp.headers_mut();
                h.set("Content-Type", "text/event-stream")
                    .map_err(internal)?;
                h.set("Cache-Control", "no-cache").map_err(internal)?;
                return Ok(resp);
            }
            (Method::Post, "utterances") => {
                let parts = read_parts(&app.limits, &mut req).await?;
                (202, client::utterance(app, &cref, parts)?)
            }
            (Method::Post, "frames") => {
                let parts = read_parts(&app.limits, &mut req).await?;
                (202, client::frames(app, &cref, parts)?)
            }
            (Method::Post, "stop") => {
                let body = req.bytes().await.map_err(bad_request)?;
                (200, client::stop(&cref, &body)?)
            }
            (Method::Post, "retry") => {
                let body = req.bytes().await.map_err(bad_request)?;
                (200, client::retry(&cref, &body)?)
            }
            (Method::Get, "trace") => (200, client::trace(&cref)),
            _ => return Err(api_error(404, "not_found", "No such route.")),
        };
        respond(status, &body).map_err(internal)
    }
}

async fn read_parts(limits: &Limits, req: &mut Request) -> ApiResult<Parts> {
    let form = req.form_data().await.map_err(bad_request)?;
    let mut p = Parts {
        meta: form.get_field("meta"),
        ..Parts::default()
    };
    for name in ["audio", "frame"] {
        match form.get(name) {
            Some(FormEntry::File(f)) => {
                let content_type = f.type_();
                client::check_part(limits, name, &content_type, f.size())?;
                let data = f.bytes().await.map_err(internal)?;
                if name == "audio" {
                    p.audio = Some((data, content_type));
                } else {
                    p.frame = Some(data);
                }
            }
            // A text field has no media type.
            Some(FormEntry::Field(_)) => client::check_part(limits, name, "", 0)?,
            None => {}
        }
    }
    Ok(p)
}
