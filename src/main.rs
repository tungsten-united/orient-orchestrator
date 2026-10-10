//! Native entry point. The API lives in the library: see lib.rs.

use std::sync::Arc;

use orient_orchestrator::client::AppState;
use orient_orchestrator::pipeline::{Pipeline, var};
use orient_orchestrator::server;

#[tokio::main]
async fn main() {
    let get = |k: &str| std::env::var(k).ok();
    let addr = var(&get, "BIND", "0.0.0.0:8000");
    let app = Arc::new(AppState::new(Pipeline::from_vars(&get), &get));
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    println!("orient-orchestrator listening on {addr}");
    axum::serve(listener, server::router(app))
        .await
        .expect("serve");
}
