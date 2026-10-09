//! Orient orchestrator: the only service the phone talks to.
//!
//! Implements docs/contracts.md from tungsten-united/project-description.
//!
//! `client` holds the state and the API operations and knows nothing about HTTP frameworks.
//! `server` (native, axum) and `cloudflare` (a Worker plus one Durable Object per client)
//! only parse requests, call `client`, and write responses.

pub mod client;
pub mod pipeline;

#[cfg(not(target_arch = "wasm32"))]
pub mod server;

#[cfg(target_arch = "wasm32")]
mod cloudflare;
