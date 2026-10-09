//! Orient orchestrator: the only service the phone talks to.
//!
//! Implements docs/contracts.md from tungsten-united/project-description.
//!
//! `client` holds the state and the API operations and knows nothing about HTTP frameworks.
//! `server` (axum) only parses requests, call `client`, and write responses.

pub mod client;
pub mod pipeline;

pub mod server;
