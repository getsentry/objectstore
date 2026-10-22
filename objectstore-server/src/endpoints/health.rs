//! Health and readiness endpoints.
//!
//! Exposes `/health` (liveness) and `/ready` (readiness). Both currently return
//! 200 `OK`, including during shutdown. Pod termination triggers endpoint removal;
//! the preStop hook keeps the server available while routing updates propagate.

use axum::response::IntoResponse;
use axum::{Router, routing};

use crate::state::ServiceState;

/// Returns a router with the `/health` and `/ready` endpoints.
pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/health", routing::get(health))
        .route("/ready", routing::get(ready))
}

async fn health() -> impl IntoResponse {
    "OK"
}

async fn ready() -> impl IntoResponse {
    "OK"
}
