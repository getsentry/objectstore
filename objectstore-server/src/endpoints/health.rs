//! Health and readiness endpoints.
//!
//! Exposes `/health` (liveness) and `/ready` (readiness). The readiness
//! endpoint fails when a [`SHUTDOWN_MARKER_PATH`] file is present, allowing
//! graceful draining before process termination.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Router, routing};

use crate::state::ServiceState;

/// Filesystem path whose presence signals that the server is shutting down.
///
/// When this file exists, the `/ready` endpoint returns `503 Service Unavailable`,
/// causing load balancers to stop routing new requests to this instance. Create
/// this file before terminating the process to drain in-flight requests gracefully.
pub const SHUTDOWN_MARKER_PATH: &str = "/tmp/objectstore.down";

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
    if tokio::fs::try_exists(SHUTDOWN_MARKER_PATH)
        .await
        .unwrap_or(false)
    {
        objectstore_log::debug!("Shutdown marker exists, failing readiness");
        (StatusCode::SERVICE_UNAVAILABLE, "Shutting down")
    } else {
        (StatusCode::OK, "OK")
    }
}
