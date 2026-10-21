//! Liveness, readiness, and traffic-serving health checks.
//!
//! - `/health/live` reports process liveness.
//! - `/health/ready` reports application readiness and remains successful during draining.
//! - `/health/serving` reports whether upstreams should send new requests. It fails
//!   during shutdown or whenever the application is not ready.
//!
//! Liveness and readiness currently always return 200 `OK`.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Router, routing};

use crate::state::ServiceState;

/// Filesystem path whose presence signals that the server is shutting down.
///
/// When this file exists, `/health/serving` returns `503 Service Unavailable`.
/// Load balancers must probe that endpoint to stop routing new requests to this
/// instance. Create this file before terminating the process to allow traffic to drain.
pub const SHUTDOWN_MARKER_PATH: &str = "/tmp/objectstore.down";

/// Returns a router with the health check endpoints.
pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/health/live", routing::get(live))
        .route("/health/ready", routing::get(ready))
        .route("/health/serving", routing::get(serving))
        // TODO: Remove these legacy routes once all consumers use /health/<kind>.
        .route("/health", routing::get(live))
        .route("/ready", routing::get(ready))
}

async fn live() -> impl IntoResponse {
    "OK"
}

async fn ready() -> impl IntoResponse {
    "OK"
}

async fn serving() -> impl IntoResponse {
    if tokio::fs::try_exists(SHUTDOWN_MARKER_PATH)
        .await
        .unwrap_or(false)
    {
        objectstore_log::debug!("Shutdown marker exists, failing serving health check");
        (StatusCode::SERVICE_UNAVAILABLE, "Shutting down").into_response()
    } else {
        ready().await.into_response()
    }
}
