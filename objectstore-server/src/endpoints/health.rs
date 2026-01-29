use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Router, routing};

use crate::state::ServiceState;

pub const SHUTDOWN_MARKER_PATH: &str = "/tmp/objectstore.down";

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
        tracing::debug!("Shutdown marker exists, failing readiness");
        (StatusCode::SERVICE_UNAVAILABLE, "Shutting down")
    } else {
        (StatusCode::OK, "OK")
    }
}
