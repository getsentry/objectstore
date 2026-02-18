use axum::extract::State;
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

async fn ready(State(state): State<ServiceState>) -> impl IntoResponse {
    let is_shutting_down = tokio::fs::try_exists(SHUTDOWN_MARKER_PATH)
        .await
        .unwrap_or(false);
    if is_shutting_down {
        tracing::debug!("Shutdown marker exists, failing readiness");
        return (StatusCode::SERVICE_UNAVAILABLE, "Shutting down");
    }

    let too_many_requests = match state.config.backpressure_thresholds.in_flight_requests {
        Some(max_concurrent_requests) => {
            state.system_metrics.in_flight_requests.get() > max_concurrent_requests
        }
        None => false,
    };
    if too_many_requests {
        tracing::warn!("Serving too many concurrent requests already, failing readiness");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Host overloaded with requests",
        );
    }

    (StatusCode::OK, "OK")
}
