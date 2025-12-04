use axum::response::IntoResponse;
use axum::{Router, routing};

use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new().route("/health", routing::get(health))
}

async fn health() -> impl IntoResponse {
    "OK"
}
