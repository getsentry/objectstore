//! Contains all HTTP endpoint handlers.
//!
//! Use [`routes`] to create a router with all endpoints.

use axum::Router;

use crate::state::ServiceState;

mod batch;
pub mod common;
pub mod health;
mod keda;
mod objects;

/// Returns `true` for internal endpoints that are exempt from metrics and concurrency limits.
pub fn is_internal_route(route: &str) -> bool {
    matches!(route, "/health" | "/ready" | "/keda")
}

pub fn routes() -> Router<ServiceState> {
    let routes_v1 = Router::new()
        .merge(objects::router())
        .merge(batch::router());

    Router::new()
        .merge(health::router())
        .merge(keda::router())
        .nest("/v1/", routes_v1)
}
