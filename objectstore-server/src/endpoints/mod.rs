//! Contains all HTTP endpoint handlers.
//!
//! Use [`routes`] to create a router with all endpoints.

use axum::Router;

use crate::state::ServiceState;

mod batch;
pub mod common;
pub mod health;
mod keda;
mod multipart;
mod objects;
#[cfg(all(target_os = "linux", feature = "profiling"))]
mod profiling;

/// Returns `true` for internal endpoints that are exempt from metrics and concurrency limits.
pub fn is_internal_route(route: &str) -> bool {
    matches!(route, "/health" | "/ready" | "/keda") || route.starts_with("/debug/")
}

/// Returns a router with all objectstore HTTP endpoints mounted.
///
/// Mounts health and KEDA endpoints at the root and all object/batch
/// endpoints under `/v1/`.
pub fn routes() -> Router<ServiceState> {
    let routes_v1 = Router::new()
        .merge(objects::router())
        .merge(batch::router())
        .merge(multipart::router());

    let router = Router::new()
        .merge(health::router())
        .merge(keda::router())
        .nest("/v1/", routes_v1);

    std::cfg_select! {
        all(target_os = "linux", feature = "profiling") => {
            router.merge(profiling::router())
        }
        _ => { router }
    }
}
