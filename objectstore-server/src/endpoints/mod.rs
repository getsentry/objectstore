//! Contains all HTTP endpoint handlers.
//!
//! Use [`routes`] to create a router with all endpoints.

use axum::routing::MethodRouter;
use axum::{Extension, Router};
use objectstore_service::operation::OperationKind;

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

/// Request extension carrying the [`OperationKind`] of the matched route.
///
/// Attached per method+route via [`OpRoute::op`] at route-definition time, and
/// read by the request extractors to tag rate-limit rejections by operation.
#[derive(Clone, Copy, Debug)]
pub struct RouteOperation(pub OperationKind);

/// Annotates a [`MethodRouter`] with the [`OperationKind`] it performs.
///
/// The annotation is a layer that inserts a [`RouteOperation`] extension before
/// the route's extractors run. Because the layer is applied to a single-method
/// [`MethodRouter`] (then `merge`d with the other methods of the same path), each
/// method on a shared path can carry its own kind.
pub trait OpRoute {
    /// Attaches `kind` to this route as a [`RouteOperation`] extension.
    fn op(self, kind: OperationKind) -> Self;
}

impl<S> OpRoute for MethodRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn op(self, kind: OperationKind) -> Self {
        self.layer(Extension(RouteOperation(kind)))
    }
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
