//! On-demand heap profiling endpoints (Linux + `profiling` feature only).
//!
//! Exposes three routes under `/debug/pprof/`:
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | `POST` | `/debug/pprof/enable` | Activate jemalloc heap sampling |
//! | `POST` | `/debug/pprof/disable` | Deactivate heap sampling |
//! | `GET` | `/debug/pprof/heap` | Dump a symbolized gzipped pprof profile |
//!
//! All routes are guarded by a loopback-only middleware: requests whose TCP peer is not
//! a loopback address (`127.0.0.1` or `::1`) are rejected with `403 Forbidden`. This
//! makes the endpoints reachable via `kubectl port-forward` or an ephemeral debug
//! container (both present as loopback inside the pod netns) while blocking normal
//! Service/ingress traffic.
//!
//! Profiling is compiled in but dormant at startup (`prof_active:false`). Enable it on
//! demand via `POST /debug/pprof/enable`, capture with `GET /debug/pprof/heap`, then
//! disable with `POST /debug/pprof/disable`.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Request};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{Router, routing};
use jemalloc_pprof::PROF_CTL;

use crate::state::ServiceState;

/// Returns a router for all `/debug/pprof/*` endpoints, protected by the loopback gate.
pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/debug/pprof/enable", routing::post(enable))
        .route("/debug/pprof/disable", routing::post(disable))
        .route("/debug/pprof/heap", routing::get(heap))
        .route_layer(axum::middleware::from_fn(require_loopback))
}

/// Middleware that rejects any request whose TCP peer is not a loopback address.
///
/// Fails closed: if `ConnectInfo` is absent from the request extensions, the request is denied.
async fn require_loopback(request: Request, next: Next) -> Response {
    let is_loopback = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .is_some_and(|ConnectInfo(addr)| addr.ip().is_loopback());

    if is_loopback {
        next.run(request).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

/// Activates jemalloc heap sampling.
///
/// Sampling runs until `POST /debug/pprof/disable` is called. The sampling interval is set
/// at startup via `malloc_conf` (`lg_prof_sample:19`, i.e. 512 KiB mean interval).
async fn enable() -> Response {
    let Some(ctl) = &*PROF_CTL else {
        return (StatusCode::SERVICE_UNAVAILABLE, "profiling unavailable").into_response();
    };
    let mut guard = ctl.lock().await;
    match guard.activate() {
        Ok(()) => StatusCode::OK.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

/// Deactivates jemalloc heap sampling.
async fn disable() -> Response {
    let Some(ctl) = &*PROF_CTL else {
        return (StatusCode::SERVICE_UNAVAILABLE, "profiling unavailable").into_response();
    };
    let mut guard = ctl.lock().await;
    match guard.deactivate() {
        Ok(()) => StatusCode::OK.into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

/// Dumps a symbolized, gzipped pprof heap profile.
///
/// Returns `403` if profiling is not currently active. Activate first with
/// `POST /debug/pprof/enable`.
///
/// The response body is a gzipped pprof protobuf (`heap.pb.gz`), ready for
/// `go tool pprof` or Speedscope without shipping a separate binary.
async fn heap() -> Response {
    let Some(ctl) = &*PROF_CTL else {
        return (StatusCode::SERVICE_UNAVAILABLE, "profiling unavailable").into_response();
    };
    let mut guard = ctl.lock().await;

    if !guard.activated() {
        return (
            StatusCode::FORBIDDEN,
            "profiling not active; POST /debug/pprof/enable first",
        )
            .into_response();
    }

    match guard.dump_pprof() {
        Ok(bytes) => {
            let headers = [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                ),
                (
                    header::CONTENT_DISPOSITION,
                    HeaderValue::from_static("attachment; filename=\"heap.pb.gz\""),
                ),
            ];
            (headers, bytes).into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::Router;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use axum::middleware::from_fn;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::require_loopback;

    fn make_loopback_app() -> Router {
        Router::new()
            .route("/test", get(|| async { StatusCode::OK }))
            .route_layer(from_fn(require_loopback))
    }

    fn request_with_peer(peer: &str) -> Request<Body> {
        let addr: SocketAddr = peer.parse().unwrap();
        Request::builder()
            .uri("/test")
            .extension(ConnectInfo(addr))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn loopback_ipv4_allowed() {
        let resp = make_loopback_app()
            .oneshot(request_with_peer("127.0.0.1:1234"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn loopback_ipv6_allowed() {
        let resp = make_loopback_app()
            .oneshot(request_with_peer("[::1]:1234"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn non_loopback_rejected() {
        let resp = make_loopback_app()
            .oneshot(request_with_peer("203.0.113.1:1234"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn no_connect_info_rejected() {
        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();
        let resp = make_loopback_app().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }
}
