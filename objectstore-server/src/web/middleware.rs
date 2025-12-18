use std::any::Any;
use std::net::SocketAddr;

use axum::RequestExt;
use axum::extract::{ConnectInfo, MatchedPath, Request};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::time::Instant;
use tower_http::set_header::SetResponseHeaderLayer;

/// The value for the `Server` HTTP header.
const SERVER: &str = concat!("objectstore/", env!("CARGO_PKG_VERSION"));

/// Create a `SetResponseHeaderLayer` that sets the `Server` header.
pub fn set_server_header() -> SetResponseHeaderLayer<HeaderValue> {
    SetResponseHeaderLayer::overriding(header::SERVER, HeaderValue::from_static(SERVER))
}

/// Create a tracing span for an HTTP request.
///
/// As opposed to `DefaultMakeSpan`, this also records the client IP address if available.
pub fn make_http_span(request: &Request) -> tracing::Span {
    let span = tracing::debug_span!(
        "request",
        method = %request.method(),
        uri = %request.uri(),
        version = ?request.version(),
        client_addr = tracing::field::Empty,
    );

    if let Some(ConnectInfo(addr)) = request
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
    {
        span.record("client_addr", tracing::field::display(addr.ip()));
    }

    span
}

/// A panic handler that logs the panic and turns it into a 500 response.
///
/// Use with the [`CatchPanicLayer`](tower_http::catch_panic::CatchPanicLayer) middleware.
pub fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response {
    let detail = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "no error details".to_owned()
    };

    tracing::error!("panic in web handler: {detail}");

    let response = (StatusCode::INTERNAL_SERVER_ERROR, detail);
    response.into_response()
}

/// A middleware that logs web request timings as metrics.
///
/// Use this with [`from_fn`](axum::middleware::from_fn).
pub async fn emit_request_metrics(mut request: Request, next: Next) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());
    let guard = EmitMetricsGuard::new(route, request.method());

    let response = next.run(request).await;

    guard.finish(response.status());
    response
}

/// Helper for [`emit_request_metrics`].
///
/// This tracks relevant generic request parameters and emits metrics. If the guard is dropped
/// without calling [`Self::finish`], it will emit a `499`` status code (derived from nginx'
/// non-standard "client closed request").
struct EmitMetricsGuard<'a> {
    route: &'a str,
    method: Method,
    start: Instant,
    status: Option<StatusCode>,
}

impl<'a> EmitMetricsGuard<'a> {
    fn new(route: &'a str, method: &Method) -> Self {
        merni::counter!(
            "server.requests": 1,
            "route" => route,
            "method" => method.as_str()
        );

        Self {
            route,
            method: method.clone(),
            start: Instant::now(),
            status: None,
        }
    }

    fn finish(mut self, status: StatusCode) {
        self.status = Some(status);
    }
}

impl Drop for EmitMetricsGuard<'_> {
    fn drop(&mut self) {
        merni::distribution!(
            "server.requests.duration"@s: self.start.elapsed(),
            "route" => self.route,
            "method" => self.method,
            "status" => self.status
                .map(|s| s.as_u16())
                .unwrap_or(499)
        );
    }
}
