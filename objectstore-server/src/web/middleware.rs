use std::any::Any;
use std::net::SocketAddr;

use axum::RequestExt;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::time::Instant;
use tower_http::metrics::in_flight_requests::InFlightRequestsCounter;
use tower_http::set_header::SetResponseHeaderLayer;

/// The value for the `Server` HTTP header.
const SERVER: &str = concat!("objectstore/", env!("CARGO_PKG_VERSION"));

/// Rejects requests with HTTP 503 when the in-flight request count reaches `max`.
///
/// Use with [`from_fn_with_state`](axum::middleware::from_fn_with_state), passing
/// `(InFlightRequestsCounter, usize)` as state — share the counter from
/// [`InFlightRequestsLayer::pair`](tower_http::metrics::InFlightRequestsLayer) so both
/// read the same atomic. Health (`/health`) and readiness (`/ready`) are excluded.
pub async fn limit_web_concurrency(
    State((counter, max)): State<(InFlightRequestsCounter, usize)>,
    mut request: Request,
    next: Next,
) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());

    if !matches!(route, "/health" | "/ready") && counter.get() >= max {
        merni::counter!("web.concurrency.rejected": 1);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    next.run(request).await
}

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
///
/// Health check endpoints (`/health` and `/ready`) are excluded from metrics as they are
/// operational checks for orchestration.
pub async fn emit_request_metrics(mut request: Request, next: Next) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());

    let should_emit = !matches!(route, "/health" | "/ready");
    let guard = should_emit.then(|| EmitMetricsGuard::new(route, request.method()));

    let response = next.run(request).await;

    if let Some(guard) = guard {
        guard.finish(response.status());
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::middleware::from_fn_with_state;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use tokio::sync::Notify;
    use tower::ServiceExt;
    use tower_http::metrics::InFlightRequestsLayer;

    use super::*;

    fn make_request(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    /// Builds a test router with the concurrency limit middleware applied.
    ///
    /// The `/v1/test/_/key` handler notifies `paused` on entry and waits on
    /// `resume` before returning. `/health` and `/ready` return 200 immediately.
    /// Returns `(router, paused, resume)`.
    fn make_app(max: usize) -> (Router, Arc<Notify>, Arc<Notify>) {
        let (in_flight_layer, counter) = InFlightRequestsLayer::pair();
        let paused = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());

        let app = Router::new()
            .route(
                "/v1/test/_/key",
                get({
                    let paused = paused.clone();
                    let resume = resume.clone();
                    move || async move {
                        paused.notify_one();
                        resume.notified().await;
                        StatusCode::OK.into_response()
                    }
                }),
            )
            .route("/health", get(|| async { StatusCode::OK.into_response() }))
            .route("/ready", get(|| async { StatusCode::OK.into_response() }))
            .layer(in_flight_layer)
            .layer(from_fn_with_state(
                (counter.clone(), max),
                limit_web_concurrency,
            ));

        (app, paused, resume)
    }

    #[tokio::test]
    async fn request_passes_below_limit() {
        let (app, _paused, resume) = make_app(5);
        resume.notify_one(); // pre-signal so the handler does not block
        let resp = app.oneshot(make_request("/v1/test/_/key")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn at_limit_rejects_health_exempt() {
        let (app, paused, resume) = make_app(1);

        // Hold the counter at 1 with a blocking request.
        let blocking = tokio::spawn(app.clone().oneshot(make_request("/v1/test/_/key")));
        tokio::time::timeout(std::time::Duration::from_secs(5), paused.notified())
            .await
            .expect("handler did not start within 5s");

        // Regular request is rejected; health and ready bypass the limit.
        let resp = app
            .clone()
            .oneshot(make_request("/v1/test/_/key"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let health = app.clone().oneshot(make_request("/health")).await.unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let ready = app.clone().oneshot(make_request("/ready")).await.unwrap();
        assert_eq!(ready.status(), StatusCode::OK);

        resume.notify_one();
        blocking.await.unwrap().unwrap();
    }
}
