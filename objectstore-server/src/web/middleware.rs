use std::any::Any;
use std::net::SocketAddr;

use axum::RequestExt;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::time::Instant;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::endpoints::is_internal_route;
use crate::state::ServiceState;

/// The value for the `Server` HTTP header.
const SERVER: &str = concat!("objectstore/", env!("CARGO_PKG_VERSION"));

/// Rejects requests with HTTP 503 when the in-flight request count reaches the configured
/// maximum.
///
/// Use with [`from_fn_with_state`](axum::middleware::from_fn_with_state), passing
/// [`ServiceState`]. The in-flight counter is shared with the
/// [`InFlightRequestsLayer`](tower_http::metrics::InFlightRequestsLayer) so both read the
/// same atomic. Internal routes (see [`is_internal_route`]) are excluded.
pub async fn limit_web_concurrency(
    State(state): State<ServiceState>,
    mut request: Request,
    next: Next,
) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());

    if !is_internal_route(route) && state.request_counter.get() >= state.config.http.max_requests {
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
/// Internal routes (see [`is_internal_route`]) are excluded from metrics.
pub async fn emit_request_metrics(mut request: Request, next: Next) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());

    let should_emit = !is_internal_route(route);
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
    use objectstore_service::{StorageConfig, StorageService};
    use tempfile::TempDir;
    use tokio::sync::Notify;
    use tower::ServiceExt;
    use tower_http::metrics::InFlightRequestsLayer;
    use tower_http::metrics::in_flight_requests::InFlightRequestsCounter;

    use crate::auth::PublicKeyDirectory;
    use crate::config::Config;
    use crate::rate_limits::RateLimiter;
    use crate::state::Services;

    use super::*;

    fn make_request(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    /// Builds a test router with the concurrency limit middleware applied.
    ///
    /// The `/v1/test/_/key` handler notifies `paused` on entry and waits on
    /// `resume` before returning. `/health` and `/ready` return 200 immediately.
    /// Returns `(router, paused, resume, _tempdir)` — hold `_tempdir` to keep
    /// the filesystem backend alive.
    async fn make_app(max: usize) -> (Router, Arc<Notify>, Arc<Notify>, TempDir) {
        let tempdir = TempDir::new().unwrap();
        let fs_config = StorageConfig::FileSystem {
            path: tempdir.path(),
        };
        let service = StorageService::new(fs_config.clone(), fs_config)
            .await
            .unwrap();

        let mut config = Config::default();
        config.http.max_requests = max;

        let request_counter = InFlightRequestsCounter::new();
        let in_flight_layer = InFlightRequestsLayer::new(request_counter.clone());

        let key_directory = PublicKeyDirectory::try_from(&config.auth).unwrap();
        let rate_limiter = RateLimiter::new(config.rate_limits.clone());

        let state = Arc::new(Services {
            config,
            service,
            key_directory,
            rate_limiter,
            request_counter,
        });

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
            .route("/keda", get(|| async { StatusCode::OK.into_response() }))
            .layer(in_flight_layer)
            .layer(from_fn_with_state(state, limit_web_concurrency));

        (app, paused, resume, tempdir)
    }

    #[tokio::test]
    async fn request_passes_below_limit() {
        let (app, _paused, resume, _tempdir) = make_app(5).await;
        resume.notify_one(); // pre-signal so the handler does not block
        let resp = app.oneshot(make_request("/v1/test/_/key")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn at_limit_rejects_health_exempt() {
        let (app, paused, resume, _tempdir) = make_app(1).await;

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

        let keda = app.clone().oneshot(make_request("/keda")).await.unwrap();
        assert_eq!(keda.status(), StatusCode::OK);

        resume.notify_one();
        blocking.await.unwrap().unwrap();
    }
}
