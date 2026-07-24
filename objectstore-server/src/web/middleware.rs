use std::any::Any;
use std::net::SocketAddr;

use axum::RequestExt;
use axum::body::Body;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use objectstore_log::tracing;
use tower_http::set_header::SetResponseHeaderLayer;

use crate::endpoints::is_internal_route;
use crate::extractors::downstream_service::DownstreamService;
use crate::web::RequestCounter;
use crate::web::metrics_body::{EmitMetricsGuard, MetricsBody};
use crate::web::sentry_body::SentryBody;

/// The value for the `Server` HTTP header.
const SERVER: &str = concat!("objectstore/", env!("CARGO_PKG_VERSION"));

/// Rejects requests with HTTP 503 when the in-flight request count reaches the configured
/// maximum.
///
/// Use with [`from_fn_with_state`](axum::middleware::from_fn_with_state), passing a
/// [`RequestCounter`]. Internal routes (see [`is_internal_route`]) are excluded.
pub async fn limit_web_concurrency(
    State(counter): State<RequestCounter>,
    mut request: Request,
    next: Next,
) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());

    if !is_internal_route(route) && counter.count() >= counter.limit() {
        let service = request.extract_parts::<DownstreamService>().await.unwrap();
        objectstore_metrics::count!("web.concurrency.rejected", service = service.to_string());
        objectstore_log::warn!("Request rejected: web concurrency limit reached");
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

    if let Some(ConnectInfo(addr)) = request.extensions().get::<ConnectInfo<SocketAddr>>() {
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

    objectstore_log::error!("panic in web handler: {detail}");

    let response = (StatusCode::INTERNAL_SERVER_ERROR, detail);
    response.into_response()
}

/// Wraps the response body so the request's Sentry hub stays active during polling.
///
/// Use this with [`from_fn`](axum::middleware::from_fn). Place it below the Sentry
/// tower layers so that `Hub::current()` returns the request-scoped hub.
pub async fn bind_sentry_body(request: Request, next: Next) -> Response {
    let hub = sentry::Hub::current();
    next.run(request)
        .await
        .map(|body| Body::new(SentryBody::new(hub, body)))
}

/// A middleware that logs web request timings as metrics.
///
/// Use this with [`from_fn`](axum::middleware::from_fn).
///
/// The request-duration metric is measured **end-to-end**: the timing guard is moved into
/// the response body (see [`MetricsBody`]) so it is dropped only once the body has finished
/// streaming, not when the handler produces the response headers.
///
/// Internal routes (see [`is_internal_route`]) are excluded from metrics.
pub async fn emit_request_metrics(mut request: Request, next: Next) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());
    let service = request.extract_parts::<DownstreamService>().await.unwrap();

    let should_emit = !is_internal_route(route);
    let guard = should_emit.then(|| EmitMetricsGuard::new(route, request.method(), service));

    let response = next.run(request).await;

    // Move the guard into the response body so the duration metric is emitted only when the
    // body has finished streaming. The header status is applied on successful completion.
    match guard {
        Some(guard) => {
            let status = response.status();
            response.map(|body| Body::new(MetricsBody::new(guard, status, body)))
        }
        None => response,
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

    use super::*;

    fn make_request(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    /// Builds a test router with [`limit_web_concurrency`] applied.
    ///
    /// The `/v1/test/_/key` handler notifies `paused` on entry and waits on `resume`
    /// before returning, allowing tests to hold a request in-flight. Internal routes
    /// (`/health`, `/ready`, `/keda`) return 200 immediately.
    fn make_app(max: usize) -> (Router, Arc<Notify>, Arc<Notify>) {
        let counter = RequestCounter::new(max);
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
            .layer(counter.layer())
            .layer(from_fn_with_state(counter, limit_web_concurrency));

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

        // Regular request is rejected; internal routes bypass the limit.
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

    /// The request-duration metric must be emitted only once the response body has finished
    /// streaming, not when the handler produces the response headers.
    ///
    /// This runs on a current-thread runtime inside [`with_capturing_test_client`] so all
    /// metric emissions happen on the capturing thread. A sentinel `test.marker` metric is
    /// emitted after the response headers are received but before the body is consumed; the
    /// duration metric must appear *after* that sentinel.
    #[test]
    fn duration_measured_end_to_end() {
        use axum::body::{self, Bytes};
        use axum::middleware::from_fn;

        let captured = objectstore_metrics::with_capturing_test_client(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let app = Router::new()
                    .route(
                        "/v1/test/_/key",
                        get(|| async {
                            let stream = async_stream::stream! {
                                yield Ok::<_, std::io::Error>(Bytes::from_static(b"hello"));
                            };
                            Body::from_stream(stream).into_response()
                        }),
                    )
                    .layer(from_fn(emit_request_metrics));

                let resp = app.oneshot(make_request("/v1/test/_/key")).await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);

                // Headers received. The duration metric must not have been emitted yet.
                objectstore_metrics::count!("test.marker");

                // Consuming the body drops the wrapping `MetricsBody` and its guard, which
                // emits the duration metric.
                let bytes = body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
                assert_eq!(&bytes[..], b"hello");
            });
        });

        let marker = captured
            .iter()
            .position(|m| m.starts_with("test.marker:"))
            .expect("sentinel marker not captured");
        let duration = captured
            .iter()
            .position(|m| m.starts_with("server.requests.duration:"))
            .expect("duration metric not captured");

        assert!(
            duration > marker,
            "duration metric emitted before body was consumed: {captured:?}"
        );
    }
}
