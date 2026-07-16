//! Response body wrapper that keeps a metrics guard alive until the body ends.
//!
//! Axum handlers produce response *headers* while the middleware stack unwinds,
//! but the response *body* is streamed by hyper afterwards. To measure the true
//! end-to-end request duration, the timing guard must live until the body has
//! been fully streamed. [`MetricsBody`] owns an [`EmitMetricsGuard`] and drops
//! it when the inner body reaches end-of-stream (or is dropped on client
//! disconnect), at which point the guard emits `server.requests.duration`.
//!
//! When the body finishes, [`MetricsBody`] calls
//! [`EmitMetricsGuard::mark_completed`] so the metric is tagged with the real
//! response status. Detecting completion is not as simple as watching for
//! `Poll::Ready(None)`: hyper only polls a body while
//! [`is_end_stream`](http_body::Body::is_end_stream) is false, so empty bodies
//! are never polled and buffered (`Full`) bodies stop after their single data
//! frame — neither ever yields `Ready(None)`.
//! [`MetricsBody`] therefore also checks `is_end_stream` at construction and
//! after each data frame. A server-side stream error instead calls
//! [`EmitMetricsGuard::mark_errored`] and is reported as `500`. If the body is
//! dropped before any of these happen — a client disconnect — the guard reports
//! a `499` status.
//!
//! Because this delegates [`size_hint`](http_body::Body::size_hint) and
//! [`is_end_stream`](http_body::Body::is_end_stream) to the inner body,
//! buffered responses keep their exact size hint and hyper still emits a
//! `Content-Length` header instead of chunked encoding.

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{Method, StatusCode};
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use pin_project_lite::pin_project;
use tokio::time::Instant;

use crate::extractors::downstream_service::DownstreamService;

/// How response-body streaming ended, which determines the `status` tag when it differs from
/// the status sent in the headers.
#[derive(Clone, Copy)]
enum BodyOutcome {
    /// The body was dropped before streaming completed, reported as `499`.
    Pending,
    /// The body streamed to completion, reported with the response status.
    Completed,
    /// The body yielded a server-side error, reported as `500`.
    Errored,
}

/// Tracks request timing and emits `server.requests.duration` when dropped.
///
/// [`MetricsBody`] owns this guard so request duration spans full response-body streaming.
pub(crate) struct EmitMetricsGuard {
    route: String,
    method: Method,
    start: Instant,
    status: Option<StatusCode>,
    outcome: BodyOutcome,
}

impl EmitMetricsGuard {
    pub(crate) fn new(route: &str, method: &Method, service: DownstreamService) -> Self {
        objectstore_metrics::count!(
            "server.requests",
            route = route.to_owned(),
            method = method.as_str().to_owned(),
            service = service.to_string(),
        );

        Self {
            route: route.to_owned(),
            method: method.clone(),
            start: Instant::now(),
            status: None,
            outcome: BodyOutcome::Pending,
        }
    }

    /// Records the response status to use after successful body completion.
    pub(crate) fn finish(&mut self, status: StatusCode) {
        self.status = Some(status);
    }

    /// Marks the response body as having streamed to completion.
    fn mark_completed(&mut self) {
        self.outcome = BodyOutcome::Completed;
    }

    /// Marks the response body stream as having errored server-side.
    fn mark_errored(&mut self) {
        self.outcome = BodyOutcome::Errored;
    }
}

impl Drop for EmitMetricsGuard {
    fn drop(&mut self) {
        let status = match self.outcome {
            BodyOutcome::Completed => self.status.map(|status| status.as_u16()).unwrap_or(499),
            BodyOutcome::Errored => 500,
            BodyOutcome::Pending => 499,
        }
        .to_string();
        objectstore_metrics::record!(
            "server.requests.duration" = self.start.elapsed(),
            route = self.route.clone(),
            method = self.method.as_str().to_owned(),
            status = status,
            // service omitted to limit cardinality
        );
    }
}

pin_project! {
    /// Wraps an axum [`Body`] and holds an [`EmitMetricsGuard`] until the body ends.
    ///
    /// The guard emits the request-duration metric on drop, so keeping it inside
    /// the body defers that emission until streaming completes.
    pub struct MetricsBody {
        // Dropped together with the body once streaming finishes; its `Drop`
        // emits the request-duration metric.
        guard: EmitMetricsGuard,
        #[pin]
        inner: Body,
    }
}

impl MetricsBody {
    /// Creates a new [`MetricsBody`] that keeps `guard` alive while polling `inner`.
    pub fn new(mut guard: EmitMetricsGuard, inner: Body) -> Self {
        // A body that already reports end-of-stream (e.g. an empty body) may never be polled
        // by hyper at all — it sets `body_rx = None` up front and writes a `Content-Length: 0`
        // response without ever driving the body. Detect that here so the guard is not left
        // `Pending` (which would misreport `499`).
        if inner.is_end_stream() {
            guard.mark_completed();
        }
        Self { guard, inner }
    }
}

impl http_body::Body for MetricsBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        let poll = this.inner.as_mut().poll_frame(cx);
        // Detect completion so the guard reports the real status instead of `499`:
        // - `Ready(None)` is the end-of-stream signal for ordinary streamed bodies.
        // - A buffered body (`Full`) yields its single data frame and then reports
        //   `is_end_stream()`; hyper stops polling after that frame without ever returning
        //   `Ready(None)`, so we must check `is_end_stream()` after each frame too.
        // - Hyper treats trailers as terminal and drops the body without polling again.
        // A `Ready(Some(Err))` frame is a server-side stream error, reported as `500`. If none
        // of these is observed before the body is dropped (client disconnect), the guard stays
        // pending and reports `499`.
        match &poll {
            Poll::Ready(None) => this.guard.mark_completed(),
            Poll::Ready(Some(Ok(frame))) if frame.is_trailers() || this.inner.is_end_stream() => {
                this.guard.mark_completed()
            }
            Poll::Ready(Some(Err(_))) => this.guard.mark_errored(),
            _ => {}
        }
        poll
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use axum::Router;
    use axum::body::{self, Body, Bytes};
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::middleware::from_fn;
    use axum::routing::get;
    use http_body::Frame;
    use tower::ServiceExt;

    use crate::web::middleware::emit_request_metrics;

    fn make_request(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    /// Runs a request whose handler returns `body`, driving it to completion on a
    /// current-thread runtime, and returns the captured `server.requests.duration` metric
    /// string. If `consume_body` is false, the response body is dropped without being read,
    /// simulating a client that disconnects mid-stream.
    fn capture_duration_metric(body: Body, consume_body: bool) -> String {
        let captured = objectstore_metrics::with_capturing_test_client(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let shared = Arc::new(std::sync::Mutex::new(Some(body)));
                let app = Router::new()
                    .route(
                        "/v1/test/_/key",
                        get(move || {
                            let shared = shared.clone();
                            async move { shared.lock().unwrap().take().unwrap() }
                        }),
                    )
                    .layer(from_fn(emit_request_metrics));

                let resp = app.oneshot(make_request("/v1/test/_/key")).await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);

                if consume_body {
                    // Drain the body to end-of-stream (or until it errors); the result is
                    // ignored so a server-side stream error does not panic the test.
                    let _ = body::to_bytes(resp.into_body(), usize::MAX).await;
                } else {
                    // Drop the response (and its body) without reading it: the wrapping
                    // `MetricsBody` never reaches end-of-stream, mirroring a client disconnect.
                    drop(resp);
                }
            });
        });

        captured
            .into_iter()
            .find(|m| m.starts_with("server.requests.duration:"))
            .expect("duration metric not captured")
    }

    /// A body streamed to completion reports the real response status.
    #[test]
    fn completed_stream_reports_real_status() {
        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"hello"));
        };
        let metric = capture_duration_metric(Body::from_stream(stream), true);
        assert!(metric.contains("status:200"), "unexpected metric: {metric}");
    }

    /// A body that needs no polling is completed when it is wrapped, because hyper may not poll
    /// it before dropping it.
    #[test]
    fn empty_body_reports_real_status() {
        let metric = capture_duration_metric(Body::empty(), false);
        assert!(metric.contains("status:200"), "unexpected metric: {metric}");
    }

    /// A buffered body completes after its final data frame, without requiring a subsequent poll
    /// that returns `None`.
    #[test]
    fn buffered_body_reports_real_status() {
        let metric = capture_duration_metric(Body::from("hello"), true);
        assert!(metric.contains("status:200"), "unexpected metric: {metric}");
    }

    /// Hyper treats trailers as terminal, so completion must be recorded when their frame is
    /// yielded rather than waiting for an unobserved following `None`.
    #[test]
    fn trailers_report_real_status() {
        struct TrailersBody(bool);

        impl http_body::Body for TrailersBody {
            type Data = Bytes;
            type Error = Infallible;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Ready(if self.0 {
                    None
                } else {
                    self.0 = true;
                    Some(Ok(Frame::trailers(HeaderMap::new())))
                })
            }
        }

        let metric = capture_duration_metric(Body::new(TrailersBody(false)), true);
        assert!(metric.contains("status:200"), "unexpected metric: {metric}");
    }

    /// A body dropped before end-of-stream (client disconnect) reports `499`, overriding the
    /// `200` status that was sent in the headers.
    #[test]
    fn interrupted_stream_reports_499() {
        // A stream that yields one chunk then pends forever, so it never reaches end-of-stream.
        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"hello"));
            std::future::pending::<()>().await;
        };
        let metric = capture_duration_metric(Body::from_stream(stream), false);
        assert!(metric.contains("status:499"), "unexpected metric: {metric}");
    }

    /// A body stream that errors server-side reports `500`, overriding the `200` status that
    /// was sent in the headers.
    #[test]
    fn errored_stream_reports_500() {
        let stream = async_stream::stream! {
            yield Ok(Bytes::from_static(b"hello"));
            yield Err(std::io::Error::other("boom"));
        };
        let metric = capture_duration_metric(Body::from_stream(stream), true);
        assert!(metric.contains("status:500"), "unexpected metric: {metric}");
    }
}
