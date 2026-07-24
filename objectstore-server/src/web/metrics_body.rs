//! Response body wrapper that emits request-duration metrics after the body finishes.

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{Method, StatusCode};
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use pin_project_lite::pin_project;
use tokio::time::Instant;

use crate::extractors::downstream_service::DownstreamService;

/// State of a response body.
#[derive(Clone, Copy)]
enum BodyState {
    /// The body is still streaming.
    Streaming(StatusCode),
    /// The body was streamed to completion.
    Completed(StatusCode),
    /// An error was encountered while streaming the body.
    Errored,
}

/// Tracks request timing and emits `server.requests.duration` when dropped.
///
/// [`MetricsBody`] owns this guard so request duration spans full response-body streaming.
pub(crate) struct EmitMetricsGuard {
    route: String,
    method: Method,
    start: Instant,
    body_state: Option<BodyState>,
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
            body_state: None,
        }
    }

    fn set_body_state(&mut self, state: BodyState) {
        self.body_state = Some(state);
    }
}

impl Drop for EmitMetricsGuard {
    fn drop(&mut self) {
        let state = match self.body_state {
            Some(BodyState::Completed(status)) => status.as_u16(),
            Some(BodyState::Streaming(_)) | None => 499,
            Some(BodyState::Errored) => 500,
        };
        objectstore_metrics::record!(
            "server.requests.duration" = self.start.elapsed(),
            route = self.route.clone(),
            method = self.method.as_str().to_owned(),
            status = state.to_string(),
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
        guard: EmitMetricsGuard,
        #[pin]
        inner: Body,
    }
}

impl MetricsBody {
    /// Creates a new [`MetricsBody`] that keeps `guard` alive while polling `inner`.
    ///
    /// `status` is the response status from headers.
    pub fn new(mut guard: EmitMetricsGuard, status: StatusCode, inner: Body) -> Self {
        // An empty response body reports end-of-stream immediately and is never polled by hyper,
        // so mark it completed up front.
        if inner.is_end_stream() {
            guard.set_body_state(BodyState::Completed(status));
        } else {
            guard.set_body_state(BodyState::Streaming(status));
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
        match &poll {
            // End-of-stream for a streamed body.
            Poll::Ready(None) => {
                if let Some(BodyState::Streaming(status)) = this.guard.body_state {
                    this.guard.set_body_state(BodyState::Completed(status));
                }
            }
            // End-of-stream for a buffered body (yields a single frame and reports end-of-stream
            // immediately, so hyper doesn't attempt to poll it again).
            Poll::Ready(Some(Ok(frame))) if frame.is_trailers() || this.inner.is_end_stream() => {
                if let Some(BodyState::Streaming(status)) = this.guard.body_state {
                    this.guard.set_body_state(BodyState::Completed(status));
                }
            }
            Poll::Ready(Some(Err(_))) => this.guard.set_body_state(BodyState::Errored),
            _ => {}
        }
        poll
    }

    fn size_hint(&self) -> SizeHint {
        // Delegating [`size_hint`](http_body::Body::size_hint) preserves `Content-Length` on buffered
        // responses instead of forcing chunked encoding.
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
