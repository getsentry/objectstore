//! Response body wrapper that emits request-duration metrics after the body finishes.

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{Method, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use pin_project_lite::pin_project;
use tokio::time::Instant;

use crate::extractors::downstream_service::DownstreamService;

/// State of a response body.
#[derive(Clone, Copy, Default)]
enum BodyState {
    /// The body is still streaming.
    #[default]
    Pending,
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
    body_state: BodyState,
}

impl EmitMetricsGuard {
    pub fn new(route: &str, method: &Method, service: DownstreamService) -> Self {
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
            body_state: BodyState::Pending,
        }
    }

    fn complete(&mut self, status: StatusCode) {
        self.body_state = BodyState::Completed(status);
    }

    fn mark_errored(&mut self) {
        self.body_state = BodyState::Errored;
    }
}

impl Drop for EmitMetricsGuard {
    fn drop(&mut self) {
        let state = match self.body_state {
            BodyState::Pending => 499,
            BodyState::Completed(status) => status.as_u16(),
            BodyState::Errored => 500,
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
    ///
    /// The body checks three conditions where hyper stops polling the body before it yields `None`:
    ///  1. An explicit `content-length` was specified and that many bytes were written.
    ///  2. For a buffered body, hyper yields a single frame.
    ///  3. For a body with trailers, hyper yields the trailers frame and then drops the body.
    pub struct MetricsBody {
        #[pin]
        inner: Body,
        guard: EmitMetricsGuard,
        status: StatusCode,
        remaining: Option<u64>,
    }
}

impl MetricsBody {
    /// Wraps a response body with a [`MetricsBody`] that keeps `guard` alive until the body ends.
    pub fn wrap(response: Response, mut guard: EmitMetricsGuard) -> Response {
        let status = response.status();
        let content_length = response
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());

        if content_length == Some(0) || response.body().is_end_stream() {
            // Fast-path: the body won't be polled, so we complete the guard immediately
            guard.complete(status);
            response
        } else {
            response.map(|inner| {
                Body::new(Self {
                    guard,
                    inner,
                    status,
                    remaining: content_length,
                })
            })
        }
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
            Poll::Ready(None) => this.guard.complete(*this.status),
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(remaining) = this.remaining.as_mut() {
                    let frame_len = frame.data_ref().map_or(0, |data| data.len() as u64);
                    *remaining = remaining.saturating_sub(frame_len);
                }

                if *this.remaining == Some(0) || this.inner.is_end_stream() || frame.is_trailers() {
                    this.guard.complete(*this.status);
                }
            }
            Poll::Ready(Some(Err(_))) => this.guard.mark_errored(),
            Poll::Pending => {}
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
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use axum::Router;
    use axum::body::{self, Body, Bytes};
    use axum::handler::Handler;
    use axum::http::{HeaderMap, Request, header};
    use axum::middleware::from_fn;
    use axum::routing::get;
    use futures::StreamExt;
    use http_body::Frame;
    use tower::ServiceExt;

    use crate::web::middleware::emit_request_metrics;

    /// How the client consumes the response body.
    enum Client {
        /// Reads the body to end-of-stream.
        ReadToEnd,
        /// Reads a single chunk and then disconnects, like hyper, which stops polling once
        /// `content-length` bytes have been written.
        ReadOneChunk,
        /// Disconnects without reading the body.
        Disconnect,
    }

    impl Client {
        async fn consume(self, response: axum::response::Response) {
            // Read results are ignored: a failing stream must be tracked, not panic the test.
            match self {
                Client::ReadToEnd => {
                    let _ = body::to_bytes(response.into_body(), usize::MAX).await;
                }
                Client::ReadOneChunk => {
                    let _ = response.into_body().into_data_stream().next().await;
                }
                Client::Disconnect => drop(response),
            }
        }
    }

    /// Serves `handler` behind [`emit_request_metrics`] and returns the status and duration
    /// tracked in `server.requests.duration`.
    async fn track_request<H, T>(handler: H, client: Client) -> (u16, Duration)
    where
        H: Handler<T, ()>,
        T: 'static,
    {
        let app = Router::new()
            .route("/", get(handler))
            .layer(from_fn(emit_request_metrics));

        let captured = objectstore_metrics::with_capturing_test_client_async(async move {
            let request = Request::get("/").body(Body::empty()).unwrap();
            let response = app.oneshot(request).await.unwrap();
            client.consume(response).await;
        })
        .await;

        // The metric is formatted as `server.requests.duration:<seconds>|d|#<tags>`.
        let metric = captured
            .iter()
            .find_map(|m| m.strip_prefix("server.requests.duration:"))
            .expect("duration metric not captured");
        let (seconds, tags) = metric
            .split_once("|d|#")
            .expect("malformed duration metric");
        let (_, status) = tags.rsplit_once("status:").expect("status tag");

        let status = status.parse().expect("numeric status");
        let duration = Duration::from_secs_f64(seconds.parse().expect("numeric duration"));
        (status, duration)
    }

    /// A stream that yields one chunk and then ends.
    fn ending_stream() -> Body {
        Body::from_stream(async_stream::stream! {
            yield Ok::<_, io::Error>(Bytes::from_static(b"hello"));
        })
    }

    /// A stream that yields one chunk and then never ends.
    fn stalling_stream() -> Body {
        Body::from_stream(async_stream::stream! {
            yield Ok::<_, io::Error>(Bytes::from_static(b"hello"));
            std::future::pending::<()>().await;
        })
    }

    /// A stream that yields one chunk after [`STREAM_DELAY`] and then ends.
    fn slow_stream(delay: Duration) -> Body {
        Body::from_stream(async_stream::stream! {
            tokio::time::sleep(delay).await;
            yield Ok::<_, io::Error>(Bytes::from_static(b"hello"));
        })
    }

    /// A stream that yields one chunk and then fails.
    fn failing_stream() -> Body {
        Body::from_stream(async_stream::stream! {
            yield Ok(Bytes::from_static(b"hello"));
            yield Err(io::Error::other("boom"));
        })
    }

    /// A body that yields a trailers frame before its end-of-stream.
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

    #[tokio::test]
    async fn completed_stream_reports_real_status() {
        let (status, _) = track_request(|| async { ending_stream() }, Client::ReadToEnd).await;
        assert_eq!(status, 200);
    }

    /// Hyper may drop an empty body without ever polling it.
    #[tokio::test]
    async fn empty_body_reports_real_status() {
        let (status, _) = track_request(|| async { Body::empty() }, Client::Disconnect).await;
        assert_eq!(status, 200);
    }

    /// A buffered body reports end-of-stream with its final frame, so no `None` poll follows.
    #[tokio::test]
    async fn buffered_body_reports_real_status() {
        let (status, _) = track_request(|| async { Body::from("hello") }, Client::ReadToEnd).await;
        assert_eq!(status, 200);
    }

    /// Hyper treats a trailers frame as terminal and never polls for the following `None`.
    #[tokio::test]
    async fn trailers_report_real_status() {
        let handler = || async { Body::new(TrailersBody(false)) };
        let (status, _) = track_request(handler, Client::ReadToEnd).await;
        assert_eq!(status, 200);
    }

    /// Hyper stops polling once `content-length` bytes have been written, so completion is
    /// derived from the byte count rather than a following end-of-stream poll.
    #[tokio::test]
    async fn content_length_stream_reports_real_status() {
        let handler = || async { ([(header::CONTENT_LENGTH, "5")], stalling_stream()) };
        let (status, _) = track_request(handler, Client::ReadOneChunk).await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn interrupted_stream_reports_499() {
        let (status, _) = track_request(|| async { stalling_stream() }, Client::Disconnect).await;
        assert_eq!(status, 499);
    }

    #[tokio::test]
    async fn errored_stream_reports_500() {
        let (status, _) = track_request(|| async { failing_stream() }, Client::ReadToEnd).await;
        assert_eq!(status, 500);
    }

    #[tokio::test(start_paused = true)]
    async fn duration_covers_body_streaming() {
        let handler = || async { slow_stream(Duration::from_secs(5)) };
        let (_, duration) = track_request(handler, Client::ReadToEnd).await;
        assert_eq!(duration, Duration::from_secs(5));
    }
}
