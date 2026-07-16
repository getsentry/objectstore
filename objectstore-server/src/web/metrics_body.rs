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
