//! Response body wrapper that keeps a metrics guard alive until the body ends.
//!
//! Axum handlers produce response *headers* while the middleware stack unwinds,
//! but the response *body* is streamed by hyper afterwards. To measure the true
//! end-to-end request duration, the timing guard must live until the body has
//! been fully streamed. [`MetricsBody`] owns an [`EmitMetricsGuard`] and drops
//! it when the inner body reaches end-of-stream (or is dropped on client
//! disconnect), at which point the guard emits `server.requests.duration`.
//!
//! When the body streams to completion, [`MetricsBody`] calls
//! [`EmitMetricsGuard::mark_completed`] so the metric is tagged with the real
//! response status. A server-side stream error instead calls
//! [`EmitMetricsGuard::mark_errored`] and is reported as `500`. If the body is
//! dropped before either happens — a client disconnect — the guard reports a
//! `499` status.
//!
//! Because this delegates [`size_hint`](http_body::Body::size_hint) and
//! [`is_end_stream`](http_body::Body::is_end_stream) to the inner body,
//! buffered responses keep their exact size hint and hyper still emits a
//! `Content-Length` header instead of chunked encoding.

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use pin_project_lite::pin_project;

use crate::web::middleware::EmitMetricsGuard;

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
    pub fn new(guard: EmitMetricsGuard, inner: Body) -> Self {
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
        let this = self.project();
        let poll = this.inner.poll_frame(cx);
        // `Ready(None)` is the only end-of-stream signal that is uniform across buffered and
        // streamed bodies (`StreamBody` does not override `is_end_stream`). Reaching it means
        // the body streamed to completion. A `Ready(Some(Err))` frame is a server-side stream
        // error. If neither is observed before the body is dropped (client disconnect), the
        // guard stays pending and reports `499`.
        match &poll {
            Poll::Ready(None) => this.guard.mark_completed(),
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
