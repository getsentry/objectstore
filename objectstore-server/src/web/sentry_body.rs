//! Response body wrapper that binds a Sentry hub around every poll.
//!
//! Axum handlers return response bodies that are polled by hyper after the
//! middleware stack unwinds, so `Hub::current()` inside a lazy body producer
//! resolves to whatever hub the tokio worker thread happens to have — not the
//! request's hub. [`SentryBody`] fixes this by re-activating the captured hub
//! on every `poll_frame`.
//!
//! Because this delegates [`size_hint`](http_body::Body::size_hint) and
//! [`is_end_stream`](http_body::Body::is_end_stream) to the inner body,
//! buffered responses keep their exact size hint and hyper still emits a
//! `Content-Length` header instead of chunked encoding.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use pin_project_lite::pin_project;
use sentry::{Hub, HubSwitchGuard};

pin_project! {
    /// Wraps an axum [`Body`] and re-activates a Sentry hub on every poll.
    ///
    /// The hub is activated for the duration of each [`poll_frame`](http_body::Body::poll_frame)
    /// call and released immediately after, so downstream producers always see the
    /// correct request hub regardless of which tokio thread they run on.
    pub struct SentryBody {
        hub: Arc<Hub>,
        #[pin]
        inner: Body,
    }
}

impl SentryBody {
    /// Creates a new [`SentryBody`] that binds `hub` around polls of `inner`.
    pub fn new(hub: Arc<Hub>, inner: Body) -> Self {
        Self { hub, inner }
    }
}

impl http_body::Body for SentryBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let _guard = HubSwitchGuard::new(Arc::clone(this.hub));
        this.inner.poll_frame(cx)
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
}
