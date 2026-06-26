//! Stream wrapper that binds a Sentry hub around every poll.
//!
//! Axum handlers return response bodies that are polled by hyper after the
//! middleware stack unwinds, so `Hub::current()` inside a stream producer
//! resolves to whatever hub the tokio worker thread happens to have — not the
//! request's hub. [`SentryStream`] fixes this by re-activating the captured
//! hub on every `poll_next`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::Stream;
use pin_project_lite::pin_project;
use sentry::{Hub, HubSwitchGuard};

pin_project! {
    /// Wraps a stream and re-activates a Sentry hub on every poll.
    ///
    /// The hub is activated for the duration of each [`Stream::poll_next`] call
    /// and released immediately after, so downstream producers always see the
    /// correct request hub regardless of which tokio thread they run on.
    #[derive(Debug)]
    pub struct SentryStream<S> {
        hub: Arc<Hub>,
        #[pin]
        inner: S,
    }
}

impl<S> SentryStream<S> {
    /// Creates a new [`SentryStream`] that binds `hub` around polls of `inner`.
    pub fn new(hub: Arc<Hub>, inner: S) -> Self {
        Self { hub, inner }
    }
}

impl<S: Stream> Stream for SentryStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let _guard = HubSwitchGuard::new(Arc::clone(this.hub));
        this.inner.poll_next(cx)
    }
}

/// Extension trait that binds a Sentry hub to any [`Stream`], mirroring
/// [`sentry::SentryFutureExt`].
pub trait SentryStreamExt: Sized {
    /// Wraps the stream so the given hub is active on every [`Stream::poll_next`] call.
    fn bind_hub<H>(self, hub: H) -> SentryStream<Self>
    where
        H: Into<Arc<Hub>>,
    {
        SentryStream::new(hub.into(), self)
    }
}

impl<S: Stream> SentryStreamExt for S {}
