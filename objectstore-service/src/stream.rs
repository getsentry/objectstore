//! Payload stream type, cancellable wrapper, and test utilities.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::Stream;
use futures_util::stream::BoxStream;
use tokio_util::sync::CancellationToken;

/// Type alias for data streams used in service APIs.
pub type PayloadStream = BoxStream<'static, io::Result<bytes::Bytes>>;

/// Wraps a [`PayloadStream`] to check a cancellation token before each chunk.
///
/// When the token is cancelled, the next poll returns an I/O error with
/// [`ErrorKind::ConnectionAborted`](io::ErrorKind::ConnectionAborted), causing
/// the consuming backend to abort the in-flight HTTP request. Once the stream
/// is exhausted (all chunks yielded), cancellation has no effect — the
/// operation completes normally.
pub(crate) struct CancellableStream {
    inner: PayloadStream,
    token: CancellationToken,
}

impl std::fmt::Debug for CancellableStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancellableStream")
            .field("cancelled", &self.token.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl CancellableStream {
    /// Creates a new `CancellableStream`.
    pub(crate) fn new(inner: PayloadStream, token: CancellationToken) -> Self {
        Self { inner, token }
    }

    /// Converts this into a boxed [`PayloadStream`].
    pub(crate) fn into_stream(self) -> PayloadStream {
        Box::pin(self)
    }
}

impl Stream for CancellableStream {
    type Item = io::Result<bytes::Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.token.is_cancelled() {
            return Poll::Ready(Some(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "stream cancelled",
            ))));
        }
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Creates a [`PayloadStream`] from a byte slice.
#[cfg(test)]
pub(crate) fn make_stream(contents: &[u8]) -> PayloadStream {
    use futures_util::StreamExt;
    tokio_stream::once(Ok(contents.to_vec().into())).boxed()
}

/// Collects a [`PayloadStream`] into a `Vec<u8>`.
#[cfg(test)]
pub(crate) async fn read_to_vec(mut stream: PayloadStream) -> crate::error::Result<Vec<u8>> {
    use futures_util::TryStreamExt;
    let mut payload = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        payload.extend(&chunk);
    }
    Ok(payload)
}

/// Creates a [`PayloadStream`] that yields multiple chunks of the given size.
#[cfg(test)]
pub(crate) fn make_chunked_stream(contents: &[u8], chunk_size: usize) -> PayloadStream {
    use futures_util::StreamExt;
    let chunks: Vec<io::Result<bytes::Bytes>> = contents
        .chunks(chunk_size)
        .map(|c| Ok(bytes::Bytes::copy_from_slice(c)))
        .collect();
    futures_util::stream::iter(chunks).boxed()
}

/// A stream wrapper that cancels a token after yielding a specified number of
/// chunks. Used in tests to deterministically trigger cancellation mid-stream.
#[cfg(test)]
pub(crate) struct CancelAfterChunks {
    inner: PayloadStream,
    token: CancellationToken,
    cancel_after: usize,
    yielded: usize,
}

#[cfg(test)]
impl CancelAfterChunks {
    /// Creates a new `CancelAfterChunks` that cancels `token` after `cancel_after` chunks.
    pub(crate) fn new(inner: PayloadStream, token: CancellationToken, cancel_after: usize) -> Self {
        Self {
            inner,
            token,
            cancel_after,
            yielded: 0,
        }
    }

    /// Boxes this into a [`PayloadStream`].
    pub(crate) fn into_stream(self) -> PayloadStream {
        Box::pin(self)
    }
}

#[cfg(test)]
impl Stream for CancelAfterChunks {
    type Item = io::Result<bytes::Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(Ok(_))) = &result {
            self.yielded += 1;
            if self.yielded >= self.cancel_after {
                self.token.cancel();
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn cancellable_stream_passes_through_when_not_cancelled() {
        let token = CancellationToken::new();
        let inner = make_stream(b"hello world");
        let stream = CancellableStream::new(inner, token);

        let data = read_to_vec(stream.into_stream()).await.unwrap();
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn cancellable_stream_errors_when_cancelled() {
        let token = CancellationToken::new();
        let inner = make_chunked_stream(b"aaabbbccc", 3);
        let mut stream = CancellableStream::new(inner, token.clone());

        // First chunk succeeds.
        let chunk = stream.try_next().await.unwrap();
        assert_eq!(chunk.as_deref(), Some(b"aaa".as_slice()));

        // Cancel the token.
        token.cancel();

        // Next poll returns ConnectionAborted.
        let err = stream.try_next().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionAborted);
    }

    #[tokio::test]
    async fn cancellable_stream_yields_remaining_after_cancel_before_poll() {
        // Cancellation is checked before delegating to inner, so even if
        // there are remaining chunks, the error fires immediately.
        let token = CancellationToken::new();
        let inner = make_chunked_stream(b"aaabbb", 3);
        let mut stream = CancellableStream::new(inner, token.clone());

        // Cancel before any poll.
        token.cancel();

        let err = stream.try_next().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionAborted);
    }
}
