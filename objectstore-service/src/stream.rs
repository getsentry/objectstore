//! Payload stream type and zero-copy buffering utilities.

use std::io;

use bytes::{Bytes, BytesMut};
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt, TryStreamExt};

/// Type alias for data streams used in service APIs.
pub type PayloadStream = BoxStream<'static, io::Result<Bytes>>;

#[derive(Debug, Default)]
enum ChunkedBytesState {
    #[default]
    Empty,
    Single(Bytes),
    Multi(BytesMut),
}

/// Lazy stream buffer that avoids copying single chunks.
///
/// Tracks three internal states:
/// - **Empty** — no allocation, no data.
/// - **Single** — first chunk stored by move (zero-copy).
/// - **Multi** — allocated on second `push`, coalesces all chunks.
#[derive(Debug)]
pub(crate) struct ChunkedBytes {
    state: ChunkedBytesState,
    capacity: usize,
}

impl ChunkedBytes {
    /// Creates a new buffer with the given capacity hint.
    ///
    /// The capacity is used when transitioning from Single to Multi state
    /// and is exposed via [`capacity()`](Self::capacity) for use as a buffer limit.
    pub fn new(capacity: usize) -> Self {
        Self {
            state: ChunkedBytesState::Empty,
            capacity,
        }
    }

    /// Appends a chunk to the buffer.
    ///
    /// Empty→Single stores by move (zero-copy). Single→Multi allocates and
    /// copies both chunks. Multi extends the existing allocation.
    pub fn push(&mut self, chunk: Bytes) {
        if chunk.is_empty() {
            return;
        }
        self.state = match std::mem::take(&mut self.state) {
            ChunkedBytesState::Empty => ChunkedBytesState::Single(chunk),
            ChunkedBytesState::Single(first) => {
                let capacity = self.capacity.max(first.len() + chunk.len());
                let mut buf = BytesMut::with_capacity(capacity);
                buf.extend_from_slice(&first);
                buf.extend_from_slice(&chunk);
                ChunkedBytesState::Multi(buf)
            }
            ChunkedBytesState::Multi(mut buf) => {
                buf.extend_from_slice(&chunk);
                ChunkedBytesState::Multi(buf)
            }
        };
    }

    /// Returns the configured capacity (used as the buffer limit).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the total number of buffered bytes.
    pub fn len(&self) -> usize {
        match &self.state {
            ChunkedBytesState::Empty => 0,
            ChunkedBytesState::Single(b) => b.len(),
            ChunkedBytesState::Multi(b) => b.len(),
        }
    }

    /// Returns `true` if no bytes have been buffered.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consumes the buffer and returns the data as a single `Bytes`.
    ///
    /// Single-chunk data is returned by move (zero-copy). Multi-chunk data
    /// is frozen from the internal `BytesMut`.
    pub fn into_bytes(self) -> Bytes {
        match self.state {
            ChunkedBytesState::Empty => Bytes::new(),
            ChunkedBytesState::Single(b) => b,
            ChunkedBytesState::Multi(b) => b.freeze(),
        }
    }
}

/// Reads up to `limit` bytes from a stream to support size-based routing decisions.
///
/// Constructed via the async [`SizedPeek::new`], which reads eagerly so that
/// [`is_exhausted()`](Self::is_exhausted) and [`len()`](Self::len) are always valid.
/// The full stream (buffered prefix plus any unconsumed remainder) is recovered
/// with [`into_stream()`](Self::into_stream).
///
/// The buffer never exceeds `limit` bytes: the chunk that would cause overflow
/// is held separately and re-emitted by `into_stream` without copying.
pub(crate) struct SizedPeek<S> {
    buffer: ChunkedBytes,
    /// The first chunk that exceeded the limit; `None` when the stream was exhausted.
    pending: Option<Bytes>,
    /// `None` when the stream was fully consumed within the limit.
    stream: Option<S>,
}

impl<S> SizedPeek<S> {
    /// Returns `true` if the stream was fully consumed within the peek limit.
    pub fn is_exhausted(&self) -> bool {
        self.pending.is_none()
    }

    /// Returns the number of bytes held in the buffer (at most `limit`).
    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl<S> SizedPeek<S>
where
    S: Stream<Item = io::Result<Bytes>> + Unpin,
{
    /// Reads from `stream` into an internal buffer until `limit` bytes are accumulated
    /// or the stream ends.
    ///
    /// Uses strictly-greater-than comparison: a stream of exactly `limit` bytes is
    /// considered exhausted.
    pub async fn new(mut stream: S, limit: usize) -> io::Result<Self> {
        let mut buffer = ChunkedBytes::new(limit);

        while let Some(chunk) = stream.try_next().await? {
            if buffer.len() + chunk.len() > buffer.capacity() {
                return Ok(Self {
                    buffer,
                    pending: Some(chunk),
                    stream: Some(stream),
                });
            }
            buffer.push(chunk);
        }

        Ok(Self {
            buffer,
            pending: None,
            stream: None,
        })
    }

    /// Consumes self and returns all bytes as a single [`Bytes`].
    ///
    /// If the peek limit was exceeded, drains the remaining stream before
    /// returning. Always correct regardless of [`is_exhausted`](Self::is_exhausted).
    pub async fn into_bytes(mut self) -> io::Result<Bytes> {
        if let Some(pending) = self.pending.take() {
            self.buffer.push(pending);
        }
        if let Some(mut stream) = self.stream.take() {
            while let Some(chunk) = stream.try_next().await? {
                self.buffer.push(chunk);
            }
        }
        Ok(self.buffer.into_bytes())
    }
}

impl<S> SizedPeek<S>
where
    S: Stream<Item = io::Result<Bytes>>,
{
    /// Consumes self and returns a stream that yields the buffered prefix first,
    /// then any remaining data from the original stream.
    pub fn into_stream(self) -> impl Stream<Item = io::Result<Bytes>> {
        let leading = [self.buffer.into_bytes(), self.pending.unwrap_or_default()]
            .into_iter()
            .filter(|b| !b.is_empty())
            .map(Ok);

        let tail = futures_util::stream::iter(self.stream).flatten();
        futures_util::stream::iter(leading).chain(tail)
    }
}

/// Creates a [`PayloadStream`] from a byte slice.
#[cfg(test)]
pub fn make_stream(contents: &[u8]) -> PayloadStream {
    tokio_stream::once(Ok(contents.to_vec().into())).boxed()
}

/// Collects a stream of `Bytes` chunks into a `Vec<u8>`.
#[cfg(test)]
pub(crate) async fn read_to_vec<S>(mut stream: S) -> crate::error::Result<Vec<u8>>
where
    S: Stream<Item = io::Result<Bytes>> + Unpin,
{
    let mut payload = Vec::new();
    while let Some(chunk) = stream.try_next().await? {
        payload.extend(&chunk);
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use futures_util::stream;

    use super::*;

    // --- ChunkedBytes tests ---

    #[test]
    fn empty_into_bytes() {
        let buf = ChunkedBytes::new(1024);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.into_bytes().len(), 0);
    }

    #[test]
    fn single_chunk_zero_copy() {
        let original = Bytes::from_static(b"hello world");
        let ptr = original.as_ptr();

        let mut buf = ChunkedBytes::new(1024);
        buf.push(original);

        assert_eq!(buf.len(), 11);
        assert!(!buf.is_empty());

        let result = buf.into_bytes();
        assert_eq!(result.as_ref(), b"hello world");
        // Pointer equality proves zero-copy.
        assert_eq!(result.as_ptr(), ptr);
    }

    #[test]
    fn multi_chunk_coalesces() {
        let mut buf = ChunkedBytes::new(1024);
        buf.push(Bytes::from_static(b"hello "));
        buf.push(Bytes::from_static(b"world"));

        assert_eq!(buf.len(), 11);
        assert_eq!(buf.into_bytes().as_ref(), b"hello world");
    }

    #[test]
    fn push_empty_chunk_is_noop() {
        let original = Bytes::from_static(b"data");
        let ptr = original.as_ptr();

        let mut buf = ChunkedBytes::new(1024);
        buf.push(Bytes::new());
        assert!(buf.is_empty());

        buf.push(original);
        buf.push(Bytes::new());
        // Still in Single state — empty pushes don't trigger Multi.
        let result = buf.into_bytes();
        assert_eq!(result.as_ptr(), ptr);
    }

    #[test]
    fn capacity_is_preserved() {
        let buf = ChunkedBytes::new(42);
        assert_eq!(buf.capacity(), 42);
    }

    // --- SizedPeek tests ---

    #[tokio::test]
    async fn exhausted_when_stream_fits() {
        let s = stream::once(std::future::ready(Ok(Bytes::from_static(b"small payload"))));

        let peeked = SizedPeek::new(s, 1024).await.unwrap();

        assert!(peeked.is_exhausted());
        assert_eq!(peeked.len(), 13);
    }

    #[tokio::test]
    async fn remaining_when_stream_exceeds_limit() {
        let chunks: Vec<io::Result<Bytes>> = vec![
            Ok(Bytes::from(vec![0u8; 600])),
            Ok(Bytes::from(vec![1u8; 600])),
        ];

        let peeked = SizedPeek::new(stream::iter(chunks), 1000).await.unwrap();

        assert!(!peeked.is_exhausted());
        // Only the first 600-byte chunk fits in the buffer; the second is pending.
        assert_eq!(peeked.len(), 600);
    }

    #[tokio::test]
    async fn into_stream_reassembles_data() {
        // "aaa" fits (3 ≤ 5), "bbb" overflows (3+3=6 > 5) → pending, "ccc" stays in stream.
        let chunks: Vec<io::Result<Bytes>> = vec![
            Ok(Bytes::from_static(b"aaa")),
            Ok(Bytes::from_static(b"bbb")),
            Ok(Bytes::from_static(b"ccc")),
        ];

        let peeked = SizedPeek::new(stream::iter(chunks), 5).await.unwrap();
        assert!(!peeked.is_exhausted());

        let output = read_to_vec(peeked.into_stream()).await.unwrap();
        assert_eq!(output, b"aaabbbccc");
    }

    #[tokio::test]
    async fn into_stream_single_chunk_zero_copy() {
        let original = Bytes::from_static(b"zero-copy roundtrip");
        let ptr = original.as_ptr();
        let s = stream::once(std::future::ready(Ok(original)));

        let peeked = SizedPeek::new(s, 1024).await.unwrap();
        let out = peeked.into_stream();
        futures_util::pin_mut!(out);
        let first = out.try_next().await.unwrap().unwrap();
        assert_eq!(first.as_ptr(), ptr);
    }

    #[tokio::test]
    async fn oversized_single_chunk_goes_to_pending() {
        // A single chunk larger than the limit never enters the buffer.
        let big = Bytes::from(vec![0u8; 2000]);
        let ptr = big.as_ptr();
        let s = stream::once(std::future::ready(Ok(big)));

        let peeked = SizedPeek::new(s, 1000).await.unwrap();
        assert!(!peeked.is_exhausted());
        assert_eq!(peeked.len(), 0); // nothing in buffer

        let out = peeked.into_stream();
        futures_util::pin_mut!(out);
        let first = out.try_next().await.unwrap().unwrap();
        // The pending chunk is emitted by move — same pointer, no copy.
        assert_eq!(first.as_ptr(), ptr);
        assert_eq!(first.len(), 2000);
    }

    #[tokio::test]
    async fn error_propagation() {
        let chunks: Vec<io::Result<Bytes>> = vec![
            Ok(Bytes::from_static(b"ok")),
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "broken")),
        ];

        let result = SizedPeek::new(stream::iter(chunks), 1024).await;
        assert!(result.is_err());
    }
}
