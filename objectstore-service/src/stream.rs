//! Payload stream type and test utilities.

use futures_util::stream::BoxStream;

/// Type alias for data streams used in service APIs.
pub type PayloadStream = BoxStream<'static, std::io::Result<bytes::Bytes>>;

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
