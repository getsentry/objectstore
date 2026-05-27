use std::{fmt, io};

use async_compression::tokio::bufread::ZstdDecoder;
use bytes::BytesMut;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::{Compression, Metadata};
use reqwest::StatusCode;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{ClientStream, ObjectKey, Session};

/// The result from a successful [`get()`](Session::get) call.
///
/// This carries the response as a stream, plus the compression algorithm of the data.
pub struct GetResponse {
    /// The metadata attached to this object, including the compression algorithm used for the payload.
    pub metadata: Metadata,
    /// The response stream.
    pub stream: ClientStream,
}

impl GetResponse {
    /// Loads the object payload fully into memory.
    pub async fn payload(self) -> crate::Result<bytes::Bytes> {
        let bytes: BytesMut = self.stream.try_collect().await?;
        Ok(bytes.freeze())
    }

    /// Loads the object payload fully into memory and interprets it as UTF-8 text.
    pub async fn text(self) -> crate::Result<String> {
        let bytes = self.payload().await?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

impl fmt::Debug for GetResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetResponse")
            .field("metadata", &self.metadata)
            .field("stream", &format_args!("[Stream]"))
            .finish()
    }
}

impl Session {
    /// Retrieves the object with the given `key`.
    pub fn get(&self, key: &str) -> GetBuilder {
        GetBuilder {
            session: self.clone(),
            key: key.to_owned(),
            decompress: true,
            accept_encoding: vec![],
        }
    }
}

/// A [`get`](Session::get) request builder.
#[derive(Debug)]
pub struct GetBuilder {
    pub(crate) session: Session,
    pub(crate) key: ObjectKey,
    pub(crate) decompress: bool,
    pub(crate) accept_encoding: Vec<Compression>,
}

impl GetBuilder {
    /// Indicates whether the request should automatically handle decompression of known algorithms,
    /// or rather return the payload as it is stored, along with the compression algorithm it is stored in.
    ///
    /// By default, automatic decompression is enabled.
    pub fn decompress(mut self, decompress: bool) -> Self {
        self.decompress = decompress;
        self
    }

    /// Specifies compression encodings the caller can handle natively.
    ///
    /// When the stored object's compression matches one of these, the payload
    /// is returned still compressed and `metadata.compression` is preserved.
    /// An empty list (the default) means the client does not accept any
    /// compressed encoding, so automatic decompression applies as usual.
    pub fn accept_encoding(mut self, encodings: impl IntoIterator<Item = Compression>) -> Self {
        self.accept_encoding = encodings.into_iter().collect();
        self
    }

    /// Sends the get request.
    pub async fn send(self) -> crate::Result<Option<GetResponse>> {
        let response = self
            .session
            .request(reqwest::Method::GET, &self.key)?
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response.error_for_status()?;

        let mut metadata = Metadata::from_headers(response.headers(), "")?;

        let stream = response.bytes_stream().map_err(io::Error::other).boxed();
        let stream = maybe_decompress(
            stream,
            &mut metadata,
            self.decompress,
            &self.accept_encoding,
        );

        Ok(Some(GetResponse { metadata, stream }))
    }
}

/// Wraps a stream in a zstd decompression layer.
///
/// Decompresses if the metadata indicates zstd compression, `decompress` is `true`,
/// and the stored encoding is not listed in `accept_encoding`. When the stored encoding
/// is in `accept_encoding`, the payload is returned compressed and `metadata.compression`
/// is preserved. Clears `metadata.compression` when decompression is applied.
pub(crate) fn maybe_decompress(
    stream: ClientStream,
    metadata: &mut Metadata,
    decompress: bool,
    accept_encoding: &[Compression],
) -> ClientStream {
    let encoding_accepted = metadata
        .compression
        .is_some_and(|c| accept_encoding.contains(&c));
    match (metadata.compression, decompress && !encoding_accepted) {
        (Some(Compression::Zstd), true) => {
            metadata.compression = None;
            let mut decoder = ZstdDecoder::new(StreamReader::new(stream));
            // Multipart uploads with compression, when each part is compressed individually,
            // will consist of multiple concatenated zstd frames.
            // This allows the client to handle automatic decompression for these objects transparently.
            decoder.multiple_members(true);
            ReaderStream::new(decoder).boxed()
        }
        _ => stream,
    }
}

#[cfg(test)]
mod tests {
    use futures_util::{StreamExt as _, TryStreamExt as _};
    use objectstore_types::metadata::{Compression, Metadata};

    use super::maybe_decompress;
    use crate::ClientStream;

    fn compressed_zstd_stream(data: &[u8]) -> ClientStream {
        let mut encoder = zstd::Encoder::new(vec![], 0).unwrap();
        std::io::copy(&mut std::io::Cursor::new(data), &mut encoder).unwrap();
        let compressed = encoder.finish().unwrap();
        futures_util::stream::once(async move {
            Ok::<_, std::io::Error>(bytes::Bytes::from(compressed))
        })
        .boxed()
    }

    fn raw_stream(data: &[u8]) -> ClientStream {
        let bytes = bytes::Bytes::copy_from_slice(data);
        futures_util::stream::once(async move { Ok::<_, std::io::Error>(bytes) }).boxed()
    }

    async fn collect(stream: ClientStream) -> Vec<u8> {
        let chunks: bytes::BytesMut = stream.try_collect().await.unwrap();
        chunks.to_vec()
    }

    fn zstd_metadata() -> Metadata {
        Metadata {
            compression: Some(Compression::Zstd),
            ..Default::default()
        }
    }

    fn no_compression_metadata() -> Metadata {
        Metadata::default()
    }

    #[tokio::test]
    async fn empty_accept_decompress_true_decompresses() {
        let payload = b"hello world";
        let stream = compressed_zstd_stream(payload);
        let mut metadata = zstd_metadata();

        let out = maybe_decompress(stream, &mut metadata, true, &[]);
        assert_eq!(collect(out).await, payload);
        assert_eq!(metadata.compression, None);
    }

    #[tokio::test]
    async fn empty_accept_decompress_false_returns_compressed() {
        let payload = b"hello world";
        let compressed_bytes = collect(compressed_zstd_stream(payload)).await;
        let stream = compressed_zstd_stream(payload);

        let mut metadata = zstd_metadata();
        let out = maybe_decompress(stream, &mut metadata, false, &[]);
        assert_eq!(collect(out).await, compressed_bytes);
        assert_eq!(metadata.compression, Some(Compression::Zstd));
    }

    #[tokio::test]
    async fn zstd_accept_decompress_true_skips_decompression() {
        let payload = b"hello world";
        let compressed_bytes = collect(compressed_zstd_stream(payload)).await;
        let stream = compressed_zstd_stream(payload);

        let mut metadata = zstd_metadata();
        let out = maybe_decompress(stream, &mut metadata, true, &[Compression::Zstd]);
        assert_eq!(collect(out).await, compressed_bytes);
        assert_eq!(metadata.compression, Some(Compression::Zstd));
    }

    #[tokio::test]
    async fn zstd_accept_decompress_false_returns_compressed() {
        let payload = b"hello world";
        let compressed_bytes = collect(compressed_zstd_stream(payload)).await;
        let stream = compressed_zstd_stream(payload);

        let mut metadata = zstd_metadata();
        let out = maybe_decompress(stream, &mut metadata, false, &[Compression::Zstd]);
        assert_eq!(collect(out).await, compressed_bytes);
        assert_eq!(metadata.compression, Some(Compression::Zstd));
    }

    #[tokio::test]
    async fn no_compression_returns_raw_regardless_of_accept() {
        let payload = b"hello world";
        let stream = raw_stream(payload);

        let mut metadata = no_compression_metadata();
        let out = maybe_decompress(stream, &mut metadata, true, &[Compression::Zstd]);
        assert_eq!(collect(out).await, payload);
        assert_eq!(metadata.compression, None);
    }

    #[tokio::test]
    async fn zstd_concatenated_frames_decompress() {
        let payload1 = b"hello ";
        let payload2 = b"world";
        let compressed1 = collect(compressed_zstd_stream(payload1)).await;
        let compressed2 = collect(compressed_zstd_stream(payload2)).await;
        let stream = futures_util::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from(compressed1)),
            Ok::<_, std::io::Error>(bytes::Bytes::from(compressed2)),
        ])
        .boxed();

        let mut metadata = zstd_metadata();
        let out = maybe_decompress(stream, &mut metadata, true, &[]);
        assert_eq!(collect(out).await, b"hello world");
        assert_eq!(metadata.compression, None);
    }
}
