use std::fmt;
use std::io::{self, Cursor};
use std::path::PathBuf;
use std::{borrow::Cow, collections::BTreeMap};

use async_compression::tokio::bufread::ZstdEncoder;
use bytes::Bytes;
use futures_util::StreamExt;
use objectstore_types::metadata::Metadata;
use reqwest::Body;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio_util::io::{ReaderStream, StreamReader};

pub use objectstore_types::metadata::{Compression, ExpirationPolicy};

use crate::response::ResponseExt as _;
use crate::{ClientStream, ObjectKey, Session};

mod automatic;
mod payload;

/// The response returned from the service after uploading an object.
#[derive(Debug, Deserialize)]
pub struct PutResponse {
    /// The key of the object, as stored.
    pub key: ObjectKey,
}

pub(crate) enum PutBody {
    Buffer(Bytes),
    Stream(ClientStream),
    File(File),
    Path(PathBuf),
}

impl PutBody {
    /// Measures remaining bytes without opening queued paths or consuming a stream.
    pub(crate) async fn remaining_size(&mut self) -> io::Result<Option<u64>> {
        match self {
            Self::Buffer(bytes) => Ok(Some(bytes.len() as u64)),
            Self::File(file) => {
                file.flush().await?;
                let metadata = file.metadata().await?;
                if !metadata.is_file() {
                    return Ok(None);
                }
                Ok(Some(
                    metadata.len().saturating_sub(file.stream_position().await?),
                ))
            }
            Self::Path(path) => {
                let metadata = tokio::fs::metadata(path).await?;
                Ok(metadata.is_file().then_some(metadata.len()))
            }
            Self::Stream(_) => Ok(None),
        }
    }
}

impl fmt::Debug for PutBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PutBody").finish_non_exhaustive()
    }
}

/// Declares how a payload relates to the compression recorded in its metadata.
///
/// Both modes record the same [`Compression`] on the object; they differ only in who performs
/// the compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompressionMode {
    /// The client compresses the payload with this algorithm before uploading it.
    Compress(Compression),
    /// The payload is already compressed with this algorithm and is uploaded verbatim.
    Precompressed(Compression),
}

impl CompressionMode {
    /// Returns the compression algorithm applied to the payload.
    pub fn compression(self) -> Compression {
        match self {
            Self::Compress(compression) | Self::Precompressed(compression) => compression,
        }
    }
}

impl Session {
    fn put_body(&self, body: PutBody) -> PutBuilder {
        let metadata = Metadata {
            expiration_policy: self.scope.usecase().expiration_policy(),
            ..Default::default()
        };

        PutBuilder {
            session: self.clone(),
            metadata,
            compression: self
                .scope
                .usecase()
                .compression()
                .map(CompressionMode::Compress),
            key: None,
            resumable_threshold_bytes: self.scope.usecase().resumable_threshold_bytes(),
            body,
        }
    }

    /// Creates or replaces an object using a [`Bytes`]-like payload.
    pub fn put(&self, body: impl Into<Bytes>) -> PutBuilder {
        self.put_body(PutBody::Buffer(body.into()))
    }

    /// Creates or replaces an object using a streaming payload.
    ///
    /// Streams use direct uploads, regardless of the resumable threshold.
    pub fn put_stream(&self, body: ClientStream) -> PutBuilder {
        self.put_body(PutBody::Stream(body))
    }

    /// Creates or replaces an object using an [`AsyncRead`] payload.
    ///
    /// Readers use direct uploads. Use [`Self::put_file`] for automatic resumable file uploads.
    pub fn put_read<R>(&self, body: R) -> PutBuilder
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        let stream = ReaderStream::new(body).boxed();
        self.put_body(PutBody::Stream(stream))
    }

    /// Creates or replaces an object using the contents of an opened file.
    ///
    /// The file descriptor is held open from the moment this method is called until the
    /// upload completes. When enqueueing many files via [`Session::many`], prefer
    /// [`put_path`](Session::put_path) instead: it defers opening the file until just before
    /// upload, keeping file descriptor usage within the active concurrency window and avoiding
    /// OS file descriptor limit (e.g., macOS's default `ulimit -n`) exhaustion.
    pub fn put_file(&self, file: File) -> PutBuilder {
        self.put_body(PutBody::File(file))
    }

    /// Creates or replaces an object using the contents of the file at `path`.
    ///
    /// Unlike [`put_file`](Session::put_file), this method defers opening the file until the
    /// request is actually sent. When enqueueing many file uploads via [`Session::many`], this
    /// ensures that file descriptors are opened only within the active concurrency window,
    /// preventing the process from exhausting the OS file descriptor limit (e.g., macOS's
    /// default `ulimit -n`).
    ///
    /// Prefer `put_path` over [`put_file`](Session::put_file) whenever you are lining up a
    /// large number of files for upload.
    pub fn put_path(&self, path: impl Into<PathBuf>) -> PutBuilder {
        self.put_body(PutBody::Path(path.into()))
    }
}

/// A [`put`](Session::put) request builder.
///
/// # Automatic resumable uploads
///
/// Buffers and regular files (including paths) use resumable uploads when their remaining
/// input size meets the configured threshold, by default 256 MiB before client compression.
/// Streams, generic readers, and files whose size cannot be determined use direct uploads.
/// Files must remain unchanged until the upload finishes. File uploads start at the current
/// position; path uploads open the file only when executed.
///
/// Eligible uploads reuse uncompressed or precompressed input. Client compression first spools
/// the complete compressed payload to a temporary file, using [`crate::ClientBuilder::spool_dir`]
/// when configured. If file creation fails, the client falls back to direct streaming. Later
/// preparation errors propagate. Unsupported or failed session creation also falls back to a
/// direct upload of the prepared bytes, without compressing them again.
///
/// Transient failures and writes making no progress are retried twice, with exponential delays
/// of 2 and 4 seconds plus up to 1 second of jitter. Recovery queries the server's persisted
/// offset before replaying data. The retry budget resets whenever that offset advances.
/// Terminal failures after session creation attempt cancellation and return
/// [`crate::Error::UploadFailed`] preserving the cause. Dropping the future releases local
/// resources; the remote session is left to expire.
///
/// Automatic uploads require no feature flag. The `resumable-upload-api` feature exposes the
/// manual protocol API. See [`Self::resumable_threshold_bytes`] to override eligibility.
#[derive(Debug)]
pub struct PutBuilder {
    pub(crate) session: Session,
    pub(crate) metadata: Metadata,
    pub(crate) compression: Option<CompressionMode>,
    pub(crate) key: Option<ObjectKey>,
    pub(crate) body: PutBody,
    pub(crate) resumable_threshold_bytes: Option<u64>,
}

impl PutBuilder {
    /// Sets the minimum remaining input size for automatic resumable uploads.
    ///
    /// Inherits [`crate::Usecase::resumable_threshold_bytes`] unless overridden. Pass [`None`]
    /// to disable or zero to include every supported payload. Eligible uploads queued through
    /// [`Session::many`] are sent individually rather than batched. See [`PutBuilder`] for
    /// preparation and recovery behavior.
    ///
    /// ```no_run
    /// # async fn example(session: objectstore_client::Session) -> objectstore_client::Result<()> {
    /// session.put("payload").resumable_threshold_bytes(None).send().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn resumable_threshold_bytes(mut self, threshold: impl Into<Option<u64>>) -> Self {
        self.resumable_threshold_bytes = threshold.into();
        self
    }

    /// Sets an explicit object key.
    ///
    /// If a key is specified, the object will be stored under that key. Otherwise, the Objectstore
    /// server will automatically assign a random key, which is then returned from this request.
    pub fn key(mut self, key: impl Into<ObjectKey>) -> Self {
        self.key = Some(key.into()).filter(|k| !k.is_empty());
        self
    }

    /// Sets an explicit compression algorithm to be used for this payload.
    ///
    /// The client compresses the payload and records the algorithm in the
    /// object's metadata. [`None`] should be used if no compression should be performed by the
    /// client, either because the payload is uncompressible (such as a media format), or if the
    /// compression should not be recorded for this object.
    ///
    /// If the payload is already compressed and the algorithm should still be recorded, use
    /// [`precompressed`](Self::precompressed) instead.
    ///
    /// By default, the compression algorithm set on this Session's Usecase is used (see
    /// [`with_compression`](crate::Usecase::with_compression)).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(session: objectstore_client::Session, media: Vec<u8>) {
    /// session.put(media)
    ///     .compress(None) // uncompressible payload
    ///     .send()
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn compress(mut self, compression: impl Into<Option<Compression>>) -> Self {
        self.compression = compression.into().map(CompressionMode::Compress);
        self
    }

    /// Deprecated in favor of [`compress`](Self::compress).
    #[deprecated(since = "0.3.0", note = "renamed to `compress`")]
    pub fn compression(self, compression: impl Into<Option<Compression>>) -> Self {
        self.compress(compression)
    }

    /// Declares that the payload is already compressed with the given algorithm.
    ///
    /// The payload is uploaded verbatim, and the algorithm is recorded in the object's metadata
    /// so that downloads decompress it transparently. Use this to hand pre-compressed data to
    /// the client without paying for another compression pass.
    ///
    /// This overrides the compression algorithm set on this Session's Usecase. To have the
    /// client perform the compression instead, use [`compress`](Self::compress).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use objectstore_client::Compression;
    /// # async fn example(session: objectstore_client::Session, zstd_data: Vec<u8>) {
    /// session.put(zstd_data)
    ///     .precompressed(Compression::Zstd)
    ///     .send()
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn precompressed(mut self, compression: Compression) -> Self {
        self.compression = Some(CompressionMode::Precompressed(compression));
        self
    }

    /// Sets the expiration policy of the object to be uploaded.
    ///
    /// By default, the expiration policy set on this Session's Usecase is used.
    pub fn expiration_policy(mut self, expiration_policy: ExpirationPolicy) -> Self {
        self.metadata.expiration_policy = expiration_policy;
        self
    }

    /// Sets the content type of the object to be uploaded.
    ///
    /// You can use the utility function [`crate::utils::guess_mime_type`] to attempt to guess a
    /// `content_type` based on magic bytes.
    pub fn content_type(mut self, content_type: impl Into<Cow<'static, str>>) -> Self {
        self.metadata.content_type = content_type.into();
        self
    }

    /// Sets the origin of the object, typically the IP address of the original source.
    ///
    /// This is an optional but encouraged field that tracks where the payload was
    /// originally obtained from. For example, the IP address of the Sentry SDK or CLI
    /// that uploaded the data.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(session: objectstore_client::Session) {
    /// session.put("data")
    ///     .origin("203.0.113.42")
    ///     .send()
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn origin(mut self, origin: impl Into<String>) -> Self {
        self.metadata.origin = Some(origin.into());
        self
    }

    /// Sets the filename of the object.
    ///
    /// When present, the server will include a `Content-Disposition: attachment; filename="<filename>"`
    /// header in GET responses, prompting browsers and download tools to save the file under
    /// this name.
    pub fn filename(mut self, filename: impl Into<String>) -> Self {
        self.metadata.filename = Some(filename.into());
        self
    }

    /// This sets the custom metadata to the provided map.
    ///
    /// It will clear any previously set metadata.
    pub fn set_metadata(mut self, metadata: impl Into<BTreeMap<String, String>>) -> Self {
        self.metadata.custom = metadata.into();
        self
    }

    /// Appends they `key`/`value` to the custom metadata of this object.
    pub fn append_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.custom.insert(key.into(), value.into());
        self
    }
}

/// Turns the body into a request body, compressing it if the mode asks for it.
///
/// Payloads declared as [`CompressionMode::Precompressed`] are forwarded verbatim.
pub(crate) async fn encode_body(body: PutBody, mode: Option<CompressionMode>) -> io::Result<Body> {
    let compression = match mode {
        Some(CompressionMode::Compress(compression)) => Some(compression),
        // The payload already carries the encoding, so nothing is left to do here.
        Some(CompressionMode::Precompressed(_)) | None => None,
    };

    Ok(match (compression, body) {
        (Some(Compression::Zstd), PutBody::Buffer(bytes)) => {
            let cursor = Cursor::new(bytes);
            let encoder = ZstdEncoder::new(cursor);
            let stream = ReaderStream::new(encoder);
            Body::wrap_stream(stream)
        }
        (Some(Compression::Zstd), PutBody::Stream(stream)) => {
            let stream = StreamReader::new(stream);
            let encoder = ZstdEncoder::new(stream);
            let stream = ReaderStream::new(encoder);
            Body::wrap_stream(stream)
        }
        (Some(Compression::Zstd), PutBody::File(file)) => {
            let reader = BufReader::new(file);
            let encoder = ZstdEncoder::new(reader);
            let stream = ReaderStream::new(encoder);
            Body::wrap_stream(stream)
        }
        (Some(Compression::Zstd), PutBody::Path(file)) => {
            let file = File::open(file).await?;
            let reader = BufReader::new(file);
            let encoder = ZstdEncoder::new(reader);
            let stream = ReaderStream::new(encoder);
            Body::wrap_stream(stream)
        }
        (None, PutBody::Buffer(bytes)) => bytes.into(),
        (None, PutBody::Stream(stream)) => Body::wrap_stream(stream),
        (None, PutBody::File(file)) => {
            let stream = ReaderStream::new(file);
            Body::wrap_stream(stream)
        }
        (None, PutBody::Path(path)) => {
            let stream = ReaderStream::new(File::open(path).await?);
            Body::wrap_stream(stream)
        }
    })
}

// TODO: instead of a separate `send` method, it would be nice to just implement `IntoFuture`.
// However, `IntoFuture` needs to define the resulting future as an associated type,
// and "impl trait in associated type position" is not yet stable :-(
impl PutBuilder {
    /// Sends the built put request to the upstream service.
    pub async fn send(mut self) -> crate::Result<PutResponse> {
        let method = match self.key {
            Some(_) => reqwest::Method::PUT,
            None => reqwest::Method::POST,
        };

        self.metadata.compression = self.compression.map(CompressionMode::compression);
        let headers = self.metadata.to_headers("")?;
        let prepared = payload::prepare(
            self.body,
            self.compression,
            self.resumable_threshold_bytes,
            self.session.client.spool_dir.clone(),
        )
        .await?;
        let session = &self.session;
        let key = self.key.as_deref().unwrap_or_default();
        let send_direct = |body: Body, content_length: Option<u64>| async move {
            // Prepare authentication only when ready to send: spooling can take long enough for
            // a token minted before preparation to expire.
            let mut builder = session.request(method, key)?.headers(headers);
            if let Some(length) = content_length {
                builder = builder.header(reqwest::header::CONTENT_LENGTH, length);
            }
            let response = builder.body(body).send().await?;
            Ok(response.error_for_status_and_drain().await?.json().await?)
        };
        match prepared {
            payload::PreparedBody::Direct(body) => {
                send_direct(encode_body(body, self.compression).await?, None).await
            }
            payload::PreparedBody::Replay(mut body) => {
                let length = body.len();
                let create = crate::resumable::CreateResumableUploadBuilder {
                    session: self.session.clone(),
                    total_length: length,
                    key: self.key.clone(),
                    metadata: self.metadata,
                };
                if let Ok(Some(upload)) = create.send().await {
                    return automatic::upload(body, upload).await;
                }
                body.with_stream(0, |stream| async {
                    send_direct(Body::wrap_stream(stream), Some(length)).await
                })
                .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::stream;
    use http_body_util::BodyExt as _;

    use super::*;

    fn zstd_compress(data: &[u8]) -> Vec<u8> {
        zstd::encode_all(Cursor::new(data), 0).expect("zstd encoding to succeed")
    }

    fn stream_body(chunks: Vec<&'static [u8]>) -> PutBody {
        let chunks = chunks.into_iter().map(|c| Ok(Bytes::from_static(c)));
        PutBody::Stream(stream::iter(chunks).boxed())
    }

    async fn collect(body: Body) -> Vec<u8> {
        body.collect()
            .await
            .expect("body to be readable")
            .to_bytes()
            .to_vec()
    }

    #[tokio::test]
    async fn compress_buffer_compresses() {
        let body = PutBody::Buffer(Bytes::from_static(b"hello world"));
        let mode = Some(CompressionMode::Compress(Compression::Zstd));

        let encoded = collect(encode_body(body, mode).await.unwrap()).await;
        assert_eq!(encoded, zstd_compress(b"hello world"));
    }

    #[tokio::test]
    async fn compress_stream_compresses() {
        let body = stream_body(vec![b"hello ", b"world"]);
        let mode = Some(CompressionMode::Compress(Compression::Zstd));

        let encoded = collect(encode_body(body, mode).await.unwrap()).await;
        assert_eq!(
            zstd::decode_all(Cursor::new(encoded)).unwrap(),
            b"hello world"
        );
    }

    #[tokio::test]
    async fn precompressed_buffer_is_forwarded_verbatim() {
        let compressed = zstd_compress(b"hello world");
        let body = PutBody::Buffer(Bytes::from(compressed.clone()));
        let mode = Some(CompressionMode::Precompressed(Compression::Zstd));

        let encoded = collect(encode_body(body, mode).await.unwrap()).await;
        assert_eq!(encoded, compressed);
    }

    #[tokio::test]
    async fn precompressed_stream_is_forwarded_verbatim() {
        let body = stream_body(vec![b"\x28\xb5\x2f\xfd", b"trailing"]);
        let mode = Some(CompressionMode::Precompressed(Compression::Zstd));

        let encoded = collect(encode_body(body, mode).await.unwrap()).await;
        assert_eq!(encoded, b"\x28\xb5\x2f\xfdtrailing");
    }

    #[tokio::test]
    async fn without_compression_is_forwarded_verbatim() {
        let body = PutBody::Buffer(Bytes::from_static(b"hello world"));

        let encoded = collect(encode_body(body, None).await.unwrap()).await;
        assert_eq!(encoded, b"hello world");
    }
}
