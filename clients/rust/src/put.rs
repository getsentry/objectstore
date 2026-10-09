use std::fmt;
use std::io::{self, Cursor};
use std::path::PathBuf;
use std::time::Duration;
use std::{borrow::Cow, collections::BTreeMap};

use async_compression::tokio::bufread::ZstdEncoder;
use bytes::Bytes;
use futures_util::StreamExt;
use objectstore_types::metadata::Metadata;
use objectstore_types::resumable::UploadProgress;
use rand::RngExt as _;
use reqwest::Body;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeekExt as _, BufReader, SeekFrom};
use tokio_util::io::{ReaderStream, StreamReader};

pub use objectstore_types::metadata::{Compression, ExpirationPolicy};

use crate::response::ResponseExt as _;
use crate::resumable::{ResumableUpload, create_resumable_upload};
use crate::{ClientStream, Error, ObjectKey, Session};

const RESUMABLE_UPLOAD_THRESHOLD: u64 = 32 * 1024 * 1024;
const MAX_RETRIES: usize = 2;
const RETRY_BASE_DELAY: Duration = Duration::from_millis(25);
const RETRY_MAX_DELAY: Duration = Duration::from_millis(200);

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
            body,
            resumable: true,
        }
    }

    /// Creates or replaces an object using a [`Bytes`]-like payload.
    pub fn put(&self, body: impl Into<Bytes>) -> PutBuilder {
        self.put_body(PutBody::Buffer(body.into()))
    }

    /// Creates or replaces an object using a streaming payload.
    pub fn put_stream(&self, body: ClientStream) -> PutBuilder {
        self.put_body(PutBody::Stream(body))
    }

    /// Creates or replaces an object using an [`AsyncRead`] payload.
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
#[derive(Debug)]
pub struct PutBuilder {
    pub(crate) session: Session,
    pub(crate) metadata: Metadata,
    pub(crate) compression: Option<CompressionMode>,
    pub(crate) key: Option<ObjectKey>,
    pub(crate) body: PutBody,
    pub(crate) resumable: bool,
}

impl PutBuilder {
    /// Controls whether this put is eligible for an automatic resumable upload.
    ///
    /// This defaults to `true`. Buffered, file, and path payloads whose source size is at least
    /// 32 MiB may use the resumable protocol. This option is eligibility, not a guarantee: smaller
    /// payloads and streaming or reader payloads use the direct put path, and a server that does
    /// not implement resumable uploads causes the client to fall back to a direct put.
    pub fn resumable(mut self, resumable: bool) -> Self {
        self.resumable = resumable;
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
    /// The client compresses the payload while uploading it and records the algorithm in the
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

#[derive(Debug)]
enum StablePayload {
    Buffer(Bytes),
    File { file: File, length: u64 },
}

impl StablePayload {
    fn len(&self) -> u64 {
        match self {
            Self::Buffer(bytes) => bytes.len() as u64,
            Self::File { length, .. } => *length,
        }
    }

    async fn body_from(&self, offset: u64) -> io::Result<Body> {
        let remaining = self.len().checked_sub(offset).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "resumable upload offset exceeds payload length",
            )
        })?;
        match self {
            Self::Buffer(bytes) => {
                let offset = usize::try_from(offset).map_err(io::Error::other)?;
                Ok(bytes.slice(offset..).into())
            }
            Self::File { file, .. } => {
                let mut file = file.try_clone().await?;
                file.seek(SeekFrom::Start(offset)).await?;
                let stream = ReaderStream::new(file.take(remaining));
                Ok(Body::wrap_stream(stream))
            }
        }
    }

    async fn into_body(self) -> io::Result<Body> {
        match self {
            Self::Buffer(bytes) => Ok(bytes.into()),
            Self::File { mut file, length } => {
                file.seek(SeekFrom::Start(0)).await?;
                Ok(Body::wrap_stream(ReaderStream::new(file.take(length))))
            }
        }
    }
}

async fn source_size(body: &mut PutBody) -> io::Result<Option<u64>> {
    match body {
        PutBody::Buffer(bytes) => Ok(Some(bytes.len() as u64)),
        PutBody::File(file) => {
            let Ok(position) = file.stream_position().await else {
                return Ok(None);
            };
            let Ok(metadata) = file.metadata().await else {
                return Ok(None);
            };
            Ok(Some(metadata.len().saturating_sub(position)))
        }
        PutBody::Path(path) => Ok(Some(tokio::fs::metadata(path).await?.len())),
        PutBody::Stream(_) => Ok(None),
    }
}

async fn prepare_stable_payload(
    body: PutBody,
    compression: Option<CompressionMode>,
) -> io::Result<StablePayload> {
    let should_compress = matches!(
        compression,
        Some(CompressionMode::Compress(Compression::Zstd))
    );
    match body {
        PutBody::Buffer(bytes) if should_compress => {
            let mut encoder = ZstdEncoder::new(Cursor::new(bytes));
            let mut compressed = Vec::new();
            encoder.read_to_end(&mut compressed).await?;
            Ok(StablePayload::Buffer(compressed.into()))
        }
        PutBody::Buffer(bytes) => Ok(StablePayload::Buffer(bytes)),
        PutBody::File(file) => stage_file(file, should_compress).await,
        PutBody::Path(path) => stage_file(File::open(path).await?, should_compress).await,
        PutBody::Stream(_) => unreachable!("streams are never eligible for resumable uploads"),
    }
}

async fn stage_file(file: File, compress: bool) -> io::Result<StablePayload> {
    let mut staged = File::from_std(tempfile::tempfile()?);
    if compress {
        let mut encoder = ZstdEncoder::new(BufReader::new(file));
        tokio::io::copy(&mut encoder, &mut staged).await?;
    } else {
        tokio::io::copy(&mut BufReader::new(file), &mut staged).await?;
    }
    let length = staged.stream_position().await?;
    staged.seek(SeekFrom::Start(0)).await?;
    Ok(StablePayload::File {
        file: staged,
        length,
    })
}

fn retryable(error: &Error) -> bool {
    let Error::Reqwest(error) = error else {
        return false;
    };
    if let Some(status) = error.status() {
        return matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504);
    }
    error.is_connect() || error.is_timeout() || error.is_request() || error.is_body()
}

async fn retry_delay(attempt: usize) {
    let factor = 1_u32.checked_shl(attempt as u32).unwrap_or(u32::MAX);
    let delay = RETRY_BASE_DELAY.saturating_mul(factor).min(RETRY_MAX_DELAY);
    let jitter = rand::rng().random_range(0..=delay.as_millis() as u64 / 2);
    tokio::time::sleep(delay + Duration::from_millis(jitter)).await;
}

async fn create_with_retries(
    session: &Session,
    key: &Option<ObjectKey>,
    metadata: &Metadata,
    length: u64,
) -> crate::Result<Option<ResumableUpload>> {
    for attempt in 0..=MAX_RETRIES {
        match create_resumable_upload(session.clone(), length, key.clone(), metadata.clone()).await
        {
            Err(error) if retryable(&error) && attempt < MAX_RETRIES => {
                retry_delay(attempt).await;
            }
            result => return result,
        }
    }
    unreachable!("retry loop always returns on its final attempt")
}

async fn progress_with_retries(upload: &ResumableUpload) -> crate::Result<UploadProgress> {
    for attempt in 0..=MAX_RETRIES {
        match upload.progress().send_automatic().await {
            Err(error) if retryable(&error) && attempt < MAX_RETRIES => {
                retry_delay(attempt).await;
            }
            result => return result,
        }
    }
    unreachable!("retry loop always returns on its final attempt")
}

fn next_offset(progress: UploadProgress, previous: u64, length: u64) -> crate::Result<Option<u64>> {
    match progress {
        UploadProgress::Complete => Ok(None),
        UploadProgress::Incomplete { offset } if offset >= length => {
            Err(Error::MalformedResponse(format!(
                "incomplete resumable upload offset {offset} is invalid for payload length {length}"
            )))
        }
        UploadProgress::Incomplete { offset } if offset <= previous => Err(
            Error::MalformedResponse(format!("resumable upload stalled at offset {offset}")),
        ),
        UploadProgress::Incomplete { offset } => Ok(Some(offset)),
    }
}

fn upload_failed(error: Error) -> Error {
    Error::UploadFailed {
        source: Box::new(error),
    }
}

async fn send_resumable(
    upload: ResumableUpload,
    payload: &StablePayload,
) -> crate::Result<PutResponse> {
    send_resumable_inner(upload, payload)
        .await
        .map_err(upload_failed)
}

async fn send_resumable_inner(
    upload: ResumableUpload,
    payload: &StablePayload,
) -> crate::Result<PutResponse> {
    let length = payload.len();
    let mut offset = 0;
    let mut recoveries = 0;

    loop {
        let body = payload.body_from(offset).await?;
        match upload
            .put_body(offset, length - offset, body)
            .send_automatic()
            .await
        {
            Ok(progress) => match next_offset(progress, offset, length)? {
                None => {
                    return Ok(PutResponse {
                        key: upload.key().to_owned(),
                    });
                }
                Some(next) => offset = next,
            },
            Err(error) if retryable(&error) && recoveries < MAX_RETRIES => {
                retry_delay(recoveries).await;
                recoveries += 1;
                match progress_with_retries(&upload).await? {
                    UploadProgress::Complete => {
                        return Ok(PutResponse {
                            key: upload.key().to_owned(),
                        });
                    }
                    UploadProgress::Incomplete {
                        offset: authoritative,
                    } => {
                        if authoritative < offset || authoritative >= length {
                            return Err(Error::MalformedResponse(format!(
                                "incomplete resumable upload offset {authoritative} is invalid after offset {offset} for payload length {length}"
                            )));
                        }
                        offset = authoritative;
                    }
                }
            }
            result => return result.map(|_| unreachable!("successful progress handled above")),
        }
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
        let eligible = if self.resumable {
            source_size(&mut self.body)
                .await?
                .is_some_and(|size| size >= RESUMABLE_UPLOAD_THRESHOLD)
        } else {
            false
        };

        if eligible {
            self.metadata.compression = self.compression.map(CompressionMode::compression);
            let payload = prepare_stable_payload(self.body, self.compression).await?;
            if let Some(upload) =
                create_with_retries(&self.session, &self.key, &self.metadata, payload.len())
                    .await
                    .map_err(upload_failed)?
            {
                return send_resumable(upload, &payload).await;
            }

            return send_direct(
                &self.session,
                self.key.as_deref(),
                &self.metadata,
                payload.into_body().await?,
            )
            .await;
        }

        let method = match self.key {
            Some(_) => reqwest::Method::PUT,
            None => reqwest::Method::POST,
        };

        let mut builder = self
            .session
            .request(method, self.key.as_deref().unwrap_or_default())?;

        self.metadata.compression = self.compression.map(CompressionMode::compression);
        let body = encode_body(self.body, self.compression).await?;

        builder = builder.headers(self.metadata.to_headers("")?);

        let response = builder.body(body).send().await?;
        Ok(response.error_for_status_and_drain().await?.json().await?)
    }
}

async fn send_direct(
    session: &Session,
    key: Option<&str>,
    metadata: &Metadata,
    body: Body,
) -> crate::Result<PutResponse> {
    let method = if key.is_some() {
        reqwest::Method::PUT
    } else {
        reqwest::Method::POST
    };
    let response = session
        .request(method, key.unwrap_or_default())?
        .headers(metadata.to_headers("")?)
        .body(body)
        .send()
        .await?;
    Ok(response.error_for_status_and_drain().await?.json().await?)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::{Body as AxumBody, to_bytes};
    use axum::extract::State;
    use axum::http::{Request, Response, StatusCode};
    use axum::routing::any;
    use futures_util::stream;
    use http_body_util::BodyExt as _;

    use super::*;
    use crate::{Client, Usecase};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RequestKind {
        Create,
        Chunk,
        Progress,
        Direct,
    }

    #[derive(Clone, Copy, Debug)]
    struct Reply {
        kind: RequestKind,
        status: StatusCode,
        offset: Option<u64>,
    }

    impl Reply {
        fn new(kind: RequestKind, status: StatusCode) -> Self {
            Self {
                kind,
                status,
                offset: None,
            }
        }

        fn offset(kind: RequestKind, status: StatusCode, offset: u64) -> Self {
            Self {
                kind,
                status,
                offset: Some(offset),
            }
        }
    }

    #[derive(Debug)]
    struct RecordedRequest {
        kind: RequestKind,
        offset: Option<String>,
        upload_length: Option<String>,
        headers: reqwest::header::HeaderMap,
        body: Vec<u8>,
    }

    #[derive(Debug)]
    struct MockState {
        replies: Mutex<VecDeque<Reply>>,
        requests: Mutex<Vec<RecordedRequest>>,
    }

    #[derive(Debug)]
    struct MockServer {
        url: String,
        state: Arc<MockState>,
        handle: tokio::task::JoinHandle<()>,
    }

    impl MockServer {
        async fn start(replies: impl IntoIterator<Item = Reply>) -> Self {
            let state = Arc::new(MockState {
                replies: Mutex::new(replies.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
            });
            let app = Router::new()
                .route("/{*path}", any(mock_handler))
                .with_state(state.clone());
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let address = listener.local_addr().unwrap();
            let handle = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                url: format!("http://{address}/"),
                state,
                handle,
            }
        }

        fn session(&self) -> Session {
            let client = Client::new(&self.url).unwrap();
            client
                .session(Usecase::new("test").for_organization(1))
                .unwrap()
        }

        fn take_requests(&self) -> Vec<RecordedRequest> {
            let requests = std::mem::take(&mut *self.state.requests.lock().unwrap());
            assert!(
                self.state.replies.lock().unwrap().is_empty(),
                "not all scripted replies were consumed"
            );
            requests
        }
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    async fn mock_handler(
        State(state): State<Arc<MockState>>,
        request: Request<AxumBody>,
    ) -> Response<AxumBody> {
        let query = request.uri().query().unwrap_or_default();
        let offset = request
            .headers()
            .get(objectstore_types::resumable::HEADER_UPLOAD_OFFSET)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let kind = if query.contains("upload_type=resumable") {
            RequestKind::Create
        } else if offset.as_deref() == Some("*") {
            RequestKind::Progress
        } else if query.contains("session=") {
            RequestKind::Chunk
        } else {
            RequestKind::Direct
        };
        let upload_length = request
            .headers()
            .get(objectstore_types::resumable::HEADER_UPLOAD_LENGTH)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let headers = request.headers().clone();
        let body = to_bytes(request.into_body(), 64 * 1024 * 1024)
            .await
            .unwrap()
            .to_vec();
        state.requests.lock().unwrap().push(RecordedRequest {
            kind,
            offset,
            upload_length,
            headers,
            body,
        });

        let reply = state
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("received more requests than scripted");
        assert_eq!(kind, reply.kind);

        let mut response = Response::builder().status(reply.status);
        if let Some(offset) = reply.offset {
            response = response.header(objectstore_types::resumable::HEADER_UPLOAD_OFFSET, offset);
        }
        let body = match (kind, reply.status) {
            (RequestKind::Create, StatusCode::OK) => {
                r#"{"key":"key","session":"dG9rZW4","granularity":1}"#
            }
            (RequestKind::Chunk | RequestKind::Progress, StatusCode::CREATED) => r#"{"key":"key"}"#,
            (RequestKind::Direct, status) if status.is_success() => r#"{"key":"key"}"#,
            _ => "",
        };
        response.body(AxumBody::from(body)).unwrap()
    }

    async fn resumable_upload(
        server: &MockServer,
        payload: impl Into<Bytes>,
    ) -> crate::Result<PutResponse> {
        let payload = StablePayload::Buffer(payload.into());
        let upload = create_resumable_upload(
            server.session(),
            payload.len(),
            Some("key".to_owned()),
            Metadata::default(),
        )
        .await?
        .unwrap();
        send_resumable(upload, &payload).await
    }

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

    #[tokio::test]
    async fn automatic_resumable_boundary_and_opt_out() {
        let server = MockServer::start([
            Reply::new(RequestKind::Direct, StatusCode::OK),
            Reply::new(RequestKind::Create, StatusCode::NOT_IMPLEMENTED),
            Reply::new(RequestKind::Direct, StatusCode::OK),
            Reply::new(RequestKind::Direct, StatusCode::OK),
        ])
        .await;
        let session = server.session();

        session
            .put(vec![0; RESUMABLE_UPLOAD_THRESHOLD as usize - 1])
            .compress(None)
            .key("key")
            .send()
            .await
            .unwrap();
        session
            .put(vec![0; RESUMABLE_UPLOAD_THRESHOLD as usize])
            .compress(None)
            .key("key")
            .send()
            .await
            .unwrap();
        session
            .put(vec![0; RESUMABLE_UPLOAD_THRESHOLD as usize])
            .compress(None)
            .resumable(false)
            .key("key")
            .send()
            .await
            .unwrap();

        let requests = server.take_requests();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.kind)
                .collect::<Vec<_>>(),
            [
                RequestKind::Direct,
                RequestKind::Create,
                RequestKind::Direct,
                RequestKind::Direct,
            ]
        );
    }

    #[tokio::test]
    async fn automatic_resumable_wraps_creation_failures() {
        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::SERVICE_UNAVAILABLE),
            Reply::new(RequestKind::Create, StatusCode::SERVICE_UNAVAILABLE),
            Reply::new(RequestKind::Create, StatusCode::SERVICE_UNAVAILABLE),
        ])
        .await;

        let error = server
            .session()
            .put(vec![0; RESUMABLE_UPLOAD_THRESHOLD as usize])
            .compress(None)
            .key("key")
            .send()
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "upload failed");
        let Error::UploadFailed { source } = error else {
            panic!("expected UploadFailed");
        };
        assert!(matches!(
            source.downcast_ref::<Error>(),
            Some(Error::Reqwest(error)) if error.status() == Some(StatusCode::SERVICE_UNAVAILABLE)
        ));
        assert_eq!(server.take_requests().len(), 3);
    }

    #[tokio::test]
    async fn automatic_resumable_preserves_compression_and_metadata() {
        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::new(RequestKind::Chunk, StatusCode::CREATED),
        ])
        .await;

        server
            .session()
            .put(vec![0; RESUMABLE_UPLOAD_THRESHOLD as usize])
            .key("key")
            .content_type("application/test")
            .origin("test-origin")
            .filename("payload.bin")
            .append_metadata("source", "test")
            .send()
            .await
            .unwrap();

        let requests = server.take_requests();
        let create = &requests[0];
        let chunk = &requests[1];
        assert_eq!(
            create
                .headers
                .get(reqwest::header::CONTENT_ENCODING)
                .unwrap(),
            "zstd"
        );
        assert_eq!(
            create.headers.get(reqwest::header::CONTENT_TYPE).unwrap(),
            "application/test"
        );
        assert_eq!(create.headers.get("x-sn-origin").unwrap(), "test-origin");
        assert_eq!(create.headers.get("x-sn-filename").unwrap(), "payload.bin");
        assert_eq!(create.headers.get("x-snme-source").unwrap(), "test");
        assert_eq!(
            create.upload_length.as_deref(),
            Some(chunk.body.len().to_string().as_str())
        );
        let decoded = zstd::decode_all(Cursor::new(&chunk.body)).unwrap();
        assert_eq!(decoded.len(), RESUMABLE_UPLOAD_THRESHOLD as usize);
        assert!(decoded.iter().all(|byte| *byte == 0));
    }

    #[tokio::test]
    async fn automatic_resumable_stages_files_and_paths() {
        use std::io::{Seek as _, SeekFrom as StdSeekFrom, Write as _};

        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::new(RequestKind::Chunk, StatusCode::CREATED),
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::new(RequestKind::Chunk, StatusCode::CREATED),
        ])
        .await;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("large");
        let mut std_file = std::fs::File::options()
            .create(true)
            .read(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .unwrap();
        std_file.write_all(b"skip").unwrap();
        std_file.set_len(RESUMABLE_UPLOAD_THRESHOLD + 4).unwrap();
        std_file.seek(StdSeekFrom::Start(4)).unwrap();
        let file = File::from_std(std_file);

        server
            .session()
            .put_file(file)
            .compress(None)
            .key("key")
            .send()
            .await
            .unwrap();
        std::fs::File::create(&path)
            .unwrap()
            .set_len(RESUMABLE_UPLOAD_THRESHOLD)
            .unwrap();
        server
            .session()
            .put_path(&path)
            .compress(None)
            .key("key")
            .send()
            .await
            .unwrap();

        let requests = server.take_requests();
        for chunk in [&requests[1], &requests[3]] {
            assert_eq!(chunk.body.len(), RESUMABLE_UPLOAD_THRESHOLD as usize);
            assert!(chunk.body.iter().all(|byte| *byte == 0));
        }
    }

    #[tokio::test]
    async fn automatic_resumable_is_carried_through_many() {
        use futures_util::StreamExt as _;

        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::new(RequestKind::Chunk, StatusCode::CREATED),
            Reply::new(RequestKind::Direct, StatusCode::OK),
        ])
        .await;
        let results = server
            .session()
            .many()
            .push(
                server
                    .session()
                    .put(vec![0; RESUMABLE_UPLOAD_THRESHOLD as usize])
                    .compress(None)
                    .key("key"),
            )
            .push(
                server
                    .session()
                    .put(vec![0; RESUMABLE_UPLOAD_THRESHOLD as usize])
                    .compress(None)
                    .resumable(false)
                    .key("key"),
            )
            .max_individual_concurrency(1)
            .send()
            .await
            .collect::<Vec<_>>()
            .await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(
            |result| matches!(result, crate::OperationResult::Put(key, Ok(_)) if key == "key")
        ));
        assert_eq!(
            server
                .take_requests()
                .iter()
                .map(|request| request.kind)
                .collect::<Vec<_>>(),
            [RequestKind::Create, RequestKind::Chunk, RequestKind::Direct]
        );
    }

    #[tokio::test]
    async fn stream_and_reader_puts_ignore_resumable_eligibility() {
        let server = MockServer::start([
            Reply::new(RequestKind::Direct, StatusCode::OK),
            Reply::new(RequestKind::Direct, StatusCode::OK),
        ])
        .await;
        let stream = stream::once(async { Ok(Bytes::from_static(b"stream")) }).boxed();
        server
            .session()
            .put_stream(stream)
            .resumable(true)
            .compress(None)
            .key("key")
            .send()
            .await
            .unwrap();
        server
            .session()
            .put_read(Cursor::new(b"reader"))
            .resumable(true)
            .compress(None)
            .key("key")
            .send()
            .await
            .unwrap();
        assert!(
            server
                .take_requests()
                .iter()
                .all(|request| request.kind == RequestKind::Direct)
        );
    }

    #[tokio::test]
    async fn resumable_recovers_from_partial_progress() {
        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::new(RequestKind::Chunk, StatusCode::SERVICE_UNAVAILABLE),
            Reply::offset(RequestKind::Progress, StatusCode::NO_CONTENT, 4),
            Reply::new(RequestKind::Chunk, StatusCode::CREATED),
        ])
        .await;

        resumable_upload(&server, Bytes::from_static(b"abcdefghij"))
            .await
            .unwrap();
        let requests = server.take_requests();
        assert_eq!(requests[1].body, b"abcdefghij");
        assert_eq!(requests[3].offset.as_deref(), Some("4"));
        assert_eq!(requests[3].body, b"efghij");
    }

    #[tokio::test]
    async fn automatic_resumable_follows_offset_conflicts() {
        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::offset(RequestKind::Chunk, StatusCode::CONFLICT, 4),
            Reply::new(RequestKind::Chunk, StatusCode::CREATED),
        ])
        .await;

        resumable_upload(&server, Bytes::from_static(b"abcdefghij"))
            .await
            .unwrap();
        let requests = server.take_requests();
        assert_eq!(requests[2].offset.as_deref(), Some("4"));
        assert_eq!(requests[2].body, b"efghij");
    }

    #[tokio::test]
    async fn resumable_bounds_creation_and_progress_retries() {
        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::SERVICE_UNAVAILABLE),
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::new(RequestKind::Chunk, StatusCode::SERVICE_UNAVAILABLE),
            Reply::new(RequestKind::Progress, StatusCode::SERVICE_UNAVAILABLE),
            Reply::new(RequestKind::Progress, StatusCode::TOO_MANY_REQUESTS),
            Reply::offset(RequestKind::Progress, StatusCode::NO_CONTENT, 2),
            Reply::new(RequestKind::Chunk, StatusCode::CREATED),
        ])
        .await;
        let payload = StablePayload::Buffer(Bytes::from_static(b"retry-me"));
        let upload = create_with_retries(
            &server.session(),
            &Some("key".to_owned()),
            &Metadata::default(),
            payload.len(),
        )
        .await
        .unwrap()
        .unwrap();

        send_resumable(upload, &payload).await.unwrap();
        let requests = server.take_requests();
        assert_eq!(requests[6].offset.as_deref(), Some("2"));
        assert_eq!(requests[6].body, b"try-me");
    }

    #[tokio::test]
    async fn resumable_recovers_a_lost_final_response() {
        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::new(RequestKind::Chunk, StatusCode::GATEWAY_TIMEOUT),
            Reply::new(RequestKind::Progress, StatusCode::CREATED),
        ])
        .await;

        resumable_upload(&server, Bytes::from_static(b"complete"))
            .await
            .unwrap();
        assert_eq!(server.take_requests().len(), 3);
    }

    #[tokio::test]
    async fn resumable_exhausts_recovery_attempts() {
        let server = MockServer::start([
            Reply::new(RequestKind::Create, StatusCode::OK),
            Reply::new(RequestKind::Chunk, StatusCode::SERVICE_UNAVAILABLE),
            Reply::offset(RequestKind::Progress, StatusCode::NO_CONTENT, 0),
            Reply::new(RequestKind::Chunk, StatusCode::SERVICE_UNAVAILABLE),
            Reply::offset(RequestKind::Progress, StatusCode::NO_CONTENT, 0),
            Reply::new(RequestKind::Chunk, StatusCode::SERVICE_UNAVAILABLE),
        ])
        .await;

        let error = resumable_upload(&server, Bytes::from_static(b"retry"))
            .await
            .unwrap_err();
        let Error::UploadFailed { source } = error else {
            panic!("expected UploadFailed");
        };
        assert!(matches!(
            source.downcast_ref::<Error>(),
            Some(Error::Reqwest(error)) if error.status() == Some(StatusCode::SERVICE_UNAVAILABLE)
        ));
        assert_eq!(server.take_requests().len(), 6);
    }

    #[tokio::test]
    async fn resumable_rejects_stalled_and_invalid_offsets() {
        for offset in [0, 5, 6] {
            let server = MockServer::start([
                Reply::new(RequestKind::Create, StatusCode::OK),
                Reply::offset(RequestKind::Chunk, StatusCode::NO_CONTENT, offset),
            ])
            .await;
            let error = resumable_upload(&server, Bytes::from_static(b"12345"))
                .await
                .unwrap_err();
            let Error::UploadFailed { source } = error else {
                panic!("expected UploadFailed");
            };
            assert!(matches!(
                source.downcast_ref::<Error>(),
                Some(Error::MalformedResponse(_))
            ));
            server.take_requests();
        }
    }

    #[tokio::test]
    async fn resumable_treats_not_found_and_gone_as_terminal() {
        for status in [StatusCode::NOT_FOUND, StatusCode::GONE] {
            let server = MockServer::start([
                Reply::new(RequestKind::Create, StatusCode::OK),
                Reply::new(RequestKind::Chunk, status),
            ])
            .await;
            let error = resumable_upload(&server, Bytes::from_static(b"terminal"))
                .await
                .unwrap_err();
            let Error::UploadFailed { source } = error else {
                panic!("expected UploadFailed");
            };
            assert!(matches!(
                source.downcast_ref::<Error>(),
                Some(Error::OperationFailure {
                    status: actual,
                    message,
                }) if *actual == status.as_u16() && message == status.canonical_reason().unwrap()
            ));
            server.take_requests();
        }
    }
}
