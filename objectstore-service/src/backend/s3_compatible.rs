//! S3-compatible backend with generic protocol support.

use std::convert::Infallible;
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::{HEADER_SIZE, Metadata};
use objectstore_types::range::{ByteRange, ContentRange};
use objectstore_types::time::Timestamp;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Body, IntoUrl, Method, RequestBuilder, Response, StatusCode};

use super::extensions::{ResponseExt, SendTraced};
use crate::backend::common::{
    self, Backend, DeleteResponse, ExpiryUpdate, GetResponse, MetadataResponse, PutResponse,
    SetExpiryResponse,
};
use crate::backend::extensions::ReqwestResultExt;
use crate::change_stream::{
    ChangeStream, ChangeStreamFactory, CostTrackerStreamConfig, flush_change_stream,
};
use crate::error::{Error, ErrorKind, Result, ResultExt as _};
use crate::id::ObjectId;
use crate::stream::{ClientStream, counting_stream};

/// Configuration for [`S3CompatibleBackend`].
///
/// Supports [Amazon S3] and other S3-compatible services. Authentication is handled via
/// environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) or IAM roles.
///
/// [Amazon S3]: https://aws.amazon.com/s3/
///
/// # Example
///
/// ```yaml
/// storage:
///   type: s3compatible
///   endpoint: https://s3.amazonaws.com
///   bucket: my-bucket
/// ```
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct S3CompatibleConfig {
    /// S3 endpoint URL.
    ///
    /// Examples: `https://s3.amazonaws.com`, `http://localhost:9000` (for MinIO)
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__TYPE=s3compatible`
    /// - `OS__STORAGE__ENDPOINT=https://s3.amazonaws.com`
    pub endpoint: String,

    /// S3 bucket name.
    ///
    /// The bucket must exist before starting the server.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__BUCKET=my-bucket`
    pub bucket: String,

    /// Reports what this backend stores, for per-usecase cost attribution.
    ///
    /// # Default
    ///
    /// `None`, which disables reporting for this backend.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__COGS__SHARED_RESOURCE_ID=s3_objectstore`
    /// - `OS__STORAGE__COGS__SAMPLE_RATE=1.0` (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cogs: Option<CostTrackerStreamConfig>,
}

/// Prefix used for custom metadata in headers for the GCS backend.
///
/// See: <https://cloud.google.com/storage/docs/xml-api/reference-headers#xgoogmeta>
const GCS_CUSTOM_PREFIX: &str = "x-goog-meta-";
/// Header used to store the expiration time for GCS using the `daysSinceCustomTime` lifecycle
/// condition.
///
/// See: <https://cloud.google.com/storage/docs/xml-api/reference-headers#xgoogcustomtime>
const GCS_CUSTOM_TIME: &str = "x-goog-custom-time";

/// An authentication token that can be passed as a bearer credential.
pub trait Token: Send + Sync {
    /// Returns the token string.
    fn as_str(&self) -> &str;
}

/// Provides authentication tokens for S3-compatible requests.
pub trait TokenProvider: Send + Sync + 'static {
    /// Error returned when a token cannot be provided.
    type Error: StdError + Send + Sync + 'static;

    /// Returns a fresh token, fetching or refreshing it as needed.
    fn get_token(
        &self,
    ) -> impl Future<Output = std::result::Result<impl Token, Self::Error>> + Send;
}

/// Placeholder [`TokenProvider`] for unauthenticated backends.
#[derive(Debug)]
pub struct NoToken;

impl TokenProvider for NoToken {
    type Error = Infallible;

    #[allow(refining_impl_trait)]
    async fn get_token(&self) -> std::result::Result<NoToken, Infallible> {
        unimplemented!()
    }
}
impl Token for NoToken {
    fn as_str(&self) -> &str {
        unimplemented!()
    }
}

/// S3-compatible storage backend with pluggable authentication.
pub struct S3CompatibleBackend<T> {
    client: reqwest::Client,

    endpoint: String,
    bucket: String,

    token_provider: Option<T>,

    change_stream: Arc<dyn ChangeStream>,
}

impl<T> S3CompatibleBackend<T> {
    /// Creates a new S3-compatible backend bound to the given bucket.
    pub fn new(
        config: S3CompatibleConfig,
        token_provider: T,
        streams: &ChangeStreamFactory,
    ) -> Self {
        Self::build(config, Some(token_provider), streams)
    }

    fn build(
        config: S3CompatibleConfig,
        token_provider: Option<T>,
        streams: &ChangeStreamFactory,
    ) -> Self {
        let S3CompatibleConfig {
            endpoint,
            bucket,
            cogs,
        } = config;
        Self {
            client: common::reqwest_client(),
            endpoint,
            bucket,
            token_provider,
            change_stream: streams.build(cogs.as_ref()),
        }
    }

    /// Formats the S3 object URL for the given key.
    fn object_url(&self, id: &ObjectId) -> String {
        format!("{}/{}/{}", self.endpoint, self.bucket, id.as_storage_path())
    }
}

/// Number of bytes the given headers occupy as stored object metadata.
fn headers_size(headers: &HeaderMap) -> u64 {
    headers
        .iter()
        .map(|(name, value)| name.as_str().len() as u64 + value.len() as u64)
        .sum()
}

/// Wraps [`Metadata::to_headers`] with GCS-specific concerns (tombstone + custom-time).
fn metadata_to_gcs_headers(
    metadata: &Metadata,
    prefix: &str,
) -> Result<HeaderMap, objectstore_types::metadata::Error> {
    let mut headers = metadata.to_headers(prefix)?;

    // The size is derived from the native `Content-Length` on every read, so it must not be
    // persisted: metadata updates rewrite *all* stored metadata, and a stored `x-sn-size` key
    // is rejected by the GCS JSON backend when it deserializes the object.
    let size = HeaderName::try_from(format!("{prefix}{HEADER_SIZE}"))?;
    headers.remove(&size);

    // GCS custom-time for lifecycle expiration
    if let Some(expires_at) = metadata.time_expires {
        let expires_at = expires_at.as_rfc3339();
        headers.append(GCS_CUSTOM_TIME, expires_at.to_string().parse()?);
    }
    Ok(headers)
}

impl<T> S3CompatibleBackend<T>
where
    T: TokenProvider,
{
    /// Creates a request builder with the appropriate authentication.
    async fn request(&self, method: Method, url: impl IntoUrl) -> Result<RequestBuilder> {
        let mut builder = self.client.request(method, url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(
                provider
                    .get_token()
                    .await
                    .context(ErrorKind::BackendFailure, "getting S3 authentication token")?
                    .as_str(),
            );
        }
        Ok(builder)
    }

    /// Fetches object metadata using the given HTTP method (GET or HEAD) and
    /// returns it with the response without modifying the object.
    async fn request_object(
        &self,
        method: Method,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<Option<(Metadata, Option<ContentRange>, Response)>> {
        let object_url = self.object_url(id);

        let mut builder = self.request(method, &object_url).await?;
        if let Some(r) = range {
            builder = builder.header(reqwest::header::RANGE, r.to_header_value());
        }
        let response = builder
            .send_traced()
            .await
            .reqwest_context("sending an S3 object request")?;

        if response.status() == StatusCode::NOT_FOUND {
            objectstore_log::debug!("Object not found");
            response.drain_body().await;
            return Ok(None);
        }

        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            let raw = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok());
            let total = raw.and_then(ContentRange::parse_unsatisfiable_total);
            let err = match total {
                Some(total) => ErrorKind::RangeNotSatisfiable { total }.into(),
                None => Error::new(ErrorKind::BackendFailure, "invalid S3 416 Content-Range"),
            };
            response.drain_body().await;
            return Err(err);
        }

        let response = response.check_error("getting an S3 object").await?;

        let headers = response.headers();
        let mut metadata = Metadata::from_headers(headers, GCS_CUSTOM_PREFIX)
            .context(ErrorKind::CorruptData, "decoding S3 object metadata")?;

        let content_range = if response.status() == StatusCode::PARTIAL_CONTENT {
            let range = headers
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<ContentRange>().ok())
                .ok_or_else(|| {
                    Error::new(ErrorKind::BackendFailure, "missing S3 206 Content-Range")
                })?;
            metadata.size = Some(range.total as usize);
            Some(range)
        } else {
            // NB: Read the header rather than `Response::content_length`, which reports the
            // length of the decoded body and is therefore always zero for a HEAD response.
            let size = headers
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.parse::<usize>())
                .transpose()
                .context(ErrorKind::CorruptData, "decoding S3 Content-Length")?;

            if let Some(size) = size {
                metadata.size = Some(size);
            } else {
                objectstore_log::warn!("S3: 200 response missing Content-Length header");
            }
            None
        };

        // Filter already expired objects but leave them to garbage collection
        if metadata.is_expired(access_time) {
            objectstore_log::debug!("Object found but past expiry");
            response.drain_body().await;
            return Ok(None);
        }

        Ok(Some((metadata, content_range, response)))
    }

    /// Issues a request to update the metadata for the given object.
    async fn update_metadata(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        deadline: Timestamp,
        etag: &HeaderValue,
    ) -> Result<SetExpiryResponse> {
        // NB: Meta updates require CopyObject + REPLACE along with *all* metadata. See
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
        let request = self
            .request(Method::PUT, self.object_url(id))
            .await?
            .header(
                "x-amz-copy-source",
                format!("/{}/{}", self.bucket, id.as_storage_path()),
            )
            .header("x-amz-metadata-directive", "REPLACE")
            .header("x-amz-copy-source-if-match", etag.clone())
            .headers(
                metadata_to_gcs_headers(metadata, GCS_CUSTOM_PREFIX)
                    .context(ErrorKind::InvalidMetadata, "encoding S3 object metadata")?,
            );

        let response = request.send_traced().await;
        let response = response.reqwest_context("updating S3 expiration")?;
        let outcome = match response.status() {
            StatusCode::NOT_FOUND => Some(SetExpiryResponse::NotFound),
            StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED => {
                Some(SetExpiryResponse::Rejected)
            }
            _ => None,
        };
        if let Some(outcome) = outcome {
            response.drain_body().await;
            return Ok(outcome);
        }
        response
            .check_error("updating S3 expiration")
            .await?
            .drain_body()
            .await;

        Ok(SetExpiryResponse::Satisfied(deadline))
    }
}

impl<T> fmt::Debug for S3CompatibleBackend<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Compatible")
            .field("client", &self.client)
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl S3CompatibleBackend<NoToken> {
    /// Creates a new S3-compatible backend that sends unauthenticated requests.
    pub fn without_token(config: S3CompatibleConfig, streams: &ChangeStreamFactory) -> Self {
        Self::build(config, None, streams)
    }
}

#[async_trait::async_trait]
impl<T: TokenProvider> Backend for S3CompatibleBackend<T> {
    fn name(&self) -> &'static str {
        "s3-compatible"
    }

    #[tracing::instrument(level = "debug", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
        _access_time: Timestamp,
    ) -> Result<PutResponse> {
        objectstore_log::debug!("Writing to s3_compatible backend");
        let headers = metadata_to_gcs_headers(metadata, GCS_CUSTOM_PREFIX)
            .context(ErrorKind::InvalidMetadata, "encoding S3 object metadata")?;
        let metadata_size = headers_size(&headers);

        // A successful PUT does not report the stored size back, so count what we send.
        let (payload_size, counted) = counting_stream(stream);

        self.request(Method::PUT, self.object_url(id))
            .await?
            .headers(headers)
            .body(Body::wrap_stream(counted))
            .send_traced()
            .await
            .check_error("uploading an S3 object")
            .await?
            .drain_body()
            .await;

        self.change_stream.write(
            id,
            metadata_size + payload_size.load(Ordering::Relaxed),
            metadata.time_expires,
        );

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from s3_compatible backend");

        let Some((metadata, content_range, response)) = self
            .request_object(Method::GET, id, access_time, range)
            .await?
        else {
            return Ok(None);
        };

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some((metadata, content_range, stream.boxed())))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_metadata(
        &self,
        id: &ObjectId,
        access_time: Timestamp,
    ) -> Result<MetadataResponse> {
        objectstore_log::debug!("Reading metadata from s3_compatible backend");
        let response = self
            .request_object(Method::HEAD, id, access_time, None)
            .await?;
        Ok(response.map(|(metadata, _, _)| metadata))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn set_expiry(
        &self,
        id: &ObjectId,
        target: ExpiryUpdate,
        access_time: Timestamp,
    ) -> Result<SetExpiryResponse> {
        let Some((mut metadata, _, response)) = self
            .request_object(Method::HEAD, id, access_time, None)
            .await?
        else {
            return Ok(SetExpiryResponse::NotFound);
        };

        let etag = response.headers().get(reqwest::header::ETAG).cloned();
        response.drain_body().await;

        let Some(current_expiry) = metadata.time_expires else {
            return Ok(SetExpiryResponse::Rejected);
        };
        let Some(expire_at) = target.resolve(metadata.time_created, access_time)? else {
            return Ok(SetExpiryResponse::Rejected);
        };
        if current_expiry >= expire_at {
            return Ok(SetExpiryResponse::Satisfied(expire_at)); // already satisfied
        }

        let etag = etag.ok_or_else(|| {
            Error::new(ErrorKind::BackendFailure, "S3 HEAD response missing ETag")
        })?;

        metadata.expiration_policy = common::extended_expiration_policy(
            metadata.expiration_policy,
            metadata.time_created,
            current_expiry,
            expire_at,
        )?;
        metadata.time_expires = Some(expire_at);
        let outcome = self
            .update_metadata(id, &metadata, expire_at, &etag)
            .await?;
        if matches!(outcome, SetExpiryResponse::Satisfied(_)) {
            self.change_stream.update(id, Some(expire_at));
        }

        Ok(outcome)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_object(
        &self,
        id: &ObjectId,
        _access_time: Timestamp,
    ) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from s3_compatible backend");
        let response = self
            .request(Method::DELETE, self.object_url(id))
            .await?
            .send_traced()
            .await
            .reqwest_context("sending an S3 delete request")?;

        // S3 deletes are idempotent; they return 204 whether a key existed or not. This
        // branch catches other 404s, like from a missing bucket.
        if response.status() == StatusCode::NOT_FOUND {
            response.drain_body().await;
            return Ok(());
        }

        response
            .check_error("deleting an S3 object")
            .await?
            .drain_body()
            .await;

        // If the object didn't exist in the first place, this emits a spurious message
        // due to S3 returning 204 to DELETEs whether the object existed or not.
        self.change_stream.delete(id);

        Ok(())
    }

    async fn join(&self) {
        flush_change_stream(&self.change_stream).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use anyhow::Result;
    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::Backend;
    use crate::id::ObjectContext;
    use crate::stream;

    // NB: To run these tests, you need to have a MinIO server running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up MinIO via devservices.

    fn create_test_backend() -> S3CompatibleBackend<NoToken> {
        S3CompatibleBackend::without_token(
            S3CompatibleConfig {
                endpoint: "http://localhost:8089".into(),
                bucket: "test-bucket".into(),
                cogs: None,
            },
            &ChangeStreamFactory::default(),
        )
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    fn read_http_request(connection: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            connection.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
        }
        String::from_utf8(bytes).unwrap()
    }

    fn start_copy_server(
        copy_status: &'static str,
        metadata: Metadata,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut head, _) = listener.accept().unwrap();
            assert!(read_http_request(&mut head).starts_with("HEAD "));
            write!(
                head,
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"etag\"\r\nConnection: close\r\n"
            )
            .unwrap();
            for (name, value) in metadata.to_headers(GCS_CUSTOM_PREFIX).unwrap().iter() {
                write!(head, "{name}: {}\r\n", value.to_str().unwrap()).unwrap();
            }
            write!(head, "\r\n").unwrap();
            let (mut copy, _) = listener.accept().unwrap();
            request_tx.send(read_http_request(&mut copy)).unwrap();
            write!(
                copy,
                "HTTP/1.1 {copy_status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });
        (endpoint, request_rx, server)
    }

    #[tokio::test]
    async fn update_metadata_uses_conditional_s3_copy() {
        let created = Timestamp::now();
        let deadline = created + Duration::from_hours(2);
        for (status, expected) in [
            ("200 OK", SetExpiryResponse::Satisfied(deadline)),
            ("404 Not Found", SetExpiryResponse::NotFound),
            ("409 Conflict", SetExpiryResponse::Rejected),
            ("412 Precondition Failed", SetExpiryResponse::Rejected),
        ] {
            let (endpoint, request_rx, server) = start_copy_server(
                status,
                Metadata {
                    expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
                    time_created: Some(created),
                    time_expires: Some(created + Duration::from_hours(1)),
                    custom: [("preserved".into(), "yes".into())].into(),
                    ..Default::default()
                },
            );
            let backend = S3CompatibleBackend::without_token(
                S3CompatibleConfig {
                    endpoint,
                    bucket: "bucket".into(),
                    cogs: None,
                },
                &ChangeStreamFactory::default(),
            );

            assert_eq!(
                backend
                    .set_expiry(
                        &make_id(),
                        common::ExpiryTarget::At(deadline).into(),
                        created
                    )
                    .await
                    .unwrap(),
                expected
            );
            let request = request_rx.recv().unwrap().to_ascii_lowercase();
            assert!(request.contains("x-amz-copy-source: /bucket/"));
            assert!(request.contains("x-amz-metadata-directive: replace"));
            assert!(request.contains("x-amz-copy-source-if-match: \"etag\""));
            let expected_headers = Metadata {
                expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(2)),
                time_created: Some(created),
                time_expires: Some(deadline),
                custom: [("preserved".into(), "yes".into())].into(),
                ..Default::default()
            }
            .to_headers(GCS_CUSTOM_PREFIX)
            .unwrap();
            for (name, value) in &expected_headers {
                assert!(request.contains(
                    &format!("{name}: {}", value.to_str().unwrap()).to_ascii_lowercase()
                ));
            }
            server.join().unwrap();
        }
    }

    #[test]
    fn metadata_to_gcs_headers_omits_size() {
        let metadata = Metadata {
            size: Some(4096),
            ..Default::default()
        };

        let headers = metadata_to_gcs_headers(&metadata, GCS_CUSTOM_PREFIX).unwrap();

        // Persisting the size would store a key that the GCS JSON backend rejects on read.
        assert!(headers.get("x-goog-meta-x-sn-size").is_none());
    }

    #[test]
    fn metadata_to_gcs_headers_uses_time_expires() {
        let expires = Timestamp::now() + Duration::from_hours(1);
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_hours(1)),
            time_expires: Some(expires),
            ..Default::default()
        };

        let headers = metadata_to_gcs_headers(&metadata, GCS_CUSTOM_PREFIX).unwrap();
        let custom_time = headers.get(GCS_CUSTOM_TIME).unwrap().to_str().unwrap();
        let expected = expires.as_rfc3339().to_string();
        assert_eq!(custom_time, expected);
    }

    #[test]
    fn metadata_to_gcs_headers_escapes_unicode() {
        let metadata = Metadata {
            filename: Some("réport-📄.pdf".into()),
            custom: BTreeMap::from_iter([("release".into(), "vérsion-1.0-🚀".into())]),
            ..Default::default()
        };

        let headers = metadata_to_gcs_headers(&metadata, GCS_CUSTOM_PREFIX).unwrap();
        assert_eq!(
            headers.get("x-goog-meta-x-sn-filename").unwrap(),
            "r%C3%A9port-%F0%9F%93%84.pdf",
        );
        assert_eq!(
            headers.get("x-goog-meta-x-snme-release").unwrap(),
            "v%C3%A9rsion-1.0-%F0%9F%9A%80",
        );

        // The prefixed headers this backend writes are the ones it reads back.
        let roundtripped = Metadata::from_headers(&headers, GCS_CUSTOM_PREFIX).unwrap();
        assert_eq!(roundtripped.filename, metadata.filename);
        assert_eq!(roundtripped.custom, metadata.custom);
    }

    #[test]
    fn headers_size_counts_names_and_values() {
        let mut headers = HeaderMap::new();
        headers.insert("x-goog-meta-a", "1".parse().unwrap());
        headers.insert("x-goog-meta-bb", "22".parse().unwrap());

        assert_eq!(
            headers_size(&headers),
            ("x-goog-meta-a".len() + 1 + "x-goog-meta-bb".len() + 2) as u64
        );
    }

    #[tokio::test]
    async fn test_get_metadata_nonexistent() -> Result<()> {
        let backend = create_test_backend();
        let id = make_id();
        let result = backend.get_metadata(&id, Timestamp::now()).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "MinIO does not support streaming bodies (requires Content-Length)"]
    async fn test_get_metadata_reports_size() -> Result<()> {
        let backend = create_test_backend();
        let id = make_id();
        let payload = "hello, world";

        backend
            .put_object(
                &id,
                &Metadata::default(),
                stream::single(payload),
                Timestamp::now(),
            )
            .await?;

        // The size must come from the `Content-Length` header, not from the (empty) body of
        // the HEAD response.
        let metadata = backend
            .get_metadata(&id, Timestamp::now())
            .await?
            .expect("object exists");
        assert_eq!(metadata.size, Some(payload.len()));

        Ok(())
    }

    #[tokio::test]
    #[ignore = "MinIO does not support streaming bodies (requires Content-Length)"]
    async fn test_ttl_immediate() -> Result<()> {
        let backend = create_test_backend();
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        let get_result = backend.get_object(&id, Timestamp::now(), None).await?;
        assert!(get_result.is_none());

        let head_result = backend.get_metadata(&id, Timestamp::now()).await?;
        assert!(head_result.is_none());

        Ok(())
    }

    #[tokio::test]
    #[ignore = "MinIO does not support streaming bodies (requires Content-Length)"]
    async fn test_tti_immediate() -> Result<()> {
        let backend = create_test_backend();
        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            time_expires: Some(Timestamp::now() - Duration::from_secs(1)),
            ..Default::default()
        };

        backend
            .put_object(
                &id,
                &metadata,
                stream::single("hello, world"),
                Timestamp::now(),
            )
            .await?;

        let get_result = backend.get_object(&id, Timestamp::now(), None).await?;
        assert!(get_result.is_none());

        let head_result = backend.get_metadata(&id, Timestamp::now()).await?;
        assert!(head_result.is_none());

        Ok(())
    }
}
