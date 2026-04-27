//! Backend that can be used with any S3-compatible object store (Amazon S3, MinIO, R2, etc.).

use std::time::{Duration, SystemTime};
use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use reqwest::header::{self, HeaderMap, HeaderName};
use s3::Bucket;
use s3::command::Command;
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::region::Region;
use s3::request::Request as _;
use s3::request::tokio_backend::ReqwestRequest;
use serde::{Deserialize, Serialize};
use tokio_util::io::StreamReader;

use crate::backend::common::{Backend, DeleteResponse, GetResponse, MetadataResponse, PutResponse};
use crate::error::{Error, Result};
use crate::id::ObjectId;
use crate::stream::{self, ClientStream};

/// Configuration for [`S3CompatibleBackend`].
///
/// Supports [Amazon S3] and other S3-compatible services such as MinIO. AWS
/// credentials are resolved via the standard [SDK chain] (env vars, profile,
/// STS web identity, ECS container, EC2 IMDS); if no credentials can be
/// resolved, the backend falls back to anonymous (unauthenticated) requests.
///
/// [Amazon S3]: https://aws.amazon.com/s3/
/// [SDK chain]: https://docs.rs/aws-creds/0.39.1/aws_creds/credentials/struct.Credentials.html
///
/// # Example
///
/// ```yaml
/// storage:
///   type: s3compatible
///   endpoint: https://s3.amazonaws.com
///   bucket: my-bucket
///   region: us-east-1
///   use_path_style: false
///   metadata_prefix: x-amz-meta-
///   protocol_prefix: x-amz-
///   custom_time_header: x-amz-meta-expiry
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Region label sent in SigV4 signatures.
    ///
    /// On AWS this must match the bucket's actual region. MinIO and most other
    /// S3-compatible services accept any value.
    ///
    /// # Default
    ///
    /// `"us-east-1"`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__REGION=us-west-2`
    #[serde(default = "default_region")]
    pub region: String,

    /// Whether to use path-style addressing (`https://host/bucket/key`)
    /// instead of virtual-hosted-style (`https://bucket.host/key`).
    ///
    /// # Default
    ///
    /// `true`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__USE_PATH_STYLE=false`
    #[serde(default = "default_use_path_style")]
    pub use_path_style: bool,

    /// Prefix used for custom object metadata on the wire.
    ///
    /// # Default
    ///
    /// `"x-amz-meta-"`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__METADATA_PREFIX=x-goog-meta-`
    #[serde(default = "default_metadata_prefix")]
    pub metadata_prefix: String,

    /// Protocol-level header prefix used for non-metadata request headers
    /// like `{prefix}copy-source` and `{prefix}metadata-directive`.
    ///
    /// # Default
    ///
    /// `"x-amz-"`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__PROTOCOL_PREFIX=x-goog-`
    #[serde(default = "default_protocol_prefix")]
    pub protocol_prefix: String,

    /// Header carrying the object's resolved expiration timestamp.
    ///
    /// # Default
    ///
    /// `"x-amz-meta-expiry"`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__CUSTOM_TIME_HEADER=x-goog-custom-time`
    #[serde(default = "default_custom_time_header")]
    pub custom_time_header: String,
}

fn default_region() -> String {
    "us-east-1".to_owned()
}

fn default_use_path_style() -> bool {
    true
}

fn default_metadata_prefix() -> String {
    "x-amz-meta-".to_owned()
}

fn default_protocol_prefix() -> String {
    "x-amz-".to_owned()
}

fn default_custom_time_header() -> String {
    "x-amz-meta-expiry".to_owned()
}

/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

/// An authentication token that can be passed as a bearer credential.
pub trait Token: Send + Sync {
    /// Returns the token string.
    fn as_str(&self) -> &str;
}

/// Provides authentication tokens for S3-compatible requests.
pub trait TokenProvider: Send + Sync + 'static {
    /// Returns a fresh token, fetching or refreshing it as needed.
    fn get_token(&self) -> impl Future<Output = anyhow::Result<impl Token>> + Send;
}

/// Placeholder [`TokenProvider`] for unauthenticated backends.
#[derive(Debug)]
pub struct NoToken;

impl TokenProvider for NoToken {
    #[allow(refining_impl_trait)]
    async fn get_token(&self) -> anyhow::Result<NoToken> {
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
    bucket: Box<Bucket>,
    endpoint: String,
    token_provider: Option<T>,
    metadata_prefix: String,
    protocol_prefix: String,
    custom_time_header: String,
}

/// Wraps [`Metadata::to_headers`] with S3-specific concerns.
fn metadata_to_s3_headers(
    metadata: &Metadata,
    prefix: &str,
    custom_time_header: &str,
) -> Result<HeaderMap, objectstore_types::metadata::Error> {
    let mut headers = metadata.to_headers(prefix)?;
    if let Some(expires_in) = metadata.expiration_policy.expires_in() {
        let expires_at = humantime::format_rfc3339_seconds(SystemTime::now() + expires_in);
        let name = HeaderName::try_from(custom_time_header)?;
        headers.append(name, expires_at.to_string().parse()?);
    }
    Ok(headers)
}

fn metadata_from_headers(headers: &HeaderMap, prefix: &str) -> Result<Metadata> {
    let mut metadata = Metadata::from_headers(headers, prefix)?;
    metadata.size = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse::<usize>().ok());
    Ok(metadata)
}

impl<T> S3CompatibleBackend<T> {
    /// Creates a new S3-compatible backend bound to the given config and token provider.
    pub fn new(config: S3CompatibleConfig, token_provider: T) -> anyhow::Result<Self> {
        let mut this = Self::from_config(config)?;
        this.token_provider = Some(token_provider);
        Ok(this)
    }

    fn from_config(config: S3CompatibleConfig) -> anyhow::Result<Self> {
        let credentials = Credentials::default().or_else(|_| Credentials::anonymous())?;

        let region = Region::Custom {
            region: config.region,
            endpoint: config.endpoint.clone(),
        };

        let mut bucket = Bucket::new(&config.bucket, region, credentials)?;
        if config.use_path_style {
            bucket.set_path_style();
        }

        Ok(Self {
            bucket,
            endpoint: config.endpoint,
            metadata_prefix: config.metadata_prefix,
            protocol_prefix: config.protocol_prefix,
            custom_time_header: config.custom_time_header,
            token_provider: None,
        })
    }

    fn object_path(&self, id: &ObjectId) -> String {
        format!("/{}", id.as_storage_path())
    }

    /// If `metadata` has a [`TimeToIdle`] policy and a
    /// [`custom_time_header`] is configured, bumps the recorded expiry via a
    /// metadata-only copy-in-place when it would otherwise fall outside the
    /// 1-day debounce window.
    ///
    /// [`TimeToIdle`]: objectstore_types::metadata::ExpirationPolicy::TimeToIdle
    /// [`custom_time_header`]: S3CompatibleConfig::custom_time_header
    async fn bump_tti_if_needed(
        &self,
        path: &str,
        headers: &HeaderMap,
        metadata: &Metadata,
    ) -> Result<()> {
        let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy else {
            return Ok(());
        };

        // TODO: Inject the access time from the request.
        let access_time = SystemTime::now();

        let current_expiry = headers
            .get(self.custom_time_header.as_str())
            .and_then(|v| v.to_str().ok())
            .and_then(|s| humantime::parse_rfc3339(s).ok())
            .unwrap_or(access_time);
        let new_expiry = access_time + tti;

        let bump_amount = new_expiry
            .duration_since(current_expiry)
            .unwrap_or_default();

        if bump_amount > TTI_DEBOUNCE {
            // TODO: Schedule into background persistently so this doesn't get lost on restarts.
            self.update_metadata(path, metadata).await?;
        }

        Ok(())
    }

    /// Rewrites the object's metadata in place via a copy-with-REPLACE
    /// request. Used to bump the expiration time header for TTI objects.
    async fn update_metadata(&self, path: &str, metadata: &Metadata) -> Result<()> {
        let copy_source_header = format!("{}copy-source", self.protocol_prefix);
        let metadata_directive_header = format!("{}metadata-directive", self.protocol_prefix);
        let copy_source = format!("/{}{}", self.bucket.name(), path);

        let mut request = self
            .bucket
            .put_object_builder(path, &[])
            .with_content_type(metadata.content_type.as_ref())
            .with_header(&copy_source_header, copy_source)
            .map_err(|e| map_s3_error(e, "S3: failed to set copy-source header"))?
            .with_header(&metadata_directive_header, "REPLACE")
            .map_err(|e| map_s3_error(e, "S3: failed to set metadata-directive header"))?;

        if let Some(compression) = metadata.compression {
            request = request
                .with_content_encoding(compression.as_str())
                .map_err(|e| map_s3_error(e, "S3: failed to set content-encoding"))?;
        }

        let headers =
            metadata_to_s3_headers(metadata, &self.metadata_prefix, &self.custom_time_header)
                .map_err(Error::Metadata)?;
        for (name, value) in &headers {
            if name == header::CONTENT_TYPE || name == header::CONTENT_ENCODING {
                continue;
            }
            let value_str = value.to_str().map_err(|e| Error::Generic {
                context: format!("S3: non-ascii metadata header value for {name}"),
                cause: Some(Box::new(e)),
            })?;
            request = request
                .with_header(name.as_str(), value_str)
                .map_err(|e| map_s3_error(e, "S3: failed to set object metadata"))?;
        }

        request
            .execute()
            .await
            .map_err(|e| map_s3_error(e, "S3: failed to update object metadata"))?;

        Ok(())
    }

    /// Issues a HEAD request and returns the raw response headers.
    ///
    /// `Bucket::head_object` is not sufficient because it only surfaces
    /// `x-amz-meta-*` keys, which could be different from `self.metadata_prefix`.
    async fn head_object(&self, path: &str) -> Result<Option<HeaderMap>> {
        let request = ReqwestRequest::new(&self.bucket, path, Command::HeadObject)
            .await
            .map_err(|e| map_s3_error(e, "S3: failed to build head request"))?;

        match request.response_header().await {
            Ok((headers, _)) => Ok(Some(headers)),
            Err(ref e) if is_not_found(e) => {
                objectstore_log::debug!("Object not found");
                Ok(None)
            }
            Err(e) => Err(map_s3_error(e, "S3: failed to head object")),
        }
    }
}

impl S3CompatibleBackend<NoToken> {
    /// Creates a new S3-compatible backend that sends unauthenticated requests.
    pub fn without_token(config: S3CompatibleConfig) -> anyhow::Result<Self> {
        Self::from_config(config)
    }
}

impl<T> fmt::Debug for S3CompatibleBackend<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3CompatibleBackend")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("metadata_prefix", &self.metadata_prefix)
            .field("protocol_prefix", &self.protocol_prefix)
            .field("custom_time_header", &self.custom_time_header)
            .finish_non_exhaustive()
    }
}

/// Maps an [`S3Error`] to our [`Error`] type with the given context.
fn map_s3_error(error: S3Error, context: &str) -> Error {
    Error::Generic {
        context: context.to_owned(),
        cause: Some(Box::new(error)),
    }
}

/// Returns `true` if `error` is an HTTP 404 from rust-s3.
fn is_not_found(error: &S3Error) -> bool {
    matches!(error, S3Error::HttpFailWithBody(404, _))
}

#[async_trait::async_trait]
impl<T: TokenProvider> Backend for S3CompatibleBackend<T> {
    fn name(&self) -> &'static str {
        "s3_compatible"
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        objectstore_log::debug!("Writing to s3_compatible backend");
        let path = self.object_path(id);

        let mut request = self
            .bucket
            .put_object_stream_builder(&path)
            .with_content_type(metadata.content_type.as_ref());

        if let Some(compression) = metadata.compression {
            request = request
                .with_content_encoding(compression.as_str())
                .map_err(|e| map_s3_error(e, "S3: failed to set content-encoding"))?;
        }

        let headers =
            metadata_to_s3_headers(metadata, &self.metadata_prefix, &self.custom_time_header)
                .map_err(Error::Metadata)?;
        for (name, value) in &headers {
            if name == header::CONTENT_TYPE || name == header::CONTENT_ENCODING {
                continue;
            }
            let value_str = value.to_str().map_err(|e| Error::Generic {
                context: format!("S3: non-ascii metadata header value for {name}"),
                cause: Some(Box::new(e)),
            })?;
            request = request
                .with_header(name, value_str)
                .map_err(|e| map_s3_error(e, "S3: failed to set object metadata"))?;
        }

        let mut reader = StreamReader::new(stream.map_err(io::Error::other));
        request
            .execute_stream(&mut reader)
            .await
            .map_err(|cause| match &cause {
                S3Error::Io(io_err) => match stream::unpack_client_error(io_err) {
                    Some(ce) => Error::Client(ce),
                    None => map_s3_error(cause, "S3: failed to put object"),
                },
                _ => map_s3_error(cause, "S3: failed to put object"),
            })?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from s3_compatible backend");
        let path = self.object_path(id);

        let Some(headers) = self.head_object(&path).await? else {
            return Ok(None);
        };
        let metadata = metadata_from_headers(&headers, &self.metadata_prefix)?;
        self.bump_tti_if_needed(&path, &headers, &metadata).await?;

        let response = match self.bucket.get_object_stream(&path).await {
            Ok(response) => response,
            Err(ref e) if is_not_found(e) => {
                // Object was deleted between HEAD and GET; treat as missing.
                objectstore_log::debug!("Object disappeared between head and get");
                return Ok(None);
            }
            Err(e) => return Err(map_s3_error(e, "S3: failed to get object")),
        };

        let stream = response.bytes.map_err(io::Error::other).boxed();
        Ok(Some((metadata, stream)))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        objectstore_log::debug!("Reading metadata from s3_compatible backend");
        let path = self.object_path(id);

        let Some(headers) = self.head_object(&path).await? else {
            return Ok(None);
        };
        let metadata = metadata_from_headers(&headers, &self.metadata_prefix)?;
        self.bump_tti_if_needed(&path, &headers, &metadata).await?;
        Ok(Some(metadata))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from s3_compatible backend");
        let path = self.object_path(id);

        match self.bucket.delete_object(&path).await {
            Ok(_) => Ok(()),
            Err(ref e) if is_not_found(e) => {
                objectstore_log::debug!("Object not found");
                Ok(())
            }
            Err(e) => Err(map_s3_error(e, "S3: failed to delete object")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::SystemTime;

    use anyhow::Result;
    use objectstore_types::metadata::{Compression, ExpirationPolicy};
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
        S3CompatibleBackend::without_token(S3CompatibleConfig {
            endpoint: "http://localhost:8089".into(),
            bucket: "test-bucket".into(),
            region: default_region(),
            use_path_style: default_use_path_style(),
            metadata_prefix: default_metadata_prefix(),
            protocol_prefix: default_protocol_prefix(),
            custom_time_header: default_custom_time_header(),
        })
        .unwrap()
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            origin: Some("203.0.113.42".into()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            time_created: Some(SystemTime::now()),
            time_expires: None,
            size: None,
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = stream::read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "hello, world");
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.origin, metadata.origin);
        assert_eq!(meta.custom, metadata.custom);
        assert!(metadata.time_created.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        backend.delete_object(&id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        let metadata = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello"))
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = stream::read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "world");
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_streaming_upload() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        let metadata = Metadata::default();

        // rust-s3 dispatches to multipart uploads when the stream exceeds the
        // 8 MiB chunk size. Use 20 MiB to ensure multiple parts are uploaded.
        let chunk = bytes::Bytes::from(vec![0xab; 1024 * 1024]); // 1 MiB
        let stream =
            futures_util::stream::iter(std::iter::repeat_with(move || Ok(chunk.clone())).take(20))
                .boxed();

        backend.put_object(&id, &metadata, stream).await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;

        assert_eq!(payload.len(), 20 * 1024 * 1024);
        assert!(payload.iter().all(|&b| b == 0xab));
        assert_eq!(meta.size, Some(20 * 1024 * 1024));

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        let metadata = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello"))
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = stream::read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "world");
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        let metadata = Metadata::default();

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        backend.delete_object(&id).await?;

        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_immediate() -> Result<()> {
        // NB: We create a TTL that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend();

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        // NB: We create a TTI that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend();

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_returns_metadata() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            origin: Some("203.0.113.42".into()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        let meta = backend.get_metadata(&id).await?.unwrap();
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.origin, metadata.origin);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_nonexistent() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        let result = backend.get_metadata(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_bumps_tti() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        // TTI must exceed TTI_DEBOUNCE (1 day) for the bump condition to be reachable.
        let tti = Duration::from_secs(2 * 24 * 3600); // 2 days
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        // Manually set custom_time to just inside the bump window.
        // The bump condition is: expire_at < now + tti - TTI_DEBOUNCE.
        let object_url = backend.object_url(&id)?;
        let old_deadline = SystemTime::now() + tti - TTI_DEBOUNCE - Duration::from_secs(60);
        backend.update_custom_time(object_url, old_deadline).await?;

        // First get_metadata sees the old timestamp and triggers a TTI bump.
        let pre_meta = backend.get_metadata(&id).await?.unwrap();
        let pre_expiry = pre_meta.time_expires.unwrap();

        // Second get_metadata sees the bumped timestamp.
        let post_meta = backend.get_metadata(&id).await?.unwrap();
        let post_expiry = post_meta.time_expires.unwrap();
        assert!(
            post_expiry > pre_expiry,
            "TTI bump should have extended the expiry: {pre_expiry:?} -> {post_expiry:?}"
        );

        // Verify the payload is still intact after the bump.
        let (_, stream) = backend.get_object(&id).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;
        assert_eq!(&payload, b"hello, world");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_does_not_bump_fresh_tti() -> Result<()> {
        let backend = create_test_backend();

        let id = make_id();
        // TTI must exceed TTI_DEBOUNCE (1 day) for the bump condition to be reachable.
        let tti = Duration::from_secs(2 * 24 * 3600); // 2 days
        let metadata = Metadata {
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(tti),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single("hello, world"))
            .await?;

        // A freshly written object has time_expires ≈ now + 2d, which is well outside
        // the bump window (now + 2d - 1d = now + 1d). No bump should occur.
        let first = backend.get_metadata(&id).await?.unwrap();
        let first_expiry = first.time_expires.unwrap();

        let second = backend.get_metadata(&id).await?.unwrap();
        let second_expiry = second.time_expires.unwrap();

        assert_eq!(
            first_expiry, second_expiry,
            "Fresh TTI object should not have its expiry bumped"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compressed_payload_roundtrip() -> Result<()> {
        use objectstore_types::metadata::Compression;

        let backend = create_test_backend();

        let plaintext = b"hello, world (but compressed with zstd)";
        let compressed = zstd::encode_all(&plaintext[..], 3)?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            compression: Some(Compression::Zstd),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, stream::single(compressed.clone()))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();
        let payload = stream::read_to_vec(stream).await?;

        assert_eq!(meta.compression, Some(Compression::Zstd));
        assert_eq!(
            payload, compressed,
            "Payload should be returned still compressed, not auto-decompressed"
        );

        Ok(())
    }
}
