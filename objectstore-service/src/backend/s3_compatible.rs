//! Backend that can be used with any S3-compatible object store (Amazon S3, MinIO, R2, etc.).

use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::{StreamExt, TryStreamExt};
use gcp_auth::TokenProvider as _;
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use reqwest::header::{self, HeaderMap, HeaderName};
use s3::Bucket;
use s3::command::Command;
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::region::Region;
use s3::request::Request as _;
use s3::request::tokio_backend::ReqwestRequest;
use secrecy::{ExposeSecret, SecretBox};
use serde::{Deserialize, Serialize};
use tokio_util::io::StreamReader;

use crate::backend::common::{Backend, DeleteResponse, GetResponse, MetadataResponse, PutResponse};
use crate::error::{Error, Result};
use crate::gcp_auth::PrefetchingTokenProvider;
use crate::id::ObjectId;
use crate::secret::ConfigSecret;
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

    /// Optional extra header that mirrors the object's resolved expiration
    /// timestamp, on top of the canonical `x-sn-time-expires`.
    ///
    /// Used to interoperate with S3-compatible backends that recognize a
    /// dedicated header for object lifecycle (such as GCS, which uses
    /// `x-goog-custom-time`). Leave unset for plain S3.
    ///
    /// # Default
    ///
    /// `None`
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__CUSTOM_TIME_HEADER=x-goog-custom-time`
    #[serde(default)]
    pub custom_time_header: Option<String>,

    /// Optional static credentials for S3 SigV4 authentication.
    ///
    /// When set, the backend uses these credentials for signing requests
    /// instead of the default credential chain. When unset, falls back to
    /// anonymous (unauthenticated) requests.
    ///
    /// # Environment Variables
    ///
    /// - `OS__STORAGE__AUTH__ACCESS_KEY_ID=…`
    /// - `OS__STORAGE__AUTH__SECRET_ACCESS_KEY=…`
    #[serde(default)]
    pub auth: Option<AuthConfig>,
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

/// Static AWS-style credentials for S3 SigV4 authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// AWS access key ID (or equivalent for S3-compatible services).
    pub access_key_id: String,
    /// AWS secret access key (or equivalent for S3-compatible services).
    pub secret_access_key: SecretBox<ConfigSecret>,
}

/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

/// S3-compatible storage backend.
#[derive(Debug)]
pub struct S3CompatibleBackend {
    bucket: Box<Bucket>,
    metadata_prefix: String,
    protocol_prefix: String,
    custom_time_header: Option<String>,
    bearer_auth_provider: Option<Arc<PrefetchingTokenProvider>>,
}

/// Wraps [`Metadata::to_headers`], additionally echoing `time_expires` under
/// `custom_time_header` when both are set.
fn metadata_to_s3_headers(
    metadata: &Metadata,
    prefix: &str,
    custom_time_header: Option<&str>,
) -> Result<HeaderMap, objectstore_types::metadata::Error> {
    let mut headers = metadata.to_headers(prefix)?;
    if let (Some(name), Some(time)) = (custom_time_header, metadata.time_expires) {
        let formatted = humantime::format_rfc3339_seconds(time);
        let name = HeaderName::try_from(name)?;
        headers.append(name, formatted.to_string().parse()?);
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

impl S3CompatibleBackend {
    /// Creates a new S3-compatible backend bound to the given config.
    pub fn new(config: S3CompatibleConfig) -> anyhow::Result<Self> {
        Self::from_config(config)
    }

    /// Creates a new S3-compatible backend bound to the given config and bearer auth provider.
    pub fn new_with_bearer_auth(
        config: S3CompatibleConfig,
        bearer_auth_provider: Arc<PrefetchingTokenProvider>,
    ) -> anyhow::Result<Self> {
        let mut slf = Self::from_config(config)?;
        slf.bearer_auth_provider = Some(bearer_auth_provider);
        Ok(slf)
    }

    fn from_config(config: S3CompatibleConfig) -> anyhow::Result<Self> {
        let credentials = config
            .auth
            .map(|auth| {
                Credentials::new(
                    Some(&auth.access_key_id),
                    Some(auth.secret_access_key.expose_secret().as_str()),
                    None,
                    None,
                    None,
                )
            })
            .unwrap_or_else(Credentials::anonymous)?;

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
            metadata_prefix: config.metadata_prefix,
            protocol_prefix: config.protocol_prefix,
            custom_time_header: config.custom_time_header,
            bearer_auth_provider: None,
        })
    }

    /// Returns a bucket with a fresh `Authorization: Bearer` header when a
    /// bearer auth provider is configured.
    async fn authed_bucket(&self) -> Result<Box<Bucket>> {
        let Some(provider) = &self.bearer_auth_provider else {
            return Ok(self.bucket.clone());
        };
        let token = provider.token(&[]).await.map_err(|e| Error::Generic {
            context: "S3: failed to get bearer token".to_owned(),
            cause: Some(Box::new(e)),
        })?;
        let mut headers = self.bucket.extra_headers().clone();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", token.as_str()).parse().map_err(
                |e: header::InvalidHeaderValue| Error::Generic {
                    context: "S3: invalid bearer token value".to_owned(),
                    cause: Some(Box::new(e)),
                },
            )?,
        );
        self.bucket
            .with_extra_headers(headers)
            .map(Box::new)
            .map_err(|e| map_s3_error(e, "S3: failed to set auth headers"))
    }

    fn object_path(&self, id: &ObjectId) -> String {
        format!("/{}", id.as_storage_path())
    }

    /// If `metadata` has a [`TimeToIdle`] policy, bumps the recorded expiry
    /// via a metadata-only copy-in-place when it would otherwise fall outside
    /// the 1-day debounce window.
    ///
    /// [`TimeToIdle`]: objectstore_types::metadata::ExpirationPolicy::TimeToIdle
    async fn bump_tti_if_needed(
        &self,
        bucket: &Bucket,
        path: &str,
        metadata: &Metadata,
    ) -> Result<()> {
        let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy else {
            return Ok(());
        };

        // TODO: Inject the access time from the request.
        let access_time = SystemTime::now();

        let current_expiry = metadata.time_expires.unwrap_or(access_time);
        let new_expiry = access_time + tti;

        let bump_amount = new_expiry
            .duration_since(current_expiry)
            .unwrap_or_default();

        if bump_amount > TTI_DEBOUNCE {
            // TODO: Schedule into background persistently so this doesn't get lost on restarts.
            self.update_custom_time(bucket, path, metadata, new_expiry)
                .await?;
        }

        Ok(())
    }

    /// Rewrites the object's metadata in place via a copy-with-REPLACE
    /// request, setting `time_expires` (and the configured
    /// [`custom_time_header`], if any) to `custom_time`. Used to bump the
    /// expiration time for TTI objects.
    ///
    /// [`custom_time_header`]: S3CompatibleConfig::custom_time_header
    async fn update_custom_time(
        &self,
        bucket: &Bucket,
        path: &str,
        metadata: &Metadata,
        custom_time: SystemTime,
    ) -> Result<()> {
        let copy_source_header = format!("{}copy-source", self.protocol_prefix);
        let metadata_directive_header = format!("{}metadata-directive", self.protocol_prefix);
        let copy_source = format!("/{}{}", bucket.name(), path);

        let mut request = bucket
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

        let mut metadata = metadata.clone();
        metadata.time_expires = Some(custom_time);
        let headers = metadata_to_s3_headers(
            &metadata,
            &self.metadata_prefix,
            self.custom_time_header.as_deref(),
        )
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
            .map_err(|e| map_s3_error(e, "S3: failed to update custom_time"))?;

        Ok(())
    }

    /// HEADs an object, filters expired entries, and bumps TTI.
    ///
    /// Returns `None` if the object is absent or past its expiry.
    async fn fetch_live_metadata(&self, bucket: &Bucket, path: &str) -> Result<Option<Metadata>> {
        let Some(headers) = self.head_object(bucket, path).await? else {
            return Ok(None);
        };
        let metadata = metadata_from_headers(&headers, &self.metadata_prefix)?;

        // Filter already expired objects but leave them to garbage collection.
        if metadata.expiration_policy.is_timeout()
            && metadata
                .time_expires
                .is_some_and(|ts| ts < SystemTime::now())
        {
            objectstore_log::debug!("Object found but past expiry");
            return Ok(None);
        }

        self.bump_tti_if_needed(bucket, path, &metadata).await?;
        Ok(Some(metadata))
    }

    /// Issues a HEAD request and returns the raw response headers.
    ///
    /// `Bucket::head_object` is not sufficient because it only surfaces
    /// `x-amz-meta-*` keys, which could be different from `self.metadata_prefix`.
    async fn head_object(&self, bucket: &Bucket, path: &str) -> Result<Option<HeaderMap>> {
        let request = ReqwestRequest::new(bucket, path, Command::HeadObject)
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

#[async_trait::async_trait]
impl Backend for S3CompatibleBackend {
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
        let bucket = self.authed_bucket().await?;
        let path = self.object_path(id);

        let mut request = bucket
            .put_object_stream_builder(&path)
            .with_content_type(metadata.content_type.as_ref());

        if let Some(compression) = metadata.compression {
            request = request
                .with_content_encoding(compression.as_str())
                .map_err(|e| map_s3_error(e, "S3: failed to set content-encoding"))?;
        }

        let mut metadata = metadata.clone();
        if let Some(d) = metadata.expiration_policy.expires_in() {
            metadata.time_expires = Some(SystemTime::now() + d);
        }
        let headers = metadata_to_s3_headers(
            &metadata,
            &self.metadata_prefix,
            self.custom_time_header.as_deref(),
        )
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
        let bucket = self.authed_bucket().await?;
        let path = self.object_path(id);

        let Some(metadata) = self.fetch_live_metadata(&bucket, &path).await? else {
            return Ok(None);
        };

        let response = match bucket.get_object_stream(&path).await {
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
        let bucket = self.authed_bucket().await?;
        let path = self.object_path(id);
        self.fetch_live_metadata(&bucket, &path).await
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from s3_compatible backend");
        let bucket = self.authed_bucket().await?;
        let path = self.object_path(id);

        match bucket.delete_object(&path).await {
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

    fn create_test_backend() -> S3CompatibleBackend {
        S3CompatibleBackend::new(S3CompatibleConfig {
            endpoint: "http://localhost:8089".into(),
            bucket: "test-bucket".into(),
            region: default_region(),
            use_path_style: default_use_path_style(),
            metadata_prefix: default_metadata_prefix(),
            protocol_prefix: default_protocol_prefix(),
            custom_time_header: None,
            auth: None,
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
        let path = backend.object_path(&id);
        let old_deadline = SystemTime::now() + tti - TTI_DEBOUNCE - Duration::from_secs(60);
        backend
            .update_custom_time(&backend.bucket, &path, &metadata, old_deadline)
            .await?;

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
