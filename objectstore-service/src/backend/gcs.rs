use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{fmt, io};

use anyhow::{Context, Result};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use reqwest::{Body, IntoUrl, Method, RequestBuilder, StatusCode, Url, header, multipart};
use serde::{Deserialize, Serialize};

use crate::backend::common::{
    self, Backend, DeleteResponse, GetResponse, MetadataResponse, PutResponse,
};
use crate::id::ObjectId;
use crate::{PayloadStream, ServiceError, ServiceResult};

/// Default endpoint used to access the GCS JSON API.
const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";
/// Permission scopes required for accessing GCS.
const TOKEN_SCOPES: &[&str] = &["https://www.googleapis.com/auth/devstorage.read_write"];
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day
/// How many times to retry failed operations.
const REQUEST_RETRY_COUNT: usize = 2;

/// Prefix for our built-in metadata stored in GCS metadata field
const BUILTIN_META_PREFIX: &str = "x-sn-";
/// Prefix for user custom metadata stored in GCS metadata field
const CUSTOM_META_PREFIX: &str = "x-snme-";

/// GCS object resource.
///
/// This is the representation of the object resource in GCS JSON API without its payload. Where no
/// dedicated fields are available, we encode both built-in and custom metadata in the `metadata`
/// field.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsObject {
    /// Content-Type of the object data. If an object is stored without a Content-Type, it is served
    /// as application/octet-stream.
    pub content_type: Cow<'static, str>,

    /// Content encoding, used to store [`Metadata::compression`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,

    /// Custom time stamp used for time-based expiration.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde"
    )]
    pub custom_time: Option<SystemTime>,

    /// The `Content-Length` of the data in bytes. GCS returns this as a string.
    ///
    /// GCS sets this in metadata responses. We can use it to know the size of an object
    /// without having to stream it.
    pub size: Option<String>,

    /// Timestamp of when this object was created.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde"
    )]
    pub time_created: Option<SystemTime>,

    /// User-provided metadata, including our built-in metadata.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<GcsMetaKey, String>,
}

impl GcsObject {
    /// Converts our Metadata type to GCS JSON object metadata.
    pub fn from_metadata(metadata: &Metadata) -> Self {
        let mut gcs_object = GcsObject {
            content_type: metadata.content_type.clone(),
            size: metadata.size.map(|size| size.to_string()),
            content_encoding: None,
            custom_time: None,
            time_created: metadata.time_created,
            metadata: BTreeMap::new(),
        };

        // For time-based expiration, set the `customTime` field. The bucket must have a
        // `daysSinceCustomTime` lifecycle rule configured to delete objects with this field set.
        // This rule automatically skips objects without `customTime` set.
        if let Some(expires_in) = metadata.expiration_policy.expires_in() {
            gcs_object.custom_time = Some(SystemTime::now() + expires_in);
        }

        if let Some(compression) = metadata.compression {
            gcs_object.content_encoding = Some(compression.to_string());
        }

        if metadata.expiration_policy != ExpirationPolicy::default() {
            gcs_object.metadata.insert(
                GcsMetaKey::Expiration,
                metadata.expiration_policy.to_string(),
            );
        }

        if let Some(origin) = &metadata.origin {
            gcs_object
                .metadata
                .insert(GcsMetaKey::Origin, origin.clone());
        }

        for (key, value) in &metadata.custom {
            gcs_object
                .metadata
                .insert(GcsMetaKey::Custom(key.clone()), value.clone());
        }

        gcs_object
    }

    /// Converts GCS JSON object metadata to our Metadata type.
    pub fn into_metadata(mut self) -> ServiceResult<Metadata> {
        // Remove ignored metadata keys that are set by the GCS emulator.
        self.metadata.remove(&GcsMetaKey::EmulatorIgnored);

        let expiration_policy = self
            .metadata
            .remove(&GcsMetaKey::Expiration)
            .map(|s| s.parse())
            .transpose()?
            .unwrap_or_default();

        let origin = self.metadata.remove(&GcsMetaKey::Origin);

        let content_type = self.content_type;
        let compression = self.content_encoding.map(|s| s.parse()).transpose()?;
        let size = self
            .size
            .map(|size| size.parse())
            .transpose()
            .map_err(|e| ServiceError::Generic {
                context: "GCS: failed to parse size from object metadata".to_string(),
                cause: Some(Box::new(e)),
            })?;
        let time_created = self.time_created;

        // At this point, all built-in metadata should have been removed from self.metadata.
        let mut custom = BTreeMap::new();
        for (key, value) in self.metadata {
            if let GcsMetaKey::Custom(custom_key) = key {
                custom.insert(custom_key, value);
            } else {
                return Err(ServiceError::Generic {
                    context: format!(
                        "GCS: unexpected built-in metadata key in object metadata: {}",
                        key
                    ),
                    cause: None,
                });
            }
        }

        Ok(Metadata {
            // while "in theory" GCS could be the "high-volume" backend that stores tombstones,
            // in practice it is not.
            is_redirect_tombstone: None,
            content_type,
            expiration_policy,
            compression,
            origin,
            size,
            custom,
            time_created,
            time_expires: self.custom_time,
        })
    }
}

/// Key for [`GcsObject::metadata`].
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
enum GcsMetaKey {
    /// Built-in metadata key for [`Metadata::expiration_policy`].
    Expiration,
    /// Built-in metadata key for [`Metadata::origin`].
    Origin,
    /// Ignored metadata set by the GCS emulator.
    EmulatorIgnored,
    /// User-defined custom metadata key.
    Custom(String),
}

impl std::str::FromStr for GcsMetaKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if matches!(s, "x_emulator_upload" | "x_testbench_upload") {
            return Ok(GcsMetaKey::EmulatorIgnored);
        }

        Ok(match s.strip_prefix(BUILTIN_META_PREFIX) {
            Some("expiration") => GcsMetaKey::Expiration,
            Some("origin") => GcsMetaKey::Origin,
            Some(unknown) => anyhow::bail!("unknown builtin metadata key: {unknown}"),
            None => match s.strip_prefix(CUSTOM_META_PREFIX) {
                Some(key) => GcsMetaKey::Custom(key.to_string()),
                None => anyhow::bail!("invalid GCS metadata key format: {s}"),
            },
        })
    }
}

impl fmt::Display for GcsMetaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expiration => write!(f, "{BUILTIN_META_PREFIX}expiration"),
            Self::Origin => write!(f, "{BUILTIN_META_PREFIX}origin"),
            Self::EmulatorIgnored => unreachable!("do not serialize emulator metadata"),
            Self::Custom(key) => write!(f, "{CUSTOM_META_PREFIX}{key}"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for GcsMetaKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = Cow::<'de, str>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for GcsMetaKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

/// Returns `true` if the error is a transient reqwest failure worth retrying.
fn is_retryable(error: &ServiceError) -> bool {
    let ServiceError::Reqwest { cause, .. } = error else {
        return false;
    };
    if cause.is_timeout() || cause.is_connect() || cause.is_request() {
        return true;
    }
    let Some(status) = cause.status() else {
        return false;
    };
    // https://docs.cloud.google.com/storage/docs/json_api/v1/status-codes
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

pub struct GcsBackend {
    client: reqwest::Client,
    endpoint: Url,
    bucket: String,
    token_provider: Option<Arc<dyn gcp_auth::TokenProvider>>,
}

impl GcsBackend {
    /// Creates an authenticated GCS JSON API backend bound to the given bucket.
    pub async fn new(endpoint: Option<&str>, bucket: &str) -> Result<Self> {
        let (endpoint, token_provider) = match endpoint {
            Some(emulator_host) => (emulator_host, None),
            None => (DEFAULT_ENDPOINT, Some(gcp_auth::provider().await?)),
        };

        Ok(Self {
            client: common::reqwest_client(),
            endpoint: endpoint.parse().context("invalid GCS endpoint URL")?,
            bucket: bucket.to_string(),
            token_provider,
        })
    }

    /// Formats the GCS object (metadata) URL for the given key.
    fn object_url(&self, id: &ObjectId) -> ServiceResult<Url> {
        let mut url = self.endpoint.clone();

        let path = id.as_storage_path().to_string();
        url.path_segments_mut()
            .map_err(|()| ServiceError::Generic {
                context: format!(
                    "GCS: invalid endpoint URL, {} cannot be a base",
                    self.endpoint
                ),
                cause: None,
            })?
            .extend(&["storage", "v1", "b", &self.bucket, "o", &path]);

        Ok(url)
    }

    /// Formats the GCS upload URL for the given upload type.
    fn upload_url(&self, id: &ObjectId, upload_type: &str) -> ServiceResult<Url> {
        let mut url = self.endpoint.clone();

        url.path_segments_mut()
            .map_err(|()| ServiceError::Generic {
                context: format!(
                    "GCS: invalid endpoint URL, {} cannot be a base",
                    self.endpoint
                ),
                cause: None,
            })?
            .extend(&["upload", "storage", "v1", "b", &self.bucket, "o"]);

        url.query_pairs_mut()
            .append_pair("uploadType", upload_type)
            .append_pair("name", &id.as_storage_path().to_string());

        Ok(url)
    }

    /// Creates a request builder with the appropriate authentication.
    async fn request(&self, method: Method, url: impl IntoUrl) -> ServiceResult<RequestBuilder> {
        let mut builder = self.client.request(method, url);
        if let Some(provider) = &self.token_provider {
            let token = provider.token(TOKEN_SCOPES).await?;
            builder = builder.bearer_auth(token.as_str());
        }
        Ok(builder)
    }

    /// Retries a GCS request on transient errors.
    async fn with_retry<T, F>(&self, action: &str, f: impl Fn() -> F) -> ServiceResult<T>
    where
        F: Future<Output = ServiceResult<T>> + Send,
    {
        let mut retry_count = 0usize;
        loop {
            match f().await {
                Ok(res) => return Ok(res),
                Err(ref e) if retry_count < REQUEST_RETRY_COUNT && is_retryable(e) => {
                    retry_count += 1;
                    merni::counter!("gcs.retries": 1, "action" => action);
                    tracing::debug!(
                        retry_count,
                        action,
                        error = e as &dyn std::error::Error,
                        "Retrying request"
                    );
                }
                Err(e) => {
                    merni::counter!("gcs.failures": 1, "action" => action);
                    return Err(e);
                }
            }
        }
    }

    /// Fetches the GCS object metadata (without the payload), bumps TTI if
    /// needed, and returns the parsed [`Metadata`].
    async fn fetch_gcs_metadata(&self, object_url: &Url) -> ServiceResult<Option<Metadata>> {
        let metadata_opt = self
            .with_retry("get_metadata", || async {
                let resp = self
                    .request(Method::GET, object_url.clone())
                    .await?
                    .send()
                    .await
                    .map_err(|e| ServiceError::reqwest("GCS: get metadata request", e))?;

                if resp.status() == StatusCode::NOT_FOUND {
                    return Ok(None);
                }

                let metadata: GcsObject = resp
                    .error_for_status()
                    .map_err(|e| ServiceError::reqwest("GCS: get metadata status", e))?
                    .json()
                    .await
                    .map_err(|e| ServiceError::reqwest("GCS: get metadata parse", e))?;

                Ok(Some(metadata))
            })
            .await?;

        let Some(gcs_metadata) = metadata_opt else {
            tracing::debug!("Object not found");
            return Ok(None);
        };

        let expire_at = gcs_metadata.custom_time;
        let metadata = gcs_metadata.into_metadata()?;

        // TODO: Inject the access time from the request.
        let access_time = SystemTime::now();

        // Filter already expired objects but leave them to garbage collection
        if metadata.expiration_policy.is_timeout() && expire_at.is_some_and(|ts| ts < access_time) {
            tracing::debug!("Object found but past expiry");
            return Ok(None);
        }

        // TODO: Schedule into background persistently so this doesn't get lost on restarts
        if let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy {
            let new_expire_at = access_time + tti;
            if expire_at.is_some_and(|ts| ts < new_expire_at - TTI_DEBOUNCE) {
                self.update_custom_time(object_url.clone(), new_expire_at)
                    .await?;
            }
        }

        Ok(Some(metadata))
    }

    async fn update_custom_time(
        &self,
        object_url: Url,
        custom_time: SystemTime,
    ) -> ServiceResult<()> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CustomTimeRequest {
            #[serde(with = "humantime_serde")]
            custom_time: SystemTime,
        }

        self.with_retry("update_custom_time", || async {
            self.request(Method::PATCH, object_url.clone())
                .await?
                .json(&CustomTimeRequest { custom_time })
                .send()
                .await
                .and_then(|r| r.error_for_status())
                .map_err(|e| ServiceError::reqwest("GCS: update custom time", e))?;
            Ok(())
        })
        .await
    }
}

impl fmt::Debug for GcsBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcsJsonApi")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Backend for GcsBackend {
    fn name(&self) -> &'static str {
        "gcs"
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: PayloadStream,
    ) -> ServiceResult<PutResponse> {
        tracing::debug!("Writing to GCS backend");
        let gcs_metadata = GcsObject::from_metadata(metadata);

        // NB: Ensure the order of these fields and that a content-type is attached to them. Both
        // are required by the GCS API.
        let metadata_json =
            serde_json::to_string(&gcs_metadata).map_err(|cause| ServiceError::Serde {
                context: "failed to serialize metadata for GCS upload".to_string(),
                cause,
            })?;

        let multipart = multipart::Form::new()
            .part(
                "metadata",
                multipart::Part::text(metadata_json)
                    .mime_str("application/json")
                    .expect("application/json is a valid mime type"),
            )
            .part(
                "media",
                multipart::Part::stream(Body::wrap_stream(stream))
                    .mime_str(&metadata.content_type)
                    .map_err(|e| ServiceError::Generic {
                        context: format!("invalid mime type: {}", &metadata.content_type),
                        cause: Some(Box::new(e)),
                    })?,
            );

        // GCS requires a multipart/related request. Its body looks identical to
        // multipart/form-data, but the Content-Type header is different. Hence, we have to manually
        // set the header *after* writing the multipart form into the request.
        let content_type = format!("multipart/related; boundary={}", multipart.boundary());

        self.request(Method::POST, self.upload_url(id, "multipart")?)
            .await?
            .multipart(multipart)
            .header(header::CONTENT_TYPE, content_type)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| ServiceError::reqwest("GCS: upload object", e))?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> ServiceResult<GetResponse> {
        tracing::debug!("Reading from GCS backend");
        let object_url = self.object_url(id)?;

        let Some(metadata) = self.fetch_gcs_metadata(&object_url).await? else {
            return Ok(None);
        };

        let mut download_url = object_url;
        download_url.query_pairs_mut().append_pair("alt", "media");

        let payload_response = self
            .with_retry("get_payload", || async {
                self.request(Method::GET, download_url.clone())
                    .await?
                    .send()
                    .await
                    .and_then(|r| r.error_for_status())
                    .map_err(|e| ServiceError::reqwest("GCS: get payload", e))
            })
            .await?;

        let stream = payload_response
            .bytes_stream()
            .map_err(io::Error::other)
            .boxed();

        Ok(Some((metadata, stream)))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_metadata(&self, id: &ObjectId) -> ServiceResult<MetadataResponse> {
        tracing::debug!("Reading metadata from GCS backend");
        let object_url = self.object_url(id)?;
        self.fetch_gcs_metadata(&object_url).await
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> ServiceResult<DeleteResponse> {
        tracing::debug!("Deleting from GCS backend");
        let object_url = self.object_url(id)?;

        self.with_retry("delete", || async {
            let resp = self
                .request(Method::DELETE, object_url.clone())
                .await?
                .send()
                .await
                .map_err(|e| ServiceError::reqwest("GCS: delete object", e))?;

            // Do not error for objects that do not exist
            if resp.status() == StatusCode::NOT_FOUND {
                return Ok(());
            }

            resp.error_for_status()
                .map_err(|e| ServiceError::reqwest("GCS: delete object", e))?;

            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::id::ObjectContext;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;

    // NB: Not run any of these tests, you need to have a GCS emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

    async fn create_test_backend() -> Result<GcsBackend> {
        GcsBackend::new(Some("http://localhost:8087"), "test-bucket").await
    }

    fn make_stream(contents: &[u8]) -> PayloadStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    async fn read_to_vec(mut stream: PayloadStream) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            payload.extend(&chunk);
        }
        Ok(payload)
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            is_redirect_tombstone: None,
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
            .put_object(&id, &metadata, make_stream(b"hello, world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = read_to_vec(stream).await?;
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
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        backend.delete_object(&id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, make_stream(b"hello"))
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, make_stream(b"world"))
            .await?;

        let (meta, stream) = backend.get_object(&id).await?.unwrap();

        let payload = read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "world");
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata::default();

        backend
            .put_object(&id, &metadata, make_stream(b"hello, world"))
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

        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        // NB: We create a TTI that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.get_object(&id).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_returns_metadata() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let metadata = Metadata {
            content_type: "text/plain".into(),
            origin: Some("203.0.113.42".into()),
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&id, &metadata, make_stream(b"hello, world"))
            .await?;

        let meta = backend.get_metadata(&id).await?.unwrap();
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.origin, metadata.origin);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_metadata_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let id = make_id();
        let result = backend.get_metadata(&id).await?;
        assert!(result.is_none());

        Ok(())
    }
}
