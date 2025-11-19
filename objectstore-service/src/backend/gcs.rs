use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{fmt, io};

use anyhow::{Context, Result};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::{ExpirationPolicy, Metadata};
use reqwest::{Body, IntoUrl, Method, RequestBuilder, StatusCode, Url, header, multipart};
use serde::{Deserialize, Serialize};

use crate::ObjectPath;
use crate::backend::common::{self, Backend, BackendStream};

/// Default endpoint used to access the GCS JSON API.
const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";
/// Permission scopes required for accessing GCS.
const TOKEN_SCOPES: &[&str] = &["https://www.googleapis.com/auth/devstorage.read_write"];
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

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

        if let Some(time_created) = metadata.time_created {
            gcs_object.metadata.insert(
                GcsMetaKey::TimeCreated,
                humantime::format_rfc3339_micros(time_created).to_string(),
            );
        }

        for (key, value) in &metadata.custom {
            gcs_object
                .metadata
                .insert(GcsMetaKey::Custom(key.clone()), value.clone());
        }

        gcs_object
    }

    /// Converts GCS JSON object metadata to our Metadata type.
    pub fn into_metadata(mut self) -> Result<Metadata> {
        // Remove ignored metadata keys that are set by the GCS emulator.
        self.metadata.remove(&GcsMetaKey::EmulatorIgnored);

        let expiration_policy = self
            .metadata
            .remove(&GcsMetaKey::Expiration)
            .map(|s| s.parse())
            .transpose()?
            .unwrap_or_default();

        let time_created = self
            .metadata
            .remove(&GcsMetaKey::TimeCreated)
            .map(|s| humantime::parse_rfc3339(&s))
            .transpose()?;

        let content_type = self.content_type;
        let compression = self.content_encoding.map(|s| s.parse()).transpose()?;
        let size = self.size.map(|size| size.parse()).transpose()?;

        // At this point, all built-in metadata should have been removed from self.metadata.
        let mut custom = BTreeMap::new();
        for (key, value) in self.metadata {
            if let GcsMetaKey::Custom(custom_key) = key {
                custom.insert(custom_key, value);
            } else {
                anyhow::bail!("unexpected metadata");
            }
        }

        Ok(Metadata {
            // while "in theory" GCS could be the "high-volume" backend that stores tombstones,
            // in practice it is not.
            is_redirect_tombstone: None,
            content_type,
            expiration_policy,
            time_created,
            compression,
            custom,
            size,
        })
    }
}

/// Key for [`GcsObject::metadata`].
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
enum GcsMetaKey {
    /// Built-in metadata key for [`Metadata::expiration_policy`].
    Expiration,
    /// Built-in metadata key for [`Metadata::time_created`].
    TimeCreated,
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
            Some("time-created") => GcsMetaKey::TimeCreated,
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
            Self::TimeCreated => write!(f, "{BUILTIN_META_PREFIX}time-created"),
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
    fn object_url(&self, path: &ObjectPath) -> Result<Url> {
        let mut url = self.endpoint.clone();

        let path = path.to_string();
        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("invalid GCS endpoint path"))?
            .extend(&["storage", "v1", "b", &self.bucket, "o", &path]);

        Ok(url)
    }

    /// Formats the GCS upload URL for the given upload type.
    fn upload_url(&self, path: &ObjectPath, upload_type: &str) -> Result<Url> {
        let mut url = self.endpoint.clone();

        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("invalid GCS endpoint path"))?
            .extend(&["upload", "storage", "v1", "b", &self.bucket, "o"]);

        url.query_pairs_mut()
            .append_pair("uploadType", upload_type)
            .append_pair("name", &path.to_string());

        Ok(url)
    }

    /// Creates a request builder with the appropriate authentication.
    async fn request(&self, method: Method, url: impl IntoUrl) -> Result<RequestBuilder> {
        let mut builder = self.client.request(method, url);
        if let Some(provider) = &self.token_provider {
            let token = provider.token(TOKEN_SCOPES).await?;
            builder = builder.bearer_auth(token.as_str());
        }
        Ok(builder)
    }

    async fn update_custom_time(&self, object_url: Url, custom_time: SystemTime) -> Result<()> {
        #[derive(Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CustomTimeRequest {
            #[serde(with = "humantime_serde")]
            custom_time: SystemTime,
        }

        self.request(Method::PATCH, object_url)
            .await?
            .json(&CustomTimeRequest { custom_time })
            .send()
            .await?
            .error_for_status()
            .context("failed to update expiration time for object with TTI")?;

        Ok(())
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

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn put_object(
        &self,
        path: &ObjectPath,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> Result<()> {
        tracing::debug!("Writing to GCS backend");
        let gcs_metadata = GcsObject::from_metadata(metadata);

        // NB: Ensure the order of these fields and that a content-type is attached to them. Both
        // are required by the GCS API.
        let multipart = multipart::Form::new()
            .part(
                "metadata",
                multipart::Part::text(serde_json::to_string(&gcs_metadata)?)
                    .mime_str("application/json")?,
            )
            .part(
                "media",
                multipart::Part::stream(Body::wrap_stream(stream))
                    .mime_str(&metadata.content_type)?,
            );

        // GCS requires a multipart/related request. Its body looks identical to
        // multipart/form-data, but the Content-Type header is different. Hence, we have to manually
        // set the header *after* writing the multipart form into the request.
        let content_type = format!("multipart/related; boundary={}", multipart.boundary());

        self.request(Method::POST, self.upload_url(path, "multipart")?)
            .await?
            .multipart(multipart)
            .header(header::CONTENT_TYPE, content_type)
            .send()
            .await?
            .error_for_status()
            .context("failed to upload object via multipart")?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn get_object(&self, path: &ObjectPath) -> Result<Option<(Metadata, BackendStream)>> {
        tracing::debug!("Reading from GCS backend");
        let object_url = self.object_url(path)?;
        let metadata_response = self
            .request(Method::GET, object_url.clone())
            .await?
            .send()
            .await?;

        if metadata_response.status() == StatusCode::NOT_FOUND {
            tracing::debug!("Object not found");
            return Ok(None);
        }

        let metadata_response = metadata_response
            .error_for_status()
            .context("failed to get object metadata")?;

        let gcs_metadata: GcsObject = metadata_response
            .json()
            .await
            .context("failed to parse object metadata")?;

        // TODO: Store custom_time directly in metadata.
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
            // Only bump if the difference in deadlines meets a minimum threshold
            let new_expire_at = SystemTime::now() + tti;
            if expire_at.is_some_and(|ts| ts < new_expire_at - TTI_DEBOUNCE) {
                self.update_custom_time(object_url.clone(), new_expire_at)
                    .await?;
            }
        }

        let mut download_url = object_url;
        download_url.query_pairs_mut().append_pair("alt", "media");
        let payload_response = self
            .request(Method::GET, download_url)
            .await?
            .send()
            .await?
            .error_for_status()
            .context("failed to get object payload")?;

        let stream = payload_response
            .bytes_stream()
            .map_err(io::Error::other)
            .boxed();

        Ok(Some((metadata, stream)))
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn delete_object(&self, path: &ObjectPath) -> Result<()> {
        tracing::debug!("Deleting from GCS backend");
        let response = self
            .request(Method::DELETE, self.object_url(path)?)
            .await?
            .send()
            .await?;

        // Do not error for objects that do not exist
        if response.status() != StatusCode::NOT_FOUND {
            tracing::debug!("Object not found");
            response
                .error_for_status()
                .context("failed to delete object")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use super::*;

    // NB: Not run any of these tests, you need to have a GCS emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

    async fn create_test_backend() -> Result<GcsBackend> {
        GcsBackend::new(Some("http://localhost:8087"), "test-bucket").await
    }

    fn make_stream(contents: &[u8]) -> BackendStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    async fn read_to_vec(mut stream: BackendStream) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            payload.extend(&chunk);
        }
        Ok(payload)
    }

    fn make_key() -> ObjectPath {
        ObjectPath {
            usecase: "testing".into(),
            scope: vec!["testing".into()],
            key: Uuid::new_v4().to_string(),
        }
    }

    #[tokio::test]
    async fn test_roundtrip() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            is_redirect_tombstone: None,
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::Manual,
            time_created: Some(SystemTime::now()),
            compression: None,
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            size: None,
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let (meta, stream) = backend.get_object(&path).await?.unwrap();

        let payload = read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "hello, world");
        assert_eq!(meta.content_type, metadata.content_type);
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_nonexistent() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        backend.delete_object(&path).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            custom: BTreeMap::from_iter([("invalid".into(), "invalid".into())]),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello"))
            .await?;

        let metadata = Metadata {
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"world"))
            .await?;

        let (meta, stream) = backend.get_object(&path).await?.unwrap();

        let payload = read_to_vec(stream).await?;
        let str_payload = str::from_utf8(&payload).unwrap();
        assert_eq!(str_payload, "world");
        assert_eq!(meta.custom, metadata.custom);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> Result<()> {
        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata::default();

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        backend.delete_object(&path).await?;

        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_immediate() -> Result<()> {
        // NB: We create a TTL that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tti_immediate() -> Result<()> {
        // NB: We create a TTI that immediately expires in this tests. This might be optimized away
        // in a future implementation, so we will have to update this test accordingly.

        let backend = create_test_backend().await?;

        let path = make_key();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(0)),
            ..Default::default()
        };

        backend
            .put_object(&path, &metadata, make_stream(b"hello, world"))
            .await?;

        let result = backend.get_object(&path).await?;
        assert!(result.is_none());

        Ok(())
    }
}
