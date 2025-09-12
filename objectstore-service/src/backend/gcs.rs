use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::{ExpirationPolicy, Metadata};
use reqwest::{Body, IntoUrl, Method, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::backend::{Backend, BackendStream};
use crate::metadata::ScopedKey;

const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";
const TOKEN_SCOPES: &[&str] = &["https://www.googleapis.com/auth/devstorage.read_write"];
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

/// Prefix for our built-in metadata stored in GCS metadata field
const BUILTIN_META_PREFIX: &str = "x-sn-";
/// Prefix for user custom metadata stored in GCS metadata field  
const CUSTOM_META_PREFIX: &str = "x-snme-";
/// Key for storing expiration policy in GCS metadata
const EXPIRATION_META_KEY: &str = "x-sn-expiration";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsMetadata {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_time: Option<String>,
}

impl GcsMetadata {
    /// Converts our Metadata type to GCS JSON object metadata.
    pub fn from_metadata(key: &ScopedKey, metadata: &Metadata) -> Self {
        let custom_time = if let Some(expires_in) = metadata.expiration_policy.expires_in() {
            let expires_at = SystemTime::now() + expires_in;
            Some(humantime::format_rfc3339_seconds(expires_at).to_string())
        } else {
            None
        };

        // Build GCS metadata combining built-in and custom metadata
        let mut gcs_metadata = BTreeMap::new();
        
        // Add expiration policy if not manual
        if metadata.expiration_policy != ExpirationPolicy::Manual {
            gcs_metadata.insert(
                EXPIRATION_META_KEY.to_string(),
                metadata.expiration_policy.to_string(),
            );
        }
        
        // Add user custom metadata with prefix
        for (key, value) in &metadata.custom {
            gcs_metadata.insert(
                format!("{}{}", CUSTOM_META_PREFIX, key),
                value.clone(),
            );
        }

        Self {
            name: key.as_path().to_string(), // TODO: Replace with object name.
            metadata: if gcs_metadata.is_empty() {
                None
            } else {
                Some(gcs_metadata)
            },
            content_encoding: metadata.compression.map(|c| c.as_str().to_string()),
            custom_time,
        }
    }

    /// Converts GCS JSON object metadata to our Metadata type.
    pub fn to_metadata(&self) -> Result<Metadata> {
        let empty_map = BTreeMap::new();
        let gcs_metadata = self.metadata.as_ref().unwrap_or(&empty_map);

        let compression = if let Some(ref content_encoding) = self.content_encoding {
            Some(content_encoding.parse()?)
        } else {
            None
        };

        let expiration_policy = if let Some(expiration_str) = gcs_metadata.get(EXPIRATION_META_KEY) {
            expiration_str.parse()?
        } else {
            ExpirationPolicy::Manual
        };

        // Extract user custom metadata by removing our prefixes
        let mut custom = BTreeMap::new();
        for (key, value) in gcs_metadata {
            if let Some(custom_key) = key.strip_prefix(CUSTOM_META_PREFIX) {
                custom.insert(custom_key.to_string(), value.clone());
            }
            // Skip built-in metadata (with BUILTIN_META_PREFIX)
        }

        Ok(Metadata {
            expiration_policy,
            compression,
            custom,
        })
    }
}

pub struct GcsJsonApi {
    client: reqwest::Client,
    endpoint: String,
    bucket: String,
    token_provider: Option<Arc<dyn gcp_auth::TokenProvider>>,
}

impl GcsJsonApi {
    /// Creates a new GCS JSON API backend bound to the given bucket.
    pub async fn new(endpoint: Option<&str>, bucket: &str) -> Result<Self> {
        let (endpoint, token_provider) = match std::env::var("GCS_EMULATOR_HOST").ok() {
            Some(emulator_host) => (emulator_host, None),
            None => (
                endpoint.unwrap_or(DEFAULT_ENDPOINT).to_owned(),
                Some(gcp_auth::provider().await?),
            ),
        };

        Ok(Self {
            client: reqwest::Client::new(),
            endpoint,
            bucket: bucket.to_string(),
            token_provider,
        })
    }

    /// Creates a new GCS JSON API backend without authentication.
    pub fn without_token(endpoint: &str, bucket: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.to_owned(),
            bucket: bucket.to_string(),
            token_provider: None,
        }
    }

    /// Formats the GCS object URL for the given key.
    /// If `media` is true, returns URL for downloading object data.
    /// If `media` is false, returns URL for accessing object metadata.
    fn object_url(&self, key: &ScopedKey, media: bool) -> String {
        let base_url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            urlencoding::encode(&key.as_path().to_string())
        );
        
        if media {
            format!("{}?alt=media", base_url)
        } else {
            base_url
        }
    }

    /// Formats the GCS upload URL for the given upload type.
    fn upload_url(&self, upload_type: &str) -> String {
        format!(
            "{}/upload/storage/v1/b/{}/o?uploadType={}",
            self.endpoint, self.bucket, upload_type
        )
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

    /// Updates metadata for TTI bumping.
    async fn update_metadata(&self, key: &ScopedKey, metadata: &Metadata) -> Result<()> {
        let update_data = json!({
            "customTime": humantime::format_rfc3339_seconds(SystemTime::now() + metadata.expiration_policy.expires_in().unwrap_or_default()).to_string()
        });

        self.request(Method::PATCH, self.object_url(key, false))
            .await?
            .header("Content-Type", "application/json")
            .json(&update_data)
            .send()
            .await?
            .error_for_status()
            .context("failed to update object metadata")?;

        Ok(())
    }
}

impl std::fmt::Debug for GcsJsonApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsJsonApi")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Backend for GcsJsonApi {
    async fn put_object(
        &self,
        key: &ScopedKey,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> Result<()> {
        let gcs_metadata = GcsMetadata::from_metadata(key, metadata);

        let form = reqwest::multipart::Form::new()
            .text("metadata", serde_json::to_string(&gcs_metadata)?)
            .part(
                "media",
                reqwest::multipart::Part::stream(Body::wrap_stream(stream)),
            );

        self.request(Method::POST, self.upload_url("multipart"))
            .await?
            .multipart(form)
            .send()
            .await?
            .error_for_status()
            .context("failed to upload object via multipart")?;

        Ok(())
    }

    async fn get_object(&self, key: &ScopedKey) -> Result<Option<(Metadata, BackendStream)>> {
        // First, get object metadata
        let metadata_response = self
            .request(Method::GET, self.object_url(key, false))
            .await?
            .send()
            .await?;

        if metadata_response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let metadata_response = metadata_response
            .error_for_status()
            .context("failed to get object metadata")?;

        let gcs_metadata: GcsMetadata = metadata_response
            .json()
            .await
            .context("failed to parse object metadata")?;

        let metadata = gcs_metadata.to_metadata()?;

        // Handle TTI bumping
        if let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy
            && let Some(custom_time_str) = &gcs_metadata.custom_time
            && let Ok(expire_at) = humantime::parse_rfc3339(custom_time_str)
        {
            let access_time = SystemTime::now();
            if expire_at < access_time + tti - TTI_DEBOUNCE {
                self.update_metadata(key, &metadata).await?;
            }
        }

        // Get object data
        let payload_response = self
            .request(Method::GET, self.object_url(key, true))
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

    async fn delete_object(&self, key: &ScopedKey) -> Result<()> {
        let response = self
            .request(Method::DELETE, self.object_url(key, false))
            .await?
            .send()
            .await?;

        // Do not error for objects that do not exist
        if response.status() != StatusCode::NOT_FOUND {
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

    use objectstore_types::Scope;

    use super::*;
    use crate::ObjectKey;

    fn create_test_backend() -> GcsJsonApi {
        GcsJsonApi::without_token("http://localhost:8087", "test-bucket")
    }

    fn make_key() -> ScopedKey {
        ScopedKey {
            usecase: "testing".into(),
            scope: Scope {
                organization: 1234,
                project: Some(1234),
            },
            key: ObjectKey::for_backend(0),
        }
    }

    #[tokio::test]
    async fn test_metadata_conversion() {
        let key = make_key();
        let metadata = Metadata {
            expiration_policy: ExpirationPolicy::Manual,
            compression: None,
            custom: BTreeMap::from_iter([("hello".into(), "world".into())]),
        };

        let gcs_metadata = GcsMetadata::from_metadata(&key, &metadata);
        assert_eq!(gcs_metadata.name, key.as_path().to_string());
        assert_eq!(gcs_metadata.metadata.as_ref().unwrap()["hello"], "world");

        let converted_back = gcs_metadata.to_metadata().unwrap();
        assert_eq!(converted_back.custom, metadata.custom);
        assert_eq!(converted_back.compression, metadata.compression);
        assert_eq!(converted_back.expiration_policy, metadata.expiration_policy);
    }
}
