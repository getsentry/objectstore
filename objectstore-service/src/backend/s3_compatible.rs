//! S3-compatible backend with generic protocol support.

use std::time::{Duration, SystemTime};
use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use reqwest::header::HeaderMap;
use reqwest::{Body, IntoUrl, Method, RequestBuilder, StatusCode};

use crate::backend::common::{
    self, Backend, DeleteResponse, GetResponse, MetadataResponse, PutResponse,
};
use crate::error::{Error, Result};
use crate::id::ObjectId;
use crate::stream::{self, ClientStream};

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
    client: reqwest::Client,

    endpoint: String,
    bucket: String,

    token_provider: Option<T>,
}

impl<T> S3CompatibleBackend<T> {
    /// Creates a new S3-compatible backend bound to the given bucket.
    pub fn new(endpoint: &str, bucket: &str, token_provider: T) -> Self {
        Self {
            client: common::reqwest_client(),
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            token_provider: Some(token_provider),
        }
    }

    /// Formats the S3 object URL for the given key.
    fn object_url(&self, id: &ObjectId) -> String {
        format!("{}/{}/{}", self.endpoint, self.bucket, id.as_storage_path())
    }
}

/// Wraps [`Metadata::to_headers`] with GCS-specific concerns (tombstone + custom-time).
fn metadata_to_gcs_headers(metadata: &Metadata, prefix: &str) -> Result<HeaderMap> {
    let mut headers = metadata
        .to_headers(prefix)
        .map_err(|e| Error::internal("S3: failed to serialize metadata headers", e))?;
    // GCS custom-time for lifecycle expiration
    if let Some(expires_in) = metadata.expiration_policy.expires_in() {
        let expires_at =
            humantime::format_rfc3339_seconds(std::time::SystemTime::now() + expires_in);
        headers.append(
            GCS_CUSTOM_TIME,
            expires_at
                .to_string()
                .parse()
                .map_err(|e| Error::internal("S3: invalid custom-time header value", e))?,
        );
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
                    .map_err(|err| {
                        Error::internal(
                            "S3: failed to get authentication token",
                            std::io::Error::other(err),
                        )
                    })?
                    .as_str(),
            );
        }
        Ok(builder)
    }

    /// Fetches object metadata using the given HTTP method (GET or HEAD),
    /// bumps TTI if needed, and returns the parsed metadata along with the
    /// response (so `get_object` can read the body from a GET).
    async fn request_object(
        &self,
        method: Method,
        id: &ObjectId,
    ) -> Result<Option<(Metadata, reqwest::Response)>> {
        let object_url = self.object_url(id);

        let response = self
            .request(method, &object_url)
            .await?
            .send()
            .await
            .map_err(|e| Error::internal("S3: failed to send request", e))?;

        if response.status() == StatusCode::NOT_FOUND {
            objectstore_log::debug!("Object not found");
            return Ok(None);
        }

        let response = response
            .error_for_status()
            .map_err(|e| Error::internal("S3: failed to get object", e))?;

        let headers = response.headers();
        let mut metadata = Metadata::from_headers(headers, GCS_CUSTOM_PREFIX)
            .map_err(|e| Error::internal("S3: failed to parse metadata from headers", e))?;
        metadata.size = response.content_length().map(|len| len as usize);

        // TODO: Schedule into background persistently so this doesn't get lost on restarts
        if let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy {
            // TODO: Inject the access time from the request.
            let access_time = SystemTime::now();

            let expire_at = headers
                .get(GCS_CUSTOM_TIME)
                .and_then(|s| s.to_str().ok())
                .and_then(|s| humantime::parse_rfc3339(s).ok())
                .unwrap_or(access_time);

            if expire_at < access_time + tti - TTI_DEBOUNCE {
                self.update_metadata(id, &metadata).await?;
            }
        }

        Ok(Some((metadata, response)))
    }

    /// Issues a request to update the metadata for the given object.
    async fn update_metadata(&self, id: &ObjectId, metadata: &Metadata) -> Result<()> {
        // NB: Meta updates require copy + REPLACE along with *all* metadata. See
        // https://cloud.google.com/storage/docs/xml-api/put-object-copy
        self.request(Method::PUT, self.object_url(id))
            .await?
            .header(
                "x-goog-copy-source",
                format!("/{}/{}", self.bucket, id.as_storage_path()),
            )
            .header("x-goog-metadata-directive", "REPLACE")
            .headers(metadata_to_gcs_headers(metadata, GCS_CUSTOM_PREFIX)?)
            .send()
            .await
            .map_err(|e| Error::internal("S3: failed to send TTI update request", e))?
            .error_for_status()
            .map_err(|e| {
                Error::internal(
                    "S3: failed to update expiration time for object with TTI",
                    e,
                )
            })?;

        Ok(())
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
    pub fn without_token(config: S3CompatibleConfig) -> Self {
        Self {
            client: common::reqwest_client(),
            endpoint: config.endpoint,
            bucket: config.bucket,
            token_provider: None,
        }
    }
}

#[async_trait::async_trait]
impl<T: TokenProvider> Backend for S3CompatibleBackend<T> {
    fn name(&self) -> &'static str {
        "s3-compatible"
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse> {
        objectstore_log::debug!("Writing to s3_compatible backend");
        self.request(Method::PUT, self.object_url(id))
            .await?
            .headers(metadata_to_gcs_headers(metadata, GCS_CUSTOM_PREFIX)?)
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .and_then(|response| response.error_for_status())
            .map_err(|e| match stream::unpack_client_error(&e) {
                Some(ce) => Error::client_stream(ce),
                _ => Error::internal("S3: failed to put object", e),
            })?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from s3_compatible backend");

        let Some((metadata, response)) = self.request_object(Method::GET, id).await? else {
            return Ok(None);
        };

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some((metadata, stream.boxed())))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        objectstore_log::debug!("Reading metadata from s3_compatible backend");
        let response = self.request_object(Method::HEAD, id).await?;
        Ok(response.map(|(metadata, _)| metadata))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse> {
        objectstore_log::debug!("Deleting from s3_compatible backend");
        let response = self
            .request(Method::DELETE, self.object_url(id))
            .await?
            .send()
            .await
            .map_err(|e| Error::internal("S3: failed to send delete request", e))?;

        // Do not error for objects that do not exist.
        if response.status() != StatusCode::NOT_FOUND {
            objectstore_log::debug!("Object not found");
            response
                .error_for_status()
                .map_err(|e| Error::internal("S3: failed to delete object", e))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::Backend;
    use crate::id::ObjectContext;

    // NB: To run these tests, you need to have a MinIO server running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up MinIO via devservices.

    fn create_test_backend() -> S3CompatibleBackend<NoToken> {
        S3CompatibleBackend::without_token(S3CompatibleConfig {
            endpoint: "http://localhost:8089".into(),
            bucket: "test-bucket".into(),
        })
    }

    fn make_id() -> ObjectId {
        ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        })
    }

    #[tokio::test]
    async fn test_get_metadata_nonexistent() -> Result<()> {
        let backend = create_test_backend();
        let id = make_id();
        let result = backend.get_metadata(&id).await?;
        assert!(result.is_none());
        Ok(())
    }
}
