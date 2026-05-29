//! S3-compatible backend with generic protocol support.

use std::time::{Duration, SystemTime};
use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::{ExpirationPolicy, Metadata};
use objectstore_types::range::{ByteRange, ContentRange};
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
fn metadata_to_gcs_headers(
    metadata: &Metadata,
    prefix: &str,
) -> Result<HeaderMap, objectstore_types::metadata::Error> {
    let mut headers = metadata.to_headers(prefix)?;
    // GCS custom-time for lifecycle expiration
    if let Some(expires_in) = metadata.expiration_policy.expires_in() {
        let expires_at =
            humantime::format_rfc3339_seconds(std::time::SystemTime::now() + expires_in);
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
                    .map_err(|err| Error::Generic {
                        context: "S3: failed to get authentication token".to_owned(),
                        cause: Some(err.into()),
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
        range: Option<ByteRange>,
    ) -> Result<Option<(Metadata, reqwest::Response)>> {
        let object_url = self.object_url(id);

        let mut builder = self.request(method, &object_url).await?;
        if let Some(r) = range {
            builder = builder.header(reqwest::header::RANGE, r.to_header_value());
        }
        let response = builder.send().await.map_err(|cause| Error::Reqwest {
            context: "S3: failed to send request".to_string(),
            cause,
        })?;

        if response.status() == StatusCode::NOT_FOUND {
            objectstore_log::debug!("Object not found");
            return Ok(None);
        }

        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            let total = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(ContentRange::parse_unsatisfiable_total)
                .unwrap_or(0);
            return Err(Error::RangeNotSatisfiable { total });
        }

        let response = response
            .error_for_status()
            .map_err(|cause| Error::Reqwest {
                context: "S3: failed to get object".to_string(),
                cause,
            })?;

        let headers = response.headers();
        let mut metadata = Metadata::from_headers(headers, GCS_CUSTOM_PREFIX)?;
        // For partial responses, Content-Length is the range body length, not the full object size.
        // Use the Content-Range total instead.
        metadata.size = if response.status() == StatusCode::PARTIAL_CONTENT {
            headers
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(ContentRange::parse)
                .map(|cr| cr.total as usize)
        } else {
            response.content_length().map(|len| len as usize)
        };

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
            .map_err(|cause| Error::Reqwest {
                context: "S3: failed to send TTI update request".to_string(),
                cause,
            })?
            .error_for_status()
            .map_err(|cause| Error::Reqwest {
                context: "S3: failed to update expiration time for object with TTI".to_string(),
                cause,
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
            .map_err(|cause| match stream::unpack_client_error(&cause) {
                Some(ce) => Error::Client(ce),
                _ => Error::Reqwest {
                    context: "S3: failed to put object".to_string(),
                    cause,
                },
            })?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId, range: Option<ByteRange>) -> Result<GetResponse> {
        objectstore_log::debug!("Reading from s3_compatible backend");

        let Some((metadata, response)) = self.request_object(Method::GET, id, range).await? else {
            return Ok(None);
        };

        let content_range = if response.status() == StatusCode::PARTIAL_CONTENT {
            response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(ContentRange::parse)
                .ok_or_else(|| Error::Generic {
                    context: "S3: 206 response missing valid Content-Range header".to_owned(),
                    cause: None,
                })?
        } else {
            ContentRange::full(metadata.size.unwrap_or(0) as u64)
        };

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some((metadata, content_range, stream.boxed())))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        objectstore_log::debug!("Reading metadata from s3_compatible backend");
        let response = self.request_object(Method::HEAD, id, None).await?;
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
            .map_err(|cause| Error::Reqwest {
                context: "S3: failed to send delete request".to_string(),
                cause,
            })?;

        // Do not error for objects that do not exist.
        if response.status() != StatusCode::NOT_FOUND {
            objectstore_log::debug!("Object not found");
            response
                .error_for_status()
                .map_err(|cause| Error::Reqwest {
                    context: "S3: failed to delete object".to_string(),
                    cause,
                })?;
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
