use std::time::{Duration, SystemTime};
use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::{ExpirationPolicy, Metadata};
use reqwest::{Body, IntoUrl, Method, RequestBuilder, StatusCode};

use crate::backend::common::{self, Backend};
use crate::id::ObjectId;
use crate::{DeleteResponse, GetResponse, PayloadStream, ServiceError, ServiceResult};

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

pub trait Token: Send + Sync {
    fn as_str(&self) -> &str;
}

pub trait TokenProvider: Send + Sync + 'static {
    fn get_token(&self) -> impl Future<Output = anyhow::Result<impl Token>> + Send;
}

// this only exists because we have to provide *some* kind of provider
#[derive(Debug)]
pub struct NoToken;

impl TokenProvider for NoToken {
    #[allow(refining_impl_trait_internal)] // otherwise, returning `!` will not implement the required traits
    async fn get_token(&self) -> anyhow::Result<NoToken> {
        unimplemented!()
    }
}
impl Token for NoToken {
    fn as_str(&self) -> &str {
        unimplemented!()
    }
}

pub struct S3CompatibleBackend<T> {
    client: reqwest::Client,

    endpoint: String,
    bucket: String,

    token_provider: Option<T>,
}

impl<T> S3CompatibleBackend<T> {
    /// Creates a new S3 compatible backend bound to the given bucket.
    #[expect(dead_code)]
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

impl<T> S3CompatibleBackend<T>
where
    T: TokenProvider,
{
    /// Creates a request builder with the appropriate authentication.
    async fn request(&self, method: Method, url: impl IntoUrl) -> ServiceResult<RequestBuilder> {
        let mut builder = self.client.request(method, url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(
                provider
                    .get_token()
                    .await
                    .map_err(|err| ServiceError::Generic {
                        context: "S3: failed to get authentication token".to_owned(),
                        cause: Some(err.into()),
                    })?
                    .as_str(),
            );
        }
        Ok(builder)
    }

    /// Issues a request to update the metadata for the given object.
    async fn update_metadata(&self, id: &ObjectId, metadata: &Metadata) -> ServiceResult<()> {
        // NB: Meta updates require copy + REPLACE along with *all* metadata. See
        // https://cloud.google.com/storage/docs/xml-api/put-object-copy
        self.request(Method::PUT, self.object_url(id))
            .await?
            .header(
                "x-goog-copy-source",
                format!("/{}/{}", self.bucket, id.as_storage_path()),
            )
            .header("x-goog-metadata-directive", "REPLACE")
            .headers(metadata.to_headers(GCS_CUSTOM_PREFIX, true)?)
            .send()
            .await
            .map_err(|cause| ServiceError::Reqwest {
                context: "S3: failed to send TTI update request".to_string(),
                cause,
            })?
            .error_for_status()
            .map_err(|cause| ServiceError::Reqwest {
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
    pub fn without_token(endpoint: &str, bucket: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
            bucket: bucket.into(),
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
        stream: PayloadStream,
    ) -> ServiceResult<DeleteResponse> {
        tracing::debug!("Writing to s3_compatible backend");
        self.request(Method::PUT, self.object_url(id))
            .await?
            .headers(metadata.to_headers(GCS_CUSTOM_PREFIX, true)?)
            .body(Body::wrap_stream(stream))
            .send()
            .await
            .map_err(|cause| ServiceError::Reqwest {
                context: "S3: failed to send put request".to_string(),
                cause,
            })?
            .error_for_status()
            .map_err(|cause| ServiceError::Reqwest {
                context: "S3: failed to put object".to_string(),
                cause,
            })?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> ServiceResult<GetResponse> {
        tracing::debug!("Reading from s3_compatible backend");
        let object_url = self.object_url(id);

        let response = self
            .request(Method::GET, &object_url)
            .await?
            .send()
            .await
            .map_err(|cause| ServiceError::Reqwest {
                context: "S3: failed to send get request".to_string(),
                cause,
            })?;
        if response.status() == StatusCode::NOT_FOUND {
            tracing::debug!("Object not found");
            return Ok(None);
        }

        let response = response
            .error_for_status()
            .map_err(|cause| ServiceError::Reqwest {
                context: "S3: failed to get object".to_string(),
                cause,
            })?;

        let headers = response.headers();
        // TODO: Populate size in metadata
        let metadata = Metadata::from_headers(headers, GCS_CUSTOM_PREFIX)?;

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
                // This serializes a new custom-time internally.
                self.update_metadata(id, &metadata).await?;
            }
        }

        // TODO: the object *GET* should probably also contain the expiration time?

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some((metadata, stream.boxed())))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> ServiceResult<DeleteResponse> {
        tracing::debug!("Deleting from s3_compatible backend");
        let response = self
            .request(Method::DELETE, self.object_url(id))
            .await?
            .send()
            .await
            .map_err(|cause| ServiceError::Reqwest {
                context: "S3: failed to send delete request".to_string(),
                cause,
            })?;

        // Do not error for objects that do not exist.
        if response.status() != StatusCode::NOT_FOUND {
            tracing::debug!("Object not found");
            response
                .error_for_status()
                .map_err(|cause| ServiceError::Reqwest {
                    context: "S3: failed to delete object".to_string(),
                    cause,
                })?;
        }

        Ok(())
    }
}
