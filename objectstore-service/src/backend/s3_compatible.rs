use std::fmt;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures::stream;
use objectstore_types::Metadata;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use s3::creds::Credentials;
use s3::request::ResponseData;
use s3::{Bucket, Region};
use tokio_util::io::StreamReader;

use crate::backend::common::{Backend, BackendStream};
use crate::path::ObjectPath;

pub struct S3CompatibleBackend {
    bucket: Box<Bucket>,
}

pub struct S3CompatibleBackendConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    #[allow(dead_code)]
    pub extra_headers: HeaderMap,
    pub request_timeout: Option<Duration>,
    pub path_style: Option<bool>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub security_token: Option<String>,
    pub session_token: Option<String>,
}

impl Default for S3CompatibleBackendConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: String::new(),
            endpoint: None,
            extra_headers: HeaderMap::new(),
            request_timeout: None,
            path_style: None,
            access_key: None,
            secret_key: None,
            security_token: None,
            session_token: None,
        }
    }
}

impl S3CompatibleBackend {
    /// Creates a new S3 compatible backend bound to the given bucket.
    pub fn new(config: S3CompatibleBackendConfig) -> Self {
        let credentials = Credentials::new(
            config.access_key.as_deref(),
            config.secret_key.as_deref(),
            config.security_token.as_deref(),
            config.session_token.as_deref(),
            None,
        )
        .unwrap();

        let mut bucket = Bucket::new(
            config.bucket.as_str(),
            Region::Custom {
                region: config.region.clone(),
                endpoint: match config.endpoint {
                    Some(endpoint) => endpoint,
                    None => format!("s3-{}.amazonaws.com", config.region),
                },
            },
            credentials,
        )
        .unwrap();

        if let Some(path_style) = config.path_style
            && path_style
        {
            bucket = bucket.with_path_style();
        }

        if let Some(request_timeout) = config.request_timeout {
            bucket = bucket.with_request_timeout(request_timeout).unwrap();
        }

        Self { bucket }
    }

    async fn handle_get_object_response(
        &self,
        _path: &ObjectPath,
        response: ResponseData,
    ) -> Result<Option<(Metadata, BackendStream)>> {
        let metadata = Metadata::from_hashmap(response.headers(), "").unwrap_or_default();
        let bytes = Bytes::from(response.to_vec());
        let stream: BackendStream = Box::pin(stream::iter(std::iter::once(Ok(bytes))));

        Ok(Some((metadata, stream)))
    }
}

impl fmt::Debug for S3CompatibleBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Compatible")
            .field("bucket", &self.bucket.name())
            .field("endpoint", &self.bucket.host())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Backend for S3CompatibleBackend {
    fn name(&self) -> &'static str {
        "s3-compatible"
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn put_object(
        &self,
        path: &ObjectPath,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> Result<()> {
        tracing::debug!("Writing to s3_compatible backend");
        let header_map = metadata
            .custom
            .iter()
            .filter_map(|(key, value)| {
                let name = key.parse::<HeaderName>().ok()?;
                let val = value.parse::<HeaderValue>().ok()?;
                Some((name, val))
            })
            .collect::<HeaderMap>();

        self.bucket
            .put_object_stream_builder(path.to_string())
            .with_content_type(metadata.content_type.as_ref())
            .with_headers(header_map)
            .execute_stream(&mut StreamReader::new(stream))
            .await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn get_object(&self, path: &ObjectPath) -> Result<Option<(Metadata, BackendStream)>> {
        tracing::debug!("Reading from s3_compatible backend");

        let response = self.bucket.get_object(path.to_string()).await?;
        if response.status_code() == StatusCode::NOT_FOUND {
            tracing::debug!("Object not found");
            return Ok(None);
        }

        return self.handle_get_object_response(path, response).await;
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn delete_object(&self, path: &ObjectPath) -> Result<()> {
        tracing::debug!("Deleting from s3_compatible backend");
        self.bucket.delete_object(path.to_string()).await?;
        Ok(())
    }
}
