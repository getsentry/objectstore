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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{StreamExt, TryStreamExt};
    use objectstore_types::ExpirationPolicy;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    // NB: Not run any of these tests, you need to have a S3 emulator running. This is done
    // automatically in CI.
    //
    // Refer to the readme for how to set up the emulator.

    async fn create_test_backend() -> Result<S3CompatibleBackend> {
        Ok(S3CompatibleBackend::new(S3CompatibleBackendConfig {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://localhost:8088".into()),
            extra_headers: HeaderMap::new(),
            request_timeout: Some(Duration::from_secs(60)),
            path_style: Some(true),
            access_key: Some("test-key".into()),
            secret_key: Some("test-secret".into()),
            security_token: None,
            session_token: None,
        }))
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
            scope: "testing".into(),
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
}
