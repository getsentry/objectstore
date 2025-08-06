use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::Metadata;
use reqwest::{Body, StatusCode};

use super::{Backend, BackendStream};

const GCS_CUSTOM_PREFIX: &str = "x-goog-meta-";

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

pub struct S3Compatible<T> {
    client: reqwest::Client,

    endpoint: String,
    bucket: String,

    token_provider: Option<T>,
}

impl<T> S3Compatible<T> {
    pub fn new(endpoint: &str, bucket: &str, token_provider: T) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            token_provider: Some(token_provider),
        }
    }
}

impl<T> fmt::Debug for S3Compatible<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Compatible")
            .field("client", &self.client)
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl S3Compatible<NoToken> {
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
impl<T: TokenProvider> Backend for S3Compatible<T> {
    async fn put_object(
        &self,
        path: &str,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> anyhow::Result<()> {
        let put_url = format!("{}/{}/{path}", self.endpoint, self.bucket);

        let mut builder = self.client.put(put_url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }

        builder = builder.headers(metadata.to_headers(GCS_CUSTOM_PREFIX, true)?);

        let _response = builder.body(Body::wrap_stream(stream)).send().await?;

        Ok(())
    }

    async fn get_object(&self, path: &str) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        let get_url = format!("{}/{}/{path}", self.endpoint, self.bucket);

        let mut builder = self.client.get(get_url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }

        let response = builder.send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response.error_for_status()?;

        let metadata = Metadata::from_headers(response.headers(), GCS_CUSTOM_PREFIX)?;
        // TODO: the object *GET* should probably also contain the expiration time?

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some((metadata, stream.boxed())))
    }

    async fn delete_object(&self, path: &str) -> anyhow::Result<()> {
        let delete_url = format!("{}/{}/{path}", self.endpoint, self.bucket);

        let mut builder = self.client.delete(delete_url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }
        let _response = builder.send().await?;

        Ok(())
    }
}
