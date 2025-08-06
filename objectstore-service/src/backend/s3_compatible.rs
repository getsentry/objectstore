use std::str::FromStr;
use std::time::SystemTime;
use std::{fmt, io};

use futures_util::{StreamExt, TryStreamExt};
use humantime::format_rfc3339_seconds;
use objectstore_types::{Compression, ExpirationPolicy, HEADER_EXPIRATION, Metadata};
use reqwest::header::HeaderValue;
use reqwest::{Body, StatusCode, header};

use super::{Backend, BackendStream};

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
    async fn put_file(
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
        if let Some(compression) = metadata.compression {
            builder = builder.header(header::CONTENT_ENCODING, compression.as_str());
        }
        if metadata.expiration_policy != ExpirationPolicy::Manual {
            builder = builder.header(HEADER_EXPIRATION, metadata.expiration_policy.to_string());
            let expires_in = metadata.expiration_policy.expires_in();
            let expires_at = format_rfc3339_seconds(SystemTime::now() + expires_in);
            builder = builder.header("Custom-Time", expires_at.to_string());
        }

        let _response = builder.body(Body::wrap_stream(stream)).send().await?;

        Ok(())
    }

    async fn get_file(&self, path: &str) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        let get_url = format!("{}/{}/{path}", self.endpoint, self.bucket);

        let mut builder = self.client.get(get_url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }
        let response = builder.send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let headers = response.headers();
        let mut metadata = Metadata::default();
        if let Some(compression) = headers
            .get(header::CONTENT_ENCODING)
            .map(HeaderValue::to_str)
            .transpose()?
        {
            metadata.compression = Some(Compression::from_str(compression)?);
        }
        if let Some(expiration_policy) = headers
            .get(HEADER_EXPIRATION)
            .map(HeaderValue::to_str)
            .transpose()?
        {
            metadata.expiration_policy = ExpirationPolicy::from_str(expiration_policy)?;
        }
        // TODO: the object *GET* should probably also contain the expiration time

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some((metadata, stream.boxed())))
    }

    async fn delete_file(&self, path: &str) -> anyhow::Result<()> {
        let delete_url = format!("{}/{}/{path}", self.endpoint, self.bucket);

        let mut builder = self.client.delete(delete_url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }
        let _response = builder.send().await?;

        Ok(())
    }
}
