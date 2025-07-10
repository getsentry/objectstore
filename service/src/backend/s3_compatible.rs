use std::io;

use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt as _, TryStreamExt};
use reqwest::{Body, StatusCode};

use crate::backend::Backend;

pub trait Token: Send + Sync {
    fn as_str(&self) -> &str;
}

pub trait TokenProvider: Send + Sync {
    fn get_token(&self) -> impl Future<Output = anyhow::Result<impl Token>> + Send;
}

// this only exists because we have to provide *some* kind of provider
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
        stream: BoxStream<'static, io::Result<Bytes>>,
    ) -> anyhow::Result<()> {
        let put_url = format!("{}/{}/{path}", self.endpoint, self.bucket);

        let mut builder = self.client.put(put_url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }
        let _response = builder.body(Body::wrap_stream(stream)).send().await?;

        Ok(())
    }

    async fn get_file(
        &self,
        path: &str,
    ) -> anyhow::Result<Option<BoxStream<'static, io::Result<Bytes>>>> {
        let get_url = format!("{}/{}/{path}", self.endpoint, self.bucket);

        let mut builder = self.client.get(get_url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }
        let response = builder.send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some(stream.boxed()))
    }
}
