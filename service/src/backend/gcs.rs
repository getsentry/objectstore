use std::io;
use std::sync::Arc;

use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt as _, TryStreamExt};
use gcp_auth::{Token, TokenProvider};
use reqwest::{Body, StatusCode};

use crate::backend::Backend;

pub struct Gcs {
    client: reqwest::Client,
    gcs_token_provider: Arc<dyn TokenProvider>,
    gcs_bucket: String,
}

impl Gcs {
    pub async fn new(bucket: &str) -> anyhow::Result<Self> {
        let gcs_token_provider = gcp_auth::provider().await?;
        Ok(Self {
            client: reqwest::Client::new(),
            gcs_token_provider,
            gcs_bucket: bucket.into(),
        })
    }
}

impl Gcs {
    pub async fn gcs_token(&self) -> anyhow::Result<Arc<Token>> {
        let token = self
            .gcs_token_provider
            .token(&["https://www.googleapis.com/auth/devstorage.read_write"])
            .await?;
        Ok(token)
    }
}

impl Backend for Gcs {
    async fn put_file(
        &self,
        path: &str,
        stream: BoxStream<'static, io::Result<Bytes>>,
    ) -> anyhow::Result<()> {
        let put_url = format!("https://storage.googleapis.com/{}/{path}", self.gcs_bucket,);
        let token = self.gcs_token().await?;

        let _response = self
            .client
            .put(put_url)
            .bearer_auth(token.as_str())
            .body(Body::wrap_stream(stream))
            .send()
            .await?;

        Ok(())
    }

    async fn get_file(
        &self,
        path: &str,
    ) -> anyhow::Result<Option<BoxStream<'static, io::Result<Bytes>>>> {
        let get_url = format!("https://storage.googleapis.com/{}/{path}", self.gcs_bucket,);
        let token = self.gcs_token().await?;
        let response = self
            .client
            .get(get_url)
            .bearer_auth(token.as_str())
            .send()
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some(stream.boxed()))
    }
}
