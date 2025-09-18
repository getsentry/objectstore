use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;

use async_compression::tokio::bufread::ZstdEncoder;
use bytes::Bytes;
use futures_util::StreamExt;
use objectstore_types::Metadata;
use reqwest::Body;
use serde::Deserialize;
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};

pub use objectstore_types::{Compression, ExpirationPolicy};

use crate::{Client, ClientStream};

impl Client {
    fn put_body(&self, body: PutBody) -> PutBuilder<'_> {
        let metadata = Metadata {
            compression: Some(self.default_compression),
            ..Default::default()
        };

        PutBuilder {
            client: self,
            metadata,
            body,
        }
    }

    /// Creates a PUT request for a [`Bytes`]-like type.
    pub fn put(&self, body: impl Into<Bytes>) -> PutBuilder<'_> {
        self.put_body(PutBody::Buffer(body.into()))
    }

    /// Creates a PUT request with a stream.
    pub fn put_stream(&self, body: ClientStream) -> PutBuilder<'_> {
        self.put_body(PutBody::Stream(body))
    }

    /// Creates a PUT request with an [`AsyncRead`] type.
    pub fn put_read<R>(&self, body: R) -> PutBuilder<'_>
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        let stream = ReaderStream::new(body).boxed();
        self.put_body(PutBody::Stream(stream))
    }
}

/// A PUT request builder.
#[derive(Debug)]
pub struct PutBuilder<'a> {
    pub(crate) client: &'a Client,
    pub(crate) metadata: Metadata,
    pub(crate) body: PutBody,
}

pub(crate) enum PutBody {
    Buffer(Bytes),
    Stream(ClientStream),
}
impl fmt::Debug for PutBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PutBody").finish_non_exhaustive()
    }
}

impl PutBuilder<'_> {
    /// Sets an explicit compression algorithm to be used for this payload.
    ///
    /// [`None`] should be used if no compression should be performed by the client,
    /// either because the payload is uncompressible (such as a media format), or if the user
    /// will handle any kind of compression, without the clients knowledge.
    pub fn compression(mut self, compression: impl Into<Option<Compression>>) -> Self {
        self.metadata.compression = compression.into();
        self
    }

    /// Sets the expiration policy of the object to be uploaded.
    pub fn expiration_policy(mut self, expiration_policy: ExpirationPolicy) -> Self {
        self.metadata.expiration_policy = expiration_policy;
        self
    }

    /// This sets the custom metadata to the provided map.
    ///
    /// It will clear any previously set metadata.
    pub fn set_metadata(mut self, metadata: impl Into<BTreeMap<String, String>>) -> Self {
        self.metadata.custom = metadata.into();
        self
    }

    /// Appends they `key`/`value` to the custom metadata of this object.
    pub fn append_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.custom.insert(key.into(), value.into());
        self
    }
}

/// The response returned from the service after uploading an object.
#[derive(Debug, Deserialize)]
pub struct PutResponse {
    /// The key of the object, as stored.
    pub key: String,
}

// TODO: instead of a separate `send` method, it would be nice to just implement `IntoFuture`.
// However, `IntoFuture` needs to define the resulting future as an associated type,
// and "impl trait in associated type position" is not yet stable :-(
impl PutBuilder<'_> {
    /// Sends the built PUT request to the upstream service.
    pub async fn send(self) -> anyhow::Result<PutResponse> {
        let mut builder = self
            .client
            .request(reqwest::Method::PUT, self.client.service_url.as_ref())?;

        let body = match (self.metadata.compression, self.body) {
            (Some(Compression::Zstd), PutBody::Buffer(bytes)) => {
                let cursor = Cursor::new(bytes);
                let encoder = ZstdEncoder::new(cursor);
                let stream = ReaderStream::new(encoder);
                Body::wrap_stream(stream)
            }
            (Some(Compression::Zstd), PutBody::Stream(stream)) => {
                let stream = StreamReader::new(stream);
                let encoder = ZstdEncoder::new(stream);
                let stream = ReaderStream::new(encoder);
                Body::wrap_stream(stream)
            }
            (None, PutBody::Buffer(bytes)) => bytes.into(),
            (None, PutBody::Stream(stream)) => Body::wrap_stream(stream),
            // _ => todo!("compression algorithms other than `zstd` are currently not supported"),
        };

        builder = builder.headers(self.metadata.to_headers("", false)?);

        let response = builder.body(body).send().await?;
        Ok(response.error_for_status()?.json().await?)
    }
}
