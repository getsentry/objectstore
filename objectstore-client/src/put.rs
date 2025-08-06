use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::marker::PhantomData;

use async_compression::tokio::bufread::ZstdEncoder;
use bytes::Bytes;
use futures_util::StreamExt;
use objectstore_types::Metadata;
use reqwest::{Body, header};
use serde::Deserialize;
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};

pub use objectstore_types::{Compression, ExpirationPolicy};

use crate::{Client, ClientStream};

impl Client {
    /// Creates a PUT request using the optional `id`.
    pub fn put<'a>(&'a self, id: impl Into<Option<&'a str>>) -> PutBuilder<'a, ()> {
        let metadata = Metadata {
            compression: Some(self.default_compression),
            ..Default::default()
        };

        PutBuilder {
            client: self,
            id: id.into(),

            metadata,

            body: PutBody::None,
            marker: PhantomData,
        }
    }
}

/// A PUT request builder.
pub struct PutBuilder<'a, Body> {
    pub(crate) client: &'a Client,
    pub(crate) id: Option<&'a str>,

    pub(crate) metadata: Metadata,

    pub(crate) body: PutBody,
    pub(crate) marker: PhantomData<Body>,
}

impl<'a, Body> fmt::Debug for PutBuilder<'a, Body> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PutBuilder")
            .field("client", &self.client)
            .field("id", &self.id)
            .field("metadata", &self.metadata)
            .field("body", &format_args!("[Body]"))
            .finish()
    }
}

/// A typestate marker to denote put requests that have a body and can thus be sent.
#[derive(Debug)]
pub enum HasBodyMarker {}

pub(crate) enum PutBody {
    None,
    Buffer(Bytes),
    Stream(ClientStream),
}

impl<'a, B> PutBuilder<'a, B> {
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

    fn with_body(self, body: PutBody) -> PutBuilder<'a, HasBodyMarker> {
        PutBuilder {
            client: self.client,
            id: self.id,

            metadata: self.metadata,

            body,
            marker: PhantomData,
        }
    }

    /// Uploads an in-memory buffer.
    pub fn buffer(self, buffer: impl Into<Bytes>) -> PutBuilder<'a, HasBodyMarker> {
        self.with_body(PutBody::Buffer(buffer.into()))
    }

    /// Uploads an async `Stream`.
    pub fn stream(self, stream: ClientStream) -> PutBuilder<'a, HasBodyMarker> {
        self.with_body(PutBody::Stream(stream))
    }

    /// Uploads an [`AsyncRead`], such as a [`File`](tokio::fs::File).
    pub fn read<R>(self, reader: R) -> PutBuilder<'a, HasBodyMarker>
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        let stream = ReaderStream::new(reader).boxed();
        self.with_body(PutBody::Stream(stream))
    }
}

/// The response returned from the service after uploading an object.
#[derive(Debug, Deserialize)]
pub struct PutResponse {
    /// The key of the object, as stored.
    pub key: String,
}

impl<'a> PutBuilder<'a, HasBodyMarker> {
    /// Sends the built PUT request to the upstream service.
    pub async fn send(self) -> anyhow::Result<PutResponse> {
        let put_url = format!(
            "{}/{}",
            self.client.service_url,
            self.id.unwrap_or_default()
        );
        let authorization = self.client.make_authorization("write")?;

        let mut builder = self
            .client
            .http
            .put(put_url)
            .header(header::AUTHORIZATION, authorization);

        let body = match (self.metadata.compression, self.body) {
            (_, PutBody::None) => unreachable!(),
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
