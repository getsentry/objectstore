use std::fmt;
use std::io::Cursor;
use std::{borrow::Cow, collections::BTreeMap};

use async_compression::tokio::bufread::ZstdEncoder;
use bytes::Bytes;
use futures_util::StreamExt;
use objectstore_types::Metadata;
use reqwest::Body;
use serde::Deserialize;
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};

pub use objectstore_types::{Compression, ExpirationPolicy};

use crate::{ClientStream, Session};

/// The response returned from the service after uploading an object.
#[derive(Debug, Deserialize)]
pub struct PutResponse {
    /// The key of the object, as stored.
    pub key: String,
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

impl Session {
    fn put_body(&self, body: PutBody) -> PutBuilder {
        let metadata = Metadata {
            expiration_policy: self.scope.usecase().expiration_policy(),
            compression: Some(self.scope.usecase().compression()),
            ..Default::default()
        };

        PutBuilder {
            session: self.clone(),
            metadata,
            key: None,
            body,
        }
    }

    /// Creates or replaces an object using a [`Bytes`]-like payload.
    pub fn put(&self, body: impl Into<Bytes>) -> PutBuilder {
        self.put_body(PutBody::Buffer(body.into()))
    }

    /// Creates or replaces an object using a streaming payload.
    pub fn put_stream(&self, body: ClientStream) -> PutBuilder {
        self.put_body(PutBody::Stream(body))
    }

    /// Creates or replaces an object using an [`AsyncRead`] payload.
    pub fn put_read<R>(&self, body: R) -> PutBuilder
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        let stream = ReaderStream::new(body).boxed();
        self.put_body(PutBody::Stream(stream))
    }
}

/// A [`put`](Session::put) request builder.
#[derive(Debug)]
pub struct PutBuilder {
    session: Session,
    metadata: Metadata,
    key: Option<String>,
    body: PutBody,
}

impl PutBuilder {
    /// Sets an explicit object key.
    ///
    /// If a key is specified, the object will be stored under that key. Otherwise, the Objectstore
    /// server will automatically assign a random key, which is then returned from this request.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into()).filter(|k| !k.is_empty());
        self
    }

    /// Sets an explicit compression algorithm to be used for this payload.
    ///
    /// [`None`] should be used if no compression should be performed by the client,
    /// either because the payload is uncompressible (such as a media format), or if the user
    /// will handle any kind of compression, without the clients knowledge.
    ///
    /// By default, the compression algorithm set on this Session's Usecase is used.
    pub fn compression(mut self, compression: impl Into<Option<Compression>>) -> Self {
        self.metadata.compression = compression.into();
        self
    }

    /// Sets the expiration policy of the object to be uploaded.
    ///
    /// By default, the expiration policy set on this Session's Usecase is used.
    pub fn expiration_policy(mut self, expiration_policy: ExpirationPolicy) -> Self {
        self.metadata.expiration_policy = expiration_policy;
        self
    }

    /// Sets the content type of the object to be uploaded.
    ///
    /// You can use the utility function [`crate::utils::guess_mime_type`] to attempt to guess a
    /// `content_type` based on magic bytes.
    pub fn content_type(mut self, content_type: impl Into<Cow<'static, str>>) -> Self {
        self.metadata.content_type = content_type.into();
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

// TODO: instead of a separate `send` method, it would be nice to just implement `IntoFuture`.
// However, `IntoFuture` needs to define the resulting future as an associated type,
// and "impl trait in associated type position" is not yet stable :-(
impl PutBuilder {
    /// Sends the built put request to the upstream service.
    pub async fn send(self) -> crate::Result<PutResponse> {
        let method = match self.key {
            Some(_) => reqwest::Method::PUT,
            None => reqwest::Method::POST,
        };

        let mut builder = self
            .session
            .request(method, self.key.as_deref().unwrap_or_default());

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
