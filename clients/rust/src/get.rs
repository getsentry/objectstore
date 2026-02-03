use std::{fmt, io};

use async_compression::tokio::bufread::ZstdDecoder;
use bytes::BytesMut;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::Metadata;
use objectstore_types::key::ObjectKey;
use reqwest::StatusCode;
use tokio_util::io::{ReaderStream, StreamReader};

pub use objectstore_types::Compression;

use crate::{ClientStream, Session};

/// The result from a successful [`get()`](Session::get) call.
///
/// This carries the response as a stream, plus the compression algorithm of the data.
pub struct GetResponse {
    /// The metadata attached to this object, including the compression algorithm used for the payload.
    pub metadata: Metadata,
    /// The response stream.
    pub stream: ClientStream,
}

impl GetResponse {
    /// Loads the object payload fully into memory.
    pub async fn payload(self) -> crate::Result<bytes::Bytes> {
        let bytes: BytesMut = self.stream.try_collect().await?;
        Ok(bytes.freeze())
    }

    /// Loads the object payload fully into memory and interprets it as UTF-8 text.
    pub async fn text(self) -> crate::Result<String> {
        let bytes = self.payload().await?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

impl fmt::Debug for GetResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetResponse")
            .field("metadata", &self.metadata)
            .field("stream", &format_args!("[Stream]"))
            .finish()
    }
}

impl Session {
    /// Retrieves the object with the given `key`.
    ///
    /// The key will be validated and encoded according to RFC 3986. Reserved characters
    /// (like `/`, `?`, `#`, etc.) will be percent-encoded automatically.
    ///
    /// Note: Key validation is deferred to the `send()` call. If the key is invalid,
    /// `send()` will return an error.
    pub fn get(&self, key: &str) -> GetBuilder {
        GetBuilder {
            session: self.clone(),
            key: ObjectKey::new(key).map_err(Into::into),
            decompress: true,
        }
    }
}

/// A [`get`](Session::get) request builder.
pub struct GetBuilder {
    session: Session,
    key: crate::Result<ObjectKey>,
    decompress: bool,
}

impl fmt::Debug for GetBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetBuilder")
            .field("session", &self.session)
            .field("key", &self.key.as_ref().map(|k| k.as_str()))
            .field("decompress", &self.decompress)
            .finish()
    }
}

impl GetBuilder {
    /// Indicates whether the request should automatically handle decompression of known algorithms,
    /// or rather return the payload as it is stored, along with the compression algorithm it is stored in.
    ///
    /// By default, automatic decompression is enabled.
    pub fn decompress(mut self, decompress: bool) -> Self {
        self.decompress = decompress;
        self
    }

    /// Sends the get request.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key is invalid (empty, too long, or contains non-ASCII characters)
    /// - The request fails to send
    /// - The server returns an error response (other than 404)
    pub async fn send(self) -> crate::Result<Option<GetResponse>> {
        let key = self.key?;
        let response = self
            .session
            .request(reqwest::Method::GET, &key)?
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response.error_for_status()?;

        let mut metadata = Metadata::from_headers(response.headers(), "")?;

        let stream = response.bytes_stream().map_err(io::Error::other);
        let stream = match (metadata.compression, self.decompress) {
            (Some(Compression::Zstd), true) => {
                metadata.compression = None;
                ReaderStream::new(ZstdDecoder::new(StreamReader::new(stream))).boxed()
            }
            _ => stream.boxed(),
        };

        Ok(Some(GetResponse { metadata, stream }))
    }
}
