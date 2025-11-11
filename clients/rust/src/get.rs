use std::{fmt, io};

use async_compression::tokio::bufread::ZstdDecoder;
use bytes::BytesMut;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::Metadata;
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
        f.debug_struct("GetResult")
            .field("metadata", &self.metadata)
            .field("stream", &format_args!("[Stream]"))
            .finish()
    }
}

impl Session {
    /// Requests the object with the given `id`.
    pub fn get<'a>(&'a self, id: &'a str) -> GetBuilder<'a> {
        GetBuilder {
            session: self,
            id,
            decompress: true,
        }
    }
}

/// A GET request builder.
#[derive(Debug)]
pub struct GetBuilder<'a> {
    session: &'a Session,
    id: &'a str,
    decompress: bool,
}

impl GetBuilder<'_> {
    /// Indicates whether the request should automatically handle decompression of known algorithms,
    /// or rather return the payload as it is stored, along with the compression algorithm it is stored in.
    pub fn decompress(mut self, decompress: bool) -> Self {
        self.decompress = decompress;
        self
    }

    /// Sends the `GET` request.
    pub async fn send(self) -> crate::Result<Option<GetResponse>> {
        let get_url = format!("{}v1/{}", self.session.client.service_url(), self.id);

        let response = self
            .session
            .request(reqwest::Method::GET, get_url)?
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
