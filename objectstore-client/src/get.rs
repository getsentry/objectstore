use std::{fmt, io};

use async_compression::tokio::bufread::ZstdDecoder;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::Metadata;
use reqwest::StatusCode;
use tokio_util::io::{ReaderStream, StreamReader};

pub use objectstore_types::{Compression, PARAM_SCOPE, PARAM_USECASE};

use crate::{Client, ClientStream};

/// The result from a successful [`get()`](Client::get) call.
///
/// This carries the response as a stream, plus the compression algorithm of the data.
pub struct GetResult {
    /// The metadata attached to this object, including the compression algorithm used for the payload.
    pub metadata: Metadata,
    /// The response stream.
    pub stream: ClientStream,
}
impl fmt::Debug for GetResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetResult")
            .field("metadata", &self.metadata)
            .field("stream", &format_args!("[Stream]"))
            .finish()
    }
}

/// A GET request builder.
#[derive(Debug)]
pub struct GetBuilder<'a> {
    client: &'a Client,
    id: &'a str,

    decompress: bool,
}

impl Client {
    /// Requests the object with the given `id`.
    pub fn get<'a>(&'a self, id: &'a str) -> GetBuilder<'a> {
        GetBuilder {
            client: self,
            id,
            decompress: true,
        }
    }
}

impl GetBuilder<'_> {
    /// Indicates whether the request should automatically handle decompression of known algorithms,
    /// or rather return the payload as it is stored, along with the compression algorithm it is stored in.
    pub fn decompress(mut self, decompress: bool) -> Self {
        self.decompress = decompress;
        self
    }

    /// Sends the `GET` request.
    pub async fn send(self) -> anyhow::Result<Option<GetResult>> {
        let get_url = format!("{}/{}", self.client.service_url, self.id);

        let builder = self.client.http.get(get_url).query(&[
            (PARAM_SCOPE, self.client.scope.as_ref()),
            (PARAM_USECASE, self.client.usecase.as_ref()),
        ]);

        let response = builder.send().await?;
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

        Ok(Some(GetResult { metadata, stream }))
    }
}
