use std::str::FromStr;
use std::{fmt, io};

use async_compression::tokio::bufread::ZstdDecoder;
use futures_util::{StreamExt, TryStreamExt};
use reqwest::{StatusCode, header};
use tokio_util::io::{ReaderStream, StreamReader};

pub use objectstore_types::Compression;

use crate::{Client, ClientStream};

/// The result from a successful [`get()`](Client::get) call.
///
/// This carries the response as a stream, plus the compression algorithm of the data.
pub struct GetResult {
    /// The response stream.
    pub stream: ClientStream,
    /// The compression algorithm of the response data.
    pub compression: Option<Compression>,
}
impl fmt::Debug for GetResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetResult")
            .field("stream", &format_args!("[Stream]"))
            .field("compression", &self.compression)
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
        let authorization = self.client.make_authorization("read")?;

        let builder = self
            .client
            .client
            .get(get_url)
            .header(header::AUTHORIZATION, authorization);

        let response = builder.send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response.error_for_status()?;

        let compression = if let Some(compression) = response
            .headers()
            .get(header::CONTENT_ENCODING)
            .map(|h| h.to_str())
        {
            Some(Compression::from_str(compression?).map_err(anyhow::Error::msg)?)
        } else {
            None
        };

        let stream = response.bytes_stream().map_err(io::Error::other);
        let (stream, compression) = match (compression, self.decompress) {
            (Some(Compression::Zstd), true) => {
                let decoder = ZstdDecoder::new(StreamReader::new(stream));
                let stream = ReaderStream::new(decoder).boxed();

                (stream, None)
            }
            _ => (stream.boxed(), compression),
        };

        Ok(Some(GetResult {
            stream,
            compression,
        }))
    }
}
