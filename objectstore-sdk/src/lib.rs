//! The Storage Client SDK
//!
//! This Client SDK can be used to put/get blobs.
//! It internally deals with chunking and compression of uploads and downloads,
//! making sure that it is done as efficiently as possible.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

use std::fmt;
use std::io::Cursor;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;

use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use jsonwebtoken::{EncodingKey, Header};
use reqwest::{Body, StatusCode, header};
use serde::{Deserialize, Serialize};
use tokio_util::io::{ReaderStream, StreamReader};

#[cfg(test)]
mod tests;

/// The compression algorithm of an object to upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Compressed using `zstd`.
    Zstd,
    /// Compressed using `gzip`.
    Gzip,
    /// Compressed using `lz4`.
    Lz4,
    /// The payload is uncompressible.
    Uncompressible,
}
impl FromStr for Compression {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "zstd" => Compression::Zstd,
            "gzip" => Compression::Gzip,
            "lz4" => Compression::Lz4,
            _ => anyhow::bail!("unknown compression algorithm"),
        })
    }
}

// TODO: this is currently duplicated with the service,
// we should move that out into a shared crate that has these definitions
/// The storage scope for each object
///
/// Each object is stored within a scope. The scope is used for access control, as well as the ability
/// to quickly run queries on all the objects associated with a scope.
/// The scope could also be used as a sharding/routing key in the future.
///
/// The organization / project scope defined here is hierarchical in the sense that
/// analytical aggregations on an organzation level take into account all the project-level objects.
/// However, accessing an object requires supplying both of these original values in order to retrieve it.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct Scope {
    /// The organization ID
    pub organization: u64,

    /// The project ID, if we have a project scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<u64>,
}

/// Service for storing and retrieving objects.
///
/// The Service contains the base configuration to connect to a service.
/// It has to be further initialized with credentials using the
/// [`for_organization`](Self::for_organization) and
/// [`for_project`](Self::for_project) functions.
pub struct StorageService {
    service_url: Arc<str>,
    client: reqwest::Client,
    jwt_key: EncodingKey,
    usecase: Arc<str>,
}
impl fmt::Debug for StorageService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageService")
            .field("service_url", &self.service_url)
            .field("client", &self.client)
            .field("jwt_key", &format_args!("[JWT Key]"))
            .field("usecase", &self.usecase)
            .finish()
    }
}

impl StorageService {
    /// Creates a new [`StorageService`].
    ///
    /// This service instance is configured to target the given `service_url`, using the `jwt_secret`
    /// for authentication.
    /// It is also scoped for the given `usecase`.
    ///
    /// In order to get or put objects, one has to create a [`StorageClient`] using the
    /// [`for_organization`](Self::for_organization) function.
    pub fn new(service_url: &str, jwt_secret: &str, usecase: &str) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            // we are dealing with de/compression ourselves:
            .no_brotli()
            .no_deflate()
            .no_gzip()
            .no_zstd()
            .build()?;
        let jwt_key = EncodingKey::from_secret(jwt_secret.as_bytes());

        Ok(Self {
            service_url: service_url.trim_end_matches('/').into(),
            client,

            jwt_key,
            usecase: usecase.into(),
        })
    }

    fn make_client(&self, scope: Scope) -> StorageClient {
        StorageClient {
            service_url: self.service_url.clone(),
            client: self.client.clone(),
            jwt_key: self.jwt_key.clone(),

            usecase: self.usecase.clone(),
            scope,
        }
    }

    /// Create a new [`StorageClient`] scoped to the given organization.
    pub fn for_organization(&self, organization_id: u64) -> StorageClient {
        self.make_client(Scope {
            organization: organization_id,
            project: None,
        })
    }

    /// Create a new [`StorageClient`] scoped to the given organization/project.
    pub fn for_project(&self, organization_id: u64, project_id: u64) -> StorageClient {
        self.make_client(Scope {
            organization: organization_id,
            project: Some(project_id),
        })
    }
}

#[derive(Debug, Deserialize)]
struct PutResponse {
    key: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
struct Claims<'a> {
    exp: u64,
    usecase: &'a str,
    scope: &'a Scope,
    permissions: &'a [&'a str],
}

/// A scoped objectstore client that can access objects in a specific use case and scope.
pub struct StorageClient {
    client: reqwest::Client,
    service_url: Arc<str>,
    jwt_key: EncodingKey,

    usecase: Arc<str>,
    scope: Scope,
}
impl fmt::Debug for StorageClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageClient")
            .field("client", &self.client)
            .field("service_url", &self.service_url)
            .field("jwt_key", &format_args!("[JWT Key]"))
            .field("usecase", &self.usecase)
            .field("scope", &self.scope)
            .finish()
    }
}

/// The type of [`Stream`](futures_core::Stream) to be used for a PUT request.
pub type ClientStream = BoxStream<'static, anyhow::Result<Bytes>>;

/// The result from a successful [`get()``](StorageClient::get) call.
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

impl StorageClient {
    fn make_authorization(&self, permission: &str) -> anyhow::Result<String> {
        let claims = Claims {
            exp: jsonwebtoken::get_current_timestamp(),
            usecase: &self.usecase,
            scope: &self.scope,
            permissions: &[permission],
        };
        let header = Header::default();

        let token = jsonwebtoken::encode(&header, &claims, &self.jwt_key)?;
        Ok(token)
    }

    /// Creates a PUT request using the optional `id`.
    pub fn put<'a>(&'a self, id: impl Into<Option<&'a str>>) -> PutBuilder<'a, ()> {
        PutBuilder {
            client: self,
            id: id.into(),
            compression: None,
            body: PutBody::None,
            marker: PhantomData,
        }
    }

    /// Requests the object with the given `id`.
    pub async fn get(
        &self,
        id: &str,
        accept_compression: &[Compression],
    ) -> anyhow::Result<Option<GetResult>> {
        let get_url = format!("{}/{id}", self.service_url);
        let authorization = self.make_authorization("read")?;

        let mut builder = self
            .client
            .get(get_url)
            .header(header::AUTHORIZATION, authorization);

        let mut accept_encoding = String::new();
        for compression in accept_compression {
            let s = match compression {
                Compression::Zstd => "zstd",
                Compression::Gzip => "gzip",
                Compression::Lz4 => "lz4",
                _ => continue,
            };
            accept_encoding.push_str(s);
            accept_encoding.push(',');
        }
        if !accept_encoding.is_empty() {
            builder = builder.header(header::ACCEPT_ENCODING, accept_encoding);
        }

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
            Some(Compression::from_str(compression?)?)
        } else {
            None
        };

        let stream = response.bytes_stream();
        if let Some(compression) = compression
            && !accept_compression.contains(&compression)
        {
            if compression != Compression::Zstd {
                anyhow::bail!("Transparent decoding of anything buf `zstd` is not implemented yet");
            }

            let stream = StreamReader::new(stream.map_err(std::io::Error::other));
            let decoder = ZstdDecoder::new(stream);
            let stream = ReaderStream::new(decoder)
                .map_err(anyhow::Error::from)
                .boxed();
            return Ok(Some(GetResult {
                stream,
                compression: None,
            }));
        }

        let stream = stream.map_err(anyhow::Error::from).boxed();
        Ok(Some(GetResult {
            stream,
            compression,
        }))
    }

    /// Deletes the object with the given `id`.
    pub async fn delete(&self, id: &str) -> anyhow::Result<()> {
        let delete_url = format!("{}/{id}", self.service_url);
        let authorization = self.make_authorization("write")?;

        let _response = self
            .client
            .delete(delete_url)
            .header(header::AUTHORIZATION, authorization)
            .send()
            .await?;

        Ok(())
    }
}

/// A PUT request builder.
pub struct PutBuilder<'a, Body> {
    client: &'a StorageClient,
    id: Option<&'a str>,
    compression: Option<Compression>,

    body: PutBody,
    marker: PhantomData<Body>,
}

impl<'a, Body> fmt::Debug for PutBuilder<'a, Body> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PutBuilder")
            .field("client", &self.client)
            .field("id", &self.id)
            .field("compression", &self.compression)
            .field("body", &format_args!("[Body]"))
            .finish()
    }
}

/// A typestate marker to denote put requests that have a body and can thus be sent.
#[derive(Debug)]
pub enum HasBodyMarker {}

enum PutBody {
    None,
    Buffer(Bytes),
    Stream(ClientStream),
}

impl<'a, B> PutBuilder<'a, B> {
    /// Sets the compression of the payload to be uploaded.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Uploads an in-memory buffer.
    pub fn buffer(self, buffer: impl Into<Bytes>) -> PutBuilder<'a, HasBodyMarker> {
        PutBuilder {
            client: self.client,
            id: self.id,
            compression: self.compression,
            body: PutBody::Buffer(buffer.into()),
            marker: PhantomData,
        }
    }

    /// Uploads an async `Stream`.
    pub fn stream(self, stream: ClientStream) -> PutBuilder<'a, HasBodyMarker> {
        PutBuilder {
            client: self.client,
            id: self.id,
            compression: self.compression,
            body: PutBody::Stream(stream),
            marker: PhantomData,
        }
    }
}
impl<'a> PutBuilder<'a, HasBodyMarker> {
    /// Sends the built PUT request to the upstream service.
    pub async fn send(self) -> anyhow::Result<String> {
        let put_url = format!(
            "{}/{}",
            self.client.service_url,
            self.id.unwrap_or_default()
        );
        let authorization = self.client.make_authorization("write")?;

        let mut builder = self
            .client
            .client
            .put(put_url)
            .header(header::AUTHORIZATION, authorization);

        let (body, content_encoding) = match self.compression {
            None => {
                let body = match self.body {
                    PutBody::None => unreachable!(),
                    PutBody::Buffer(bytes) => {
                        let cursor = Cursor::new(bytes);
                        let encoder = ZstdEncoder::new(cursor);
                        let stream = ReaderStream::new(encoder);
                        Body::wrap_stream(stream)
                    }
                    PutBody::Stream(stream) => {
                        let stream = StreamReader::new(stream.map_err(std::io::Error::other));
                        let encoder = ZstdEncoder::new(stream);
                        let stream = ReaderStream::new(encoder);
                        Body::wrap_stream(stream)
                    }
                };
                (body, Some("zstd"))
            }
            Some(compression) => {
                let body = match self.body {
                    PutBody::None => unreachable!(),
                    PutBody::Buffer(bytes) => bytes.into(),
                    PutBody::Stream(stream) => Body::wrap_stream(stream),
                };
                let encoding = match compression {
                    Compression::Zstd => Some("zstd"),
                    Compression::Gzip => Some("gzip"),
                    Compression::Lz4 => Some("lz4"),
                    Compression::Uncompressible => None,
                };
                (body, encoding)
            }
        };
        if let Some(content_encoding) = content_encoding {
            builder = builder.header(header::CONTENT_ENCODING, content_encoding);
        }

        let response = builder.body(body).send().await?;

        let PutResponse { key } = response.json().await?;
        Ok(key)
    }
}
