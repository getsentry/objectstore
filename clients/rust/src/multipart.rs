use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Cursor;

use async_compression::tokio::bufread::ZstdEncoder;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::metadata::Metadata;
use objectstore_types::multipart::{
    CompleteErrorDetail, CompletePart, CompleteRequest, CompleteSuccessResponse, InitiateResponse,
    ListPartsResponse, UploadPartResponse,
};
use reqwest::Body;
use serde::Deserialize;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::put::MetadataBuilder;
use crate::{ClientStream, ObjectKey, Session};

pub use objectstore_types::multipart::CompletePart as MultipartCompletePart;

#[derive(Deserialize)]
#[serde(untagged)]
enum CompleteResponse {
    Error { error: CompleteErrorDetail },
    Success(CompleteSuccessResponse),
}

impl Session {
    /// Creates a builder for initiating a multipart upload.
    pub fn create_multipart_upload(&self) -> InitiateBuilder {
        let metadata = Metadata {
            expiration_policy: self.scope.usecase().expiration_policy(),
            compression: Some(self.scope.usecase().compression()),
            ..Default::default()
        };

        InitiateBuilder {
            session: self.clone(),
            metadata,
            key: None,
        }
    }
}

/// A builder for initiating a multipart upload.
#[derive(Debug)]
pub struct InitiateBuilder {
    session: Session,
    metadata: Metadata,
    key: Option<ObjectKey>,
}

impl MetadataBuilder for InitiateBuilder {
    fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }
}

impl InitiateBuilder {
    /// Sets an explicit object key.
    ///
    /// If a key is specified, the object will be stored under that key. Otherwise, the Objectstore
    /// server will automatically assign a random key, which is then returned from the initiate
    /// request.
    pub fn key(mut self, key: impl Into<ObjectKey>) -> Self {
        self.key = Some(key.into()).filter(|k| !k.is_empty());
        self
    }

    /// Sets an explicit compression algorithm.
    ///
    /// By default, the compression algorithm set on this Session's Usecase is used.
    /// When set, each uploaded part is compressed client-side before it is sent.
    pub fn compression(self, compression: impl Into<Option<crate::Compression>>) -> Self {
        MetadataBuilder::compression(self, compression)
    }

    /// Sets the expiration policy.
    pub fn expiration_policy(self, expiration_policy: crate::ExpirationPolicy) -> Self {
        MetadataBuilder::expiration_policy(self, expiration_policy)
    }

    /// Sets the content type.
    pub fn content_type(self, content_type: impl Into<Cow<'static, str>>) -> Self {
        MetadataBuilder::content_type(self, content_type)
    }

    /// Sets the origin.
    pub fn origin(self, origin: impl Into<String>) -> Self {
        MetadataBuilder::origin(self, origin)
    }

    /// Sets the custom metadata map.
    pub fn set_metadata(self, metadata: impl Into<BTreeMap<String, String>>) -> Self {
        MetadataBuilder::set_metadata(self, metadata)
    }

    /// Appends a key/value to the custom metadata.
    pub fn append_metadata(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        MetadataBuilder::append_metadata(self, key, value)
    }

    /// Sends the initiate request and returns a [`MultipartUpload`] handle.
    pub async fn send(self) -> crate::Result<MultipartUpload> {
        let method = match self.key {
            Some(_) => reqwest::Method::PUT,
            None => reqwest::Method::POST,
        };

        let mut builder =
            self.session
                .multipart_request(method, None, self.key.as_deref(), vec![])?;

        builder = builder.headers(self.metadata.to_headers("")?);

        let response: InitiateResponse = builder.send().await?.error_for_status()?.json().await?;

        Ok(MultipartUpload {
            session: self.session,
            key: response.key,
            upload_id: response.upload_id,
            compression: self.metadata.compression,
        })
    }
}

/// Handle to an in-progress multipart upload.
///
/// Returned by [`InitiateBuilder::send`]. Use it to upload parts, list parts,
/// and complete or abort the upload.
#[derive(Debug)]
pub struct MultipartUpload {
    session: Session,
    key: String,
    upload_id: String,
    compression: Option<crate::Compression>,
}

impl MultipartUpload {
    /// Returns the upload session identifier.
    pub fn id(&self) -> &str {
        &self.upload_id
    }

    /// Returns the object key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Uploads a part from an in-memory buffer.
    ///
    /// An optional raw MD5 digest can be provided for server-side integrity
    /// verification. When compression is enabled on this upload, the digest
    /// must match the transmitted compressed part bytes.
    pub async fn put(
        &self,
        body: impl Into<Bytes>,
        part_number: u32,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<String> {
        let (body, content_length) = self
            .prepare_part_body(MultipartPart::Buffer(body.into()), None)
            .await?;
        self.upload_part(body, content_length, part_number, content_md5)
            .await
    }

    /// Uploads a part from a stream. The caller must provide the exact content length.
    ///
    /// An optional raw MD5 digest can be provided for server-side integrity
    /// verification. When compression is enabled on this upload, the digest
    /// must match the transmitted compressed part bytes.
    pub async fn put_stream(
        &self,
        stream: ClientStream,
        content_length: u64,
        part_number: u32,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<String> {
        let (body, content_length) = self
            .prepare_part_body(MultipartPart::Stream(stream), Some(content_length))
            .await?;
        self.upload_part(body, content_length, part_number, content_md5)
            .await
    }

    async fn prepare_part_body(
        &self,
        part: MultipartPart,
        content_length: Option<u64>,
    ) -> crate::Result<(Body, u64)> {
        match (self.compression, part) {
            (None, MultipartPart::Buffer(bytes)) => Ok((bytes.clone().into(), bytes.len() as u64)),
            (None, MultipartPart::Stream(stream)) => Ok((
                Body::wrap_stream(stream),
                content_length.expect("stream parts require content_length"),
            )),
            (Some(crate::Compression::Zstd), MultipartPart::Buffer(bytes)) => {
                let stream = ReaderStream::new(ZstdEncoder::new(Cursor::new(bytes)))
                    .map_err(std::io::Error::other)
                    .boxed();
                let compressed = collect_stream_bytes(stream).await?;
                Ok((compressed.clone().into(), compressed.len() as u64))
            }
            (Some(crate::Compression::Zstd), MultipartPart::Stream(stream)) => {
                let stream = ReaderStream::new(ZstdEncoder::new(StreamReader::new(stream)))
                    .map_err(std::io::Error::other)
                    .boxed();
                let compressed = collect_stream_bytes(stream).await?;
                Ok((compressed.clone().into(), compressed.len() as u64))
            }
        }
    }

    async fn upload_part(
        &self,
        body: Body,
        content_length: u64,
        part_number: u32,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<String> {
        use base64::Engine;

        let mut builder = self
            .session
            .multipart_request(
                reqwest::Method::PUT,
                Some("parts"),
                Some(&self.key),
                vec![
                    ("upload_id", self.upload_id.clone()),
                    ("part_number", part_number.to_string()),
                ],
            )?
            .header(reqwest::header::CONTENT_LENGTH, content_length)
            .body(body);

        if let Some(md5) = content_md5 {
            let encoded = base64::engine::general_purpose::STANDARD.encode(md5);
            builder = builder.header("content-md5", encoded);
        }

        let response: UploadPartResponse = builder.send().await?.error_for_status()?.json().await?;
        Ok(response.etag)
    }

    /// Lists the parts that have been uploaded for this multipart upload.
    pub async fn list_parts(
        &self,
        max_parts: Option<u32>,
        part_number_marker: Option<u32>,
    ) -> crate::Result<ListPartsResponse> {
        let mut params = vec![("upload_id", self.upload_id.clone())];
        if let Some(max) = max_parts {
            params.push(("max_parts", max.to_string()));
        }
        if let Some(marker) = part_number_marker {
            params.push(("part_number_marker", marker.to_string()));
        }

        let builder = self.session.multipart_request(
            reqwest::Method::GET,
            Some("parts"),
            Some(&self.key),
            params,
        )?;

        let response: ListPartsResponse = builder.send().await?.error_for_status()?.json().await?;
        Ok(response)
    }

    /// Aborts this multipart upload, discarding any uploaded parts.
    pub async fn abort(self) -> crate::Result<()> {
        let MultipartUpload {
            session,
            key,
            upload_id,
            compression: _,
        } = self;
        let builder = session.multipart_request(
            reqwest::Method::DELETE,
            None,
            Some(&key),
            vec![("upload_id", upload_id)],
        )?;
        builder.send().await?.error_for_status()?;
        Ok(())
    }

    /// Completes the multipart upload, assembling all parts into the final object.
    ///
    /// Returns the final object key on success. The server may return an error in
    /// the response body even with HTTP 200 (following the S3 pattern), which is
    /// surfaced as [`crate::Error::MultipartComplete`].
    pub async fn complete(self, parts: Vec<CompletePart>) -> crate::Result<String> {
        let MultipartUpload {
            session,
            key,
            upload_id,
            compression: _,
        } = self;
        let builder = session
            .multipart_request(
                reqwest::Method::POST,
                Some("complete"),
                Some(&key),
                vec![("upload_id", upload_id)],
            )?
            .json(&CompleteRequest { parts });

        // The complete endpoint streams whitespace as keepalive before the JSON
        // payload. serde_json (used by reqwest's .json()) skips leading whitespace,
        // so we can deserialize directly.
        //
        // The response is always HTTP 200 (S3 pattern) — errors are in the body.
        let response = builder.send().await?.error_for_status()?;
        match response.json::<CompleteResponse>().await? {
            CompleteResponse::Success(s) => Ok(s.key),
            CompleteResponse::Error { error } => Err(crate::Error::MultipartComplete {
                code: error.code,
                message: error.message,
            }),
        }
    }
}

enum MultipartPart {
    Buffer(Bytes),
    Stream(ClientStream),
}

async fn collect_stream_bytes(stream: ClientStream) -> crate::Result<Bytes> {
    let bytes = stream.try_collect::<bytes::BytesMut>().await?;
    Ok(bytes.freeze())
}
