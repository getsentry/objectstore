use bytes::Bytes;
use objectstore_types::metadata::Metadata;
use objectstore_types::multipart::{
    CompleteErrorDetail, CompletePart, CompleteRequest, CompleteSuccessResponse, InitiateResponse,
    ListPartsResponse, UploadPartResponse,
};
use reqwest::Body;
use serde::Deserialize;

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
    ///
    /// The returned [`InitiateBuilder`] inherits the session's default compression
    /// and expiration settings. Unlike single-object uploads, the client does
    /// **not** compress parts — the caller must pre-compress each part to match
    /// the compression algorithm set in metadata.
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
///
/// Metadata set here (compression, expiration, content type, etc.) is sent to
/// the server when [`send`](Self::send) is called. Note that unlike
/// single-object uploads, the client does **not** compress parts automatically —
/// if compression is configured, the caller must pre-compress each part before
/// uploading it via [`MultipartUpload::put`] or [`MultipartUpload::put_stream`].
#[derive(Debug)]
pub struct InitiateBuilder {
    session: Session,
    metadata: Metadata,
    key: Option<ObjectKey>,
}

impl InitiateBuilder {
    metadata_builder_methods!(metadata);

    /// Sets an explicit object key.
    ///
    /// If a key is specified, the object will be stored under that key. Otherwise, the Objectstore
    /// server will automatically assign a random key, which is then returned from the initiate
    /// request.
    pub fn key(mut self, key: impl Into<ObjectKey>) -> Self {
        self.key = Some(key.into()).filter(|k| !k.is_empty());
        self
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
        })
    }
}

/// Handle to an in-progress multipart upload.
///
/// Returned by [`InitiateBuilder::send`]. Use it to upload parts, list parts,
/// and complete or abort the upload.
///
/// Parts are uploaded as-is — the client does **not** compress them. If the
/// upload was initiated with compression metadata, the caller is responsible
/// for pre-compressing each part before calling [`put`](Self::put) or
/// [`put_stream`](Self::put_stream). `content_length` and `content_md5` always
/// refer to the bytes actually transmitted.
#[derive(Debug)]
pub struct MultipartUpload {
    session: Session,
    key: String,
    upload_id: String,
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
    /// verification. The digest must match the bytes being transmitted.
    pub async fn put(
        &self,
        body: impl Into<Bytes>,
        part_number: u32,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<String> {
        let bytes = body.into();
        let content_length = bytes.len() as u64;
        self.upload_part(bytes.into(), content_length, part_number, content_md5)
            .await
    }

    /// Uploads a part from a stream.
    ///
    /// The caller must provide the exact `content_length` of the stream.
    /// An optional raw MD5 digest can be provided for server-side integrity
    /// verification. The digest must match the bytes being transmitted.
    pub async fn put_stream(
        &self,
        stream: ClientStream,
        content_length: u64,
        part_number: u32,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<String> {
        self.upload_part(
            Body::wrap_stream(stream),
            content_length,
            part_number,
            content_md5,
        )
        .await
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
        let builder = self.session.multipart_request(
            reqwest::Method::DELETE,
            None,
            Some(&self.key),
            vec![("upload_id", self.upload_id)],
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
        let builder = self
            .session
            .multipart_request(
                reqwest::Method::POST,
                Some("complete"),
                Some(&self.key),
                vec![("upload_id", self.upload_id)],
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
