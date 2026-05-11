use std::borrow::Cow;
use std::collections::BTreeMap;

use base64::Engine as _;
use bytes::Bytes;
use futures_util::StreamExt as _;
use objectstore_types::metadata::Metadata;
use objectstore_types::multipart::{
    CompleteErrorDetail, CompleteRequest, CompleteSuccessResponse, InitiateResponse,
    ListPartsResponse, UploadPartResponse,
};
use reqwest::Body;
use serde::Deserialize;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

use crate::{ClientStream, ObjectKey, Session};

pub use objectstore_types::multipart::CompletePart;
pub use objectstore_types::multipart::ETag;
pub use objectstore_types::multipart::PartInfo;
pub use objectstore_types::multipart::UploadId;

#[derive(Deserialize)]
#[serde(untagged)]
enum CompleteResponse {
    Error { error: CompleteErrorDetail },
    Success(CompleteSuccessResponse),
}

impl Session {
    /// Creates a builder for initiating a multipart upload.
    ///
    /// The returned [`InitiateMultipartBuilder`] inherits the session's default compression
    /// and expiration settings.
    ///
    /// IMPORTANT: unlike single-object uploads, the client does not automatically compress the
    /// contents of [`MultipartUpload::put`]/[`MultipartUpload::put_stream`] based on the
    /// configured `compression`.
    /// The caller is responsible to compress the payload in accordance with the configured
    /// `compression`.
    /// That's because we require `content_length` on each part to be the length of the compressed
    /// content, which we wouldn't be able to know beforehand if `objectstore_client` automatically
    /// compressed payloads on the fly.
    pub fn initiate_multipart_upload(&self) -> InitiateMultipartBuilder {
        let metadata = Metadata {
            expiration_policy: self.scope.usecase().expiration_policy(),
            compression: self.scope.usecase().compression(),
            ..Default::default()
        };

        InitiateMultipartBuilder {
            session: self.clone(),
            metadata,
            key: None,
        }
    }

    /// Resumes an existing multipart upload from its key and upload ID.
    ///
    /// This reconstructs a [`MultipartUpload`] handle from previously obtained identifiers, and
    /// doesn't make any network calls.
    /// Use this to resume an upload after a process restart or to continue an upload initiated elsewhere.
    pub fn resume_multipart_upload(
        &self,
        key: impl Into<ObjectKey>,
        upload_id: impl Into<UploadId>,
    ) -> MultipartUpload {
        MultipartUpload {
            session: self.clone(),
            key: key.into(),
            upload_id: upload_id.into(),
        }
    }
}

/// A builder for initiating a multipart upload.
#[derive(Debug)]
pub struct InitiateMultipartBuilder {
    session: Session,
    metadata: Metadata,
    key: Option<ObjectKey>,
}

impl InitiateMultipartBuilder {
    /// Sets an explicit object key.
    ///
    /// If a key is specified, the object will be stored under that key. Otherwise, the Objectstore
    /// server will automatically assign a random key, which is then returned from this request.
    pub fn key(mut self, key: impl Into<ObjectKey>) -> Self {
        self.key = Some(key.into()).filter(|k| !k.is_empty());
        self
    }

    /// Sets the compression algorithm recorded in this object's metadata.
    ///
    /// IMPORTANT: unlike single-object uploads, the client does not automatically compress the
    /// contents of [`MultipartUpload::put`]/[`MultipartUpload::put_stream`] based on the
    /// configured `compression`.
    /// The caller is responsible to compress the payload in accordance with the configured
    /// `compression`.
    ///
    /// By default, the compression algorithm set on this Session's Usecase is used.
    pub fn compression(mut self, compression: impl Into<Option<crate::Compression>>) -> Self {
        self.metadata.compression = compression.into();
        self
    }

    /// Sets the expiration policy of the object to be uploaded.
    ///
    /// By default, the expiration policy set on this Session's Usecase is used.
    pub fn expiration_policy(mut self, expiration_policy: crate::ExpirationPolicy) -> Self {
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

    /// Sets the origin of the object, typically the IP address of the original source.
    ///
    /// This is an optional but encouraged field that tracks where the payload was
    /// originally obtained from. For example, the IP address of the Sentry SDK or CLI
    /// that uploaded the data.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(session: objectstore_client::Session) {
    /// session.initiate_multipart_upload()
    ///     .origin("203.0.113.42")
    ///     .send()
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn origin(mut self, origin: impl Into<String>) -> Self {
        self.metadata.origin = Some(origin.into());
        self
    }

    /// Sets the custom metadata to the provided map.
    ///
    /// It will clear any previously set metadata.
    pub fn set_metadata(mut self, metadata: impl Into<BTreeMap<String, String>>) -> Self {
        self.metadata.custom = metadata.into();
        self
    }

    /// Appends the `key`/`value` to the custom metadata of this object.
    pub fn append_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.custom.insert(key.into(), value.into());
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
                .multipart_request(method, None, self.key.as_deref(), None)?;

        builder = builder.headers(self.metadata.to_headers("")?);

        let response: InitiateResponse = builder.send().await?.error_for_status()?.json().await?;

        Ok(MultipartUpload {
            session: self.session,
            key: response.key,
            upload_id: response.upload_id,
        })
    }
}

/// Represents an ongoing Multipart Upload, tied to a specific [`Session`] and [`UploadId`].
///
/// Create a Multipart Upload handle using [`Session::initiate_multipart_upload`] or [`Session::resume_multipart_upload`].
#[derive(Debug)]
pub struct MultipartUpload {
    session: Session,
    key: String,
    upload_id: UploadId,
}

impl MultipartUpload {
    /// Returns the upload session identifier.
    pub fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    /// Returns the key of the object that this upload will create.
    pub fn key(&self) -> &ObjectKey {
        &self.key
    }

    /// Uploads a part using a [`Bytes`]-like payload.
    ///
    /// IMPORTANT: unlike single-object uploads, the client does not automatically compress
    /// contents based on this upload's `Metadata::compression`.
    /// The caller is responsible to compress the payload in accordance with the `compression`,
    /// and, optionally, to pass the `content_md5` of the compressed payload.
    pub async fn put(
        &self,
        body: impl Into<Bytes>,
        part_number: u32,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<CompletePart> {
        let bytes = body.into();
        let content_length = bytes.len() as u64;
        self.upload_part(bytes.into(), part_number, content_length, content_md5)
            .await
    }

    /// Uploads a part using a streaming payload.
    ///
    /// IMPORTANT: unlike single-object uploads, the client does not automatically compress
    /// contents based on this upload's `Metadata::compression`.
    /// The caller is responsible to compress the payload in accordance with the `compression`,
    /// and to pass the `content_length` and, optionally, `content_md5` of the compressed payload.
    pub async fn put_stream(
        &self,
        stream: ClientStream,
        part_number: u32,
        content_length: u64,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<CompletePart> {
        self.upload_part(
            Body::wrap_stream(stream),
            part_number,
            content_length,
            content_md5,
        )
        .await
    }

    /// Uploads a part from an [`AsyncRead`] source.
    ///
    /// IMPORTANT: unlike single-object uploads, the client does not automatically compress
    /// contents based on this upload's `Metadata::compression`.
    /// The caller is responsible to compress the payload in accordance with the `compression`,
    /// and to pass the `content_length` and, optionally, `content_md5` of the compressed payload.
    pub async fn put_read<R>(
        &self,
        reader: R,
        part_number: u32,
        content_length: u64,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<CompletePart>
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        let stream = ReaderStream::new(reader).boxed();
        self.put_stream(stream, part_number, content_length, content_md5)
            .await
    }

    async fn upload_part(
        &self,
        body: Body,
        part_number: u32,
        content_length: u64,
        content_md5: Option<&[u8; 16]>,
    ) -> crate::Result<CompletePart> {
        let mut builder = self
            .session
            .multipart_request(
                reqwest::Method::PUT,
                Some("parts"),
                Some(&self.key),
                Some(vec![
                    ("upload_id", self.upload_id.clone()),
                    ("part_number", part_number.to_string()),
                ]),
            )?
            .header(reqwest::header::CONTENT_LENGTH, content_length)
            .body(body);

        if let Some(md5) = content_md5 {
            let encoded = base64::engine::general_purpose::STANDARD.encode(md5);
            builder = builder.header("content-md5", encoded);
        }

        let response: UploadPartResponse = builder.send().await?.error_for_status()?.json().await?;
        Ok(CompletePart {
            part_number,
            etag: response.etag,
        })
    }

    /// Lists all parts that have been uploaded for this multipart upload.
    pub async fn list_parts(&self) -> crate::Result<Vec<PartInfo>> {
        let mut all_parts = Vec::new();
        let mut marker = None;

        loop {
            let page = self.list_parts_page(None, marker).await?;
            all_parts.extend(page.parts);

            if !page.is_truncated {
                return Ok(all_parts);
            }
            marker = page.next_part_number_marker;
            if marker.is_none() {
                return Err(crate::Error::MalformedResponse(
                    "server returned is_truncated=true but no next_part_number_marker. Please report a bug.".into(),
                ));
            }
        }
    }

    async fn list_parts_page(
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
            Some(params),
        )?;

        let response: ListPartsResponse = builder.send().await?.error_for_status()?.json().await?;
        Ok(response)
    }

    /// Aborts this multipart upload.
    pub async fn abort(self) -> crate::Result<()> {
        let builder = self.session.multipart_request(
            reqwest::Method::DELETE,
            None,
            Some(&self.key),
            Some(vec![("upload_id", self.upload_id)]),
        )?;
        builder.send().await?.error_for_status()?;
        Ok(())
    }

    /// Completes the multipart upload, assembling all parts into the final object.
    pub async fn complete(
        self,
        parts: impl IntoIterator<Item = CompletePart>,
    ) -> crate::Result<ObjectKey> {
        let builder = self
            .session
            .multipart_request(
                reqwest::Method::POST,
                Some("complete"),
                Some(&self.key),
                Some(vec![("upload_id", self.upload_id)]),
            )?
            .json(&CompleteRequest {
                parts: parts.into_iter().collect(),
            });

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
