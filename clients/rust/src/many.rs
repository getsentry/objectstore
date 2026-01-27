use std::collections::HashMap;
use std::io;

use async_compression::tokio::bufread::ZstdDecoder;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use futures_util::StreamExt as _;
use multer::Field;
use objectstore_types::{Compression, Metadata};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::multipart::Part;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::error::Error;
use crate::put::{PutBody, compress_body};
use crate::{
    DeleteBuilder, DeleteResponse, GetBuilder, GetResponse, ObjectKey, PutBuilder, PutResponse,
    Session,
};

const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";
const HEADER_BATCH_OPERATION_KIND: &str = "x-sn-batch-operation-kind";
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

/// Maximum number of operations per batch request (server limit).
const MAX_BATCH_SIZE: usize = 1000;

/// A builder that can be used to enqueue multiple operations.
///
/// The client can optionally execute the operations as batch requests, leading to
/// reduced network overhead.
#[derive(Debug)]
pub struct ManyBuilder {
    session: Session,
    operations: Vec<BatchOperation>,
}

impl Session {
    /// Creates as [`ManyBuilder`] associated with this session.
    ///
    /// A [`ManyBuilder`] can be used to execute multiple operations, which the client can choose to
    /// execute as batch requests via a dedicated endpoint, minimizing network overhead.
    pub fn many(&self) -> ManyBuilder {
        ManyBuilder {
            session: self.clone(),
            operations: vec![],
        }
    }
}

/// An individual operation in a batch request.
#[derive(Debug)]
pub(crate) enum BatchOperation {
    /// A GET operation.
    Get {
        /// The object key.
        key: ObjectKey,
        /// Whether to decompress the response.
        decompress: bool,
    },
    /// An INSERT operation.
    Insert {
        /// The object key.
        key: ObjectKey,
        /// The object metadata.
        metadata: Metadata,
        /// The object body.
        body: PutBody,
    },
    /// A DELETE operation.
    Delete {
        /// The object key.
        key: ObjectKey,
    },
}

impl From<GetBuilder> for BatchOperation {
    fn from(value: GetBuilder) -> Self {
        BatchOperation::Get {
            key: value.key,
            decompress: value.decompress,
        }
    }
}

impl From<PutBuilder> for BatchOperation {
    fn from(value: PutBuilder) -> Self {
        let key = value
            .key
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        BatchOperation::Insert {
            key,
            metadata: value.metadata,
            body: value.body,
        }
    }
}

impl From<DeleteBuilder> for BatchOperation {
    fn from(value: DeleteBuilder) -> Self {
        BatchOperation::Delete { key: value.key }
    }
}

impl BatchOperation {
    async fn into_part(self) -> crate::Result<Part> {
        match self {
            BatchOperation::Get { key, .. } => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HeaderName::from_static(HEADER_BATCH_OPERATION_KIND),
                    HeaderValue::from_static("get"),
                );
                headers.insert(
                    HeaderName::from_static(HEADER_BATCH_OPERATION_KEY),
                    key_to_header_value(&key)?,
                );
                Ok(Part::text("").headers(headers))
            }
            BatchOperation::Insert {
                key,
                metadata,
                body,
            } => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HeaderName::from_static(HEADER_BATCH_OPERATION_KIND),
                    HeaderValue::from_static("insert"),
                );
                headers.insert(
                    HeaderName::from_static(HEADER_BATCH_OPERATION_KEY),
                    key_to_header_value(&key)?,
                );
                // Add metadata headers (includes compression info)
                headers.extend(metadata.to_headers("", false)?);

                let body = compress_body(body, metadata.compression);
                Ok(Part::stream(body).headers(headers))
            }
            BatchOperation::Delete { key } => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HeaderName::from_static(HEADER_BATCH_OPERATION_KIND),
                    HeaderValue::from_static("delete"),
                );
                headers.insert(
                    HeaderName::from_static(HEADER_BATCH_OPERATION_KEY),
                    key_to_header_value(&key)?,
                );
                Ok(Part::text("").headers(headers))
            }
        }
    }
}

fn key_to_header_value(key: &str) -> crate::Result<HeaderValue> {
    let encoded = BASE64_STANDARD.encode(key.as_bytes());
    HeaderValue::try_from(encoded)
        .map_err(|e| Error::MalformedResponse(format!("invalid object key for header value: {e}")))
}

/// The result of an individual operation.
#[derive(Debug)]
pub enum OperationResult {
    /// The result of a get operation. Returns `Ok(None)` if the object was not found.
    Get(ObjectKey, Result<Option<GetResponse>, Error>),
    /// The result of a put operation.
    Put(ObjectKey, Result<PutResponse, Error>),
    /// The result of a delete operation.
    Delete(ObjectKey, Result<DeleteResponse, Error>),
    /// The server returned an error for one of the operations, but it's impossible to determine
    /// which one exactly.
    /// This can happen if either the client or the server encounter parsing errors or unexpected
    /// request or response formats.
    Error(Error),
}

impl OperationResult {
    async fn from_field(field: Field<'_>, decompress_map: &HashMap<ObjectKey, bool>) -> Self {
        match Self::try_from_field(field, decompress_map).await {
            Ok(result) => result,
            Err(e) => OperationResult::Error(e),
        }
    }

    async fn try_from_field(
        field: Field<'_>,
        decompress_map: &HashMap<ObjectKey, bool>,
    ) -> Result<Self, Error> {
        let mut headers = field.headers().clone();
        let key = headers.remove(HEADER_BATCH_OPERATION_KEY).and_then(|v| {
            v.to_str().ok().and_then(|encoded| {
                BASE64_STANDARD
                    .decode(encoded)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            })
        });
        let kind = headers
            .remove(HEADER_BATCH_OPERATION_KIND)
            .and_then(|v| v.to_str().ok().map(|s| s.to_owned()));
        let status: u16 = headers
            .remove(HEADER_BATCH_OPERATION_STATUS)
            .and_then(|v| {
                v.to_str().ok().and_then(|s| {
                    // Status header format is "code reason" (e.g., "200 OK")
                    // Split on first space and parse the code
                    s.split_once(' ')
                        .map(|(code, _)| code)
                        .unwrap_or(s)
                        .parse()
                        .ok()
                })
            })
            .ok_or_else(|| {
                Error::MalformedResponse(format!(
                    "missing or invalid {HEADER_BATCH_OPERATION_STATUS} header"
                ))
            })?;

        // If both key and kind are missing, this is a request-level error (e.g., rate limiting)
        // that applies to the entire batch rather than a specific operation.
        let (key, kind) = match (key, kind) {
            (Some(k), Some(kd)) => (k, kd),
            (None, None) => {
                // Request-level error: extract error details from body and status
                let body = field.bytes().await?;
                let message = String::from_utf8_lossy(&body).into_owned();
                return Err(Error::OperationError { status, message });
            }
            _ => {
                // One header present but not the other is malformed
                return Err(Error::MalformedResponse(format!(
                    "inconsistent batch headers: both {HEADER_BATCH_OPERATION_KEY} and \
                     {HEADER_BATCH_OPERATION_KIND} must be present or absent together"
                )));
            }
        };

        let body = field.bytes().await?;

        let is_error = status >= 400 && !(kind == "get" && status == 404);
        if is_error {
            let message = String::from_utf8_lossy(&body).into_owned();
            let error = Error::OperationError { status, message };

            return match kind.as_str() {
                "get" => Ok(OperationResult::Get(key, Err(error))),
                "insert" => Ok(OperationResult::Put(key, Err(error))),
                "delete" => Ok(OperationResult::Delete(key, Err(error))),
                _ => Ok(OperationResult::Error(error)),
            };
        }

        match kind.as_str() {
            "get" => {
                if status == 404 {
                    Ok(OperationResult::Get(key, Ok(None)))
                } else {
                    let mut metadata = Metadata::from_headers(&headers, "")?;
                    let should_decompress = decompress_map.get(&key).copied().unwrap_or(false);

                    let stream = match (metadata.compression, should_decompress) {
                        (Some(Compression::Zstd), true) => {
                            metadata.compression = None;
                            let stream =
                                futures_util::stream::once(async move { Ok::<_, io::Error>(body) });
                            ReaderStream::new(ZstdDecoder::new(StreamReader::new(stream))).boxed()
                        }
                        _ => futures_util::stream::once(async move { Ok(body) }).boxed(),
                    };

                    Ok(OperationResult::Get(
                        key,
                        Ok(Some(GetResponse { metadata, stream })),
                    ))
                }
            }
            "insert" => Ok(OperationResult::Put(key.clone(), Ok(PutResponse { key }))),
            "delete" => Ok(OperationResult::Delete(key, Ok(()))),
            _ => Err(Error::MalformedResponse(format!(
                "unknown operation kind: {kind}, body: {}",
                String::from_utf8_lossy(&body)
            ))),
        }
    }
}

impl ManyBuilder {
    /// Executes all enqueued operations, returning an iterator over their results.
    /// The results are not guaranteed to be in the same order as the original enqueuing order.
    pub async fn send(mut self) -> crate::Result<impl Iterator<Item = OperationResult>> {
        let mut all_results = Vec::new();

        while !self.operations.is_empty() {
            let chunk_size = self.operations.len().min(MAX_BATCH_SIZE);
            let chunk: Vec<_> = self.operations.drain(..chunk_size).collect();
            let results = self.send_batch(chunk).await?;
            all_results.extend(results);
        }

        Ok(all_results.into_iter())
    }

    async fn send_batch(
        &self,
        operations: Vec<BatchOperation>,
    ) -> crate::Result<Vec<OperationResult>> {
        // Build decompress map for GET operations
        let decompress_map: HashMap<ObjectKey, bool> = operations
            .iter()
            .filter_map(|op| match op {
                BatchOperation::Get { key, decompress } => Some((key.clone(), *decompress)),
                _ => None,
            })
            .collect();

        let mut form = reqwest::multipart::Form::new();
        for (i, op) in operations.into_iter().enumerate() {
            let part = op.into_part().await?;
            form = form.part(format!("part{i}"), part);
        }

        let request = self.session.batch_request()?.multipart(form);
        let response = request.send().await?.error_for_status()?;

        let boundary = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Error::MalformedResponse("missing Content-Type header".to_owned()))
            .map(multer::parse_boundary)??;

        let stream = response.bytes_stream().map(|r| r.map_err(io::Error::other));
        let mut multipart = multer::Multipart::new(stream, boundary);

        let mut results = Vec::new();
        while let Some(field) = multipart.next_field().await? {
            results.push(OperationResult::from_field(field, &decompress_map).await);
        }

        Ok(results)
    }

    /// Enqueues an operation.
    ///
    /// This method takes a [`GetBuilder`]/[`PutBuilder`]/[`DeleteBuilder`], which you can
    /// construct using [`Session::get`]/[`Session::put`]/[`Session::delete`].
    #[allow(private_bounds)]
    pub fn push<B: Into<BatchOperation>>(mut self, builder: B) -> Self {
        self.operations.push(builder.into());
        self
    }
}
