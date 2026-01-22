//! Batch operations for the Objectstore client.
//!
//! This module provides the ability to send multiple operations (GET, PUT, DELETE) in a single
//! HTTP request, improving efficiency when working with many objects.
//!
//! # Example
//!
//! ```no_run
//! use objectstore_client::{Client, Usecase};
//!
//! # async fn example() -> objectstore_client::Result<()> {
//! let client = Client::new("http://localhost:8888/")?;
//! let session = client.session(Usecase::new("my_app").for_organization(12345))?;
//!
//! let response = session
//!     .many()
//!     .add_put(session.put("Hello, world!").key("key1"))
//!     .add_get(session.get("key2"))
//!     .add_delete(session.delete("key3"))
//!     .send()
//!     .await?;
//!
//! for item in response.iter() {
//!     match item {
//!         objectstore_client::BatchResponseItem::Get { key, result } => {
//!             println!("GET {}: {:?}", key, result);
//!         }
//!         objectstore_client::BatchResponseItem::Put { key, result } => {
//!             println!("PUT {}: {:?}", key, result);
//!         }
//!         objectstore_client::BatchResponseItem::Delete { key, result } => {
//!             println!("DELETE {}: {:?}", key, result);
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::io::Cursor;

use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::Metadata;
use reqwest::header::{HeaderMap, CONTENT_TYPE};
use reqwest::multipart::{Form, Part};
use reqwest::Body;
use serde::Deserialize;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{ClientStream, Compression, DeleteBuilder, GetBuilder, PutBuilder, Session};

const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";
const HEADER_BATCH_OPERATION_KIND: &str = "x-sn-batch-operation-kind";
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

/// Maximum number of operations per batch request.
const MAX_OPERATIONS_PER_BATCH: usize = 1000;

/// Payload for a batch PUT operation - can be either bytes or a stream.
pub(crate) enum BatchPutPayload {
    Buffer(Bytes),
    Stream(ClientStream),
}

impl fmt::Debug for BatchPutPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchPutPayload::Buffer(b) => f.debug_tuple("Buffer").field(&b.len()).finish(),
            BatchPutPayload::Stream(_) => f.debug_tuple("Stream").finish(),
        }
    }
}

/// Internal representation of a batch operation.
pub(crate) enum BatchOperation {
    Get { key: String, decompress: bool },
    Put { key: String, payload: BatchPutPayload, metadata: Metadata },
    Delete { key: String },
}

impl fmt::Debug for BatchOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchOperation::Get { key, decompress } => f
                .debug_struct("Get")
                .field("key", key)
                .field("decompress", decompress)
                .finish(),
            BatchOperation::Put { key, metadata, .. } => f
                .debug_struct("Put")
                .field("key", key)
                .field("metadata", metadata)
                .finish(),
            BatchOperation::Delete { key } => f.debug_struct("Delete").field("key", key).finish(),
        }
    }
}

impl BatchOperation {
    fn kind(&self) -> &'static str {
        match self {
            BatchOperation::Get { .. } => "get",
            BatchOperation::Put { .. } => "insert",
            BatchOperation::Delete { .. } => "delete",
        }
    }
}

/// Lightweight metadata about an operation for response parsing.
/// This is kept separately since the full operation is consumed during encoding.
#[derive(Debug, Clone)]
enum OperationMeta {
    Get { key: String, decompress: bool },
    Put { key: String },
    Delete { key: String },
}

/// Error returned when a single operation in a batch fails.
#[derive(Debug, Clone)]
pub struct BatchOperationError {
    /// HTTP status code returned by the server.
    pub status: u16,
    /// Error detail message.
    pub detail: String,
    /// Optional causes.
    pub causes: Vec<String>,
}

impl std::fmt::Display for BatchOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "batch operation error (status {}): {}", self.status, self.detail)
    }
}

impl std::error::Error for BatchOperationError {}

/// Server error response body structure.
#[derive(Debug, Deserialize)]
struct ErrorResponseBody {
    detail: String,
    #[serde(default)]
    causes: Vec<String>,
}

/// Result of a single GET operation in a batch.
#[derive(Debug)]
pub enum BatchGetResult {
    /// Object found with metadata and payload.
    Found {
        /// The metadata attached to this object.
        metadata: Metadata,
        /// The object payload.
        payload: Bytes,
    },
    /// Object not found (404).
    NotFound,
}

/// Result of a single PUT operation in a batch.
#[derive(Debug)]
pub struct BatchPutResult {
    /// The key under which the object was stored.
    pub key: String,
}

/// Result of a single DELETE operation in a batch.
#[derive(Debug)]
pub struct BatchDeleteResult;

/// A single item from a batch response.
#[derive(Debug)]
pub enum BatchResponseItem {
    /// Result of a GET operation.
    Get {
        /// The object key.
        key: String,
        /// The operation result.
        result: Result<BatchGetResult, BatchOperationError>,
    },
    /// Result of a PUT operation.
    Put {
        /// The object key.
        key: String,
        /// The operation result.
        result: Result<BatchPutResult, BatchOperationError>,
    },
    /// Result of a DELETE operation.
    Delete {
        /// The object key.
        key: String,
        /// The operation result.
        result: Result<BatchDeleteResult, BatchOperationError>,
    },
}

/// Response from a batch request containing results for all operations.
#[derive(Debug)]
pub struct BatchResponse {
    items: Vec<BatchResponseItem>,
}

impl BatchResponse {
    /// Returns an iterator over all response items.
    pub fn iter(&self) -> impl Iterator<Item = &BatchResponseItem> {
        self.items.iter()
    }

    /// Returns the number of items in the response.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true if the response contains no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl IntoIterator for BatchResponse {
    type Item = BatchResponseItem;
    type IntoIter = std::vec::IntoIter<BatchResponseItem>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

/// Builder for constructing batch requests.
#[derive(Debug)]
pub struct BatchBuilder {
    session: Session,
    operations: Vec<BatchOperation>,
}

impl BatchBuilder {
    pub(crate) fn new(session: Session) -> Self {
        Self {
            session,
            operations: Vec::new(),
        }
    }

    /// Adds a PUT operation to the batch.
    pub fn add_put(mut self, builder: PutBuilder) -> Self {
        self.operations.push(builder.into_batch_operation());
        self
    }

    /// Adds a GET operation to the batch.
    pub fn add_get(mut self, builder: GetBuilder) -> Self {
        self.operations.push(builder.into_batch_operation());
        self
    }

    /// Adds a DELETE operation to the batch.
    pub fn add_delete(mut self, builder: DeleteBuilder) -> Self {
        self.operations.push(builder.into_batch_operation());
        self
    }

    /// Sends the batch request.
    ///
    /// If more than 1000 operations are added, they will be automatically
    /// chunked into multiple batch requests sent sequentially.
    pub async fn send(self) -> crate::Result<BatchResponse> {
        if self.operations.is_empty() {
            return Ok(BatchResponse { items: vec![] });
        }

        let total_ops = self.operations.len();
        let mut all_items = Vec::with_capacity(total_ops);

        // Split operations into chunks using drain
        let session = self.session;
        let mut operations = self.operations;
        while !operations.is_empty() {
            let chunk_size = std::cmp::min(MAX_OPERATIONS_PER_BATCH, operations.len());
            let chunk: Vec<BatchOperation> = operations.drain(..chunk_size).collect();
            let items = send_chunk(&session, chunk).await?;
            all_items.extend(items);
        }

        Ok(BatchResponse { items: all_items })
    }
}

async fn send_chunk(session: &Session, operations: Vec<BatchOperation>) -> crate::Result<Vec<BatchResponseItem>> {
    // Store operation metadata for response parsing before encoding consumes the operations
    let op_metadata: Vec<OperationMeta> = operations
        .iter()
        .map(|op| match op {
            BatchOperation::Get { key, decompress } => OperationMeta::Get {
                key: key.clone(),
                decompress: *decompress,
            },
            BatchOperation::Put { key, .. } => OperationMeta::Put { key: key.clone() },
            BatchOperation::Delete { key } => OperationMeta::Delete { key: key.clone() },
        })
        .collect();

    let form = encode_operations(operations)?;

    let (_url, builder) = session.batch_request()?;
    let builder = builder.multipart(form);

    let response = builder.send().await?;
    let response = response.error_for_status()?;

    parse_response(response, &op_metadata).await
}

fn encode_operations(operations: Vec<BatchOperation>) -> crate::Result<Form> {
    let mut form = Form::new();

    for (idx, op) in operations.into_iter().enumerate() {
        let kind = op.kind();
        let part = match op {
            BatchOperation::Get { key, .. } => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER_BATCH_OPERATION_KEY, key.parse()?);
                headers.insert(HEADER_BATCH_OPERATION_KIND, kind.parse()?);
                Part::bytes(vec![]).headers(headers)
            }
            BatchOperation::Put { key, payload, metadata } => {
                let mut headers = metadata.to_headers("", false)?;
                headers.insert(HEADER_BATCH_OPERATION_KEY, key.parse()?);
                headers.insert(HEADER_BATCH_OPERATION_KIND, kind.parse()?);

                match (metadata.compression, payload) {
                    (Some(Compression::Zstd), BatchPutPayload::Buffer(bytes)) => {
                        let cursor = Cursor::new(bytes);
                        let encoder = ZstdEncoder::new(cursor);
                        let stream = ReaderStream::new(encoder);
                        Part::stream(Body::wrap_stream(stream)).headers(headers)
                    }
                    (Some(Compression::Zstd), BatchPutPayload::Stream(stream)) => {
                        let reader = StreamReader::new(stream);
                        let encoder = ZstdEncoder::new(reader);
                        let stream = ReaderStream::new(encoder);
                        Part::stream(Body::wrap_stream(stream)).headers(headers)
                    }
                    (None, BatchPutPayload::Buffer(bytes)) => {
                        Part::bytes(bytes.to_vec()).headers(headers)
                    }
                    (None, BatchPutPayload::Stream(stream)) => {
                        Part::stream(Body::wrap_stream(stream)).headers(headers)
                    }
                }
            }
            BatchOperation::Delete { key } => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER_BATCH_OPERATION_KEY, key.parse()?);
                headers.insert(HEADER_BATCH_OPERATION_KIND, kind.parse()?);
                Part::bytes(vec![]).headers(headers)
            }
        };
        form = form.part(format!("part{}", idx), part);
    }

    Ok(form)
}

fn extract_boundary(content_type: &str) -> Option<String> {
    // Parse "multipart/form-data; boundary=\"...\""
    for part in content_type.split(';') {
        let part = part.trim();
        if let Some(boundary) = part.strip_prefix("boundary=") {
            // Remove quotes if present
            let boundary = boundary.trim_matches('"');
            return Some(boundary.to_string());
        }
    }
    None
}

async fn parse_response(
    response: reqwest::Response,
    original_operations: &[OperationMeta],
) -> crate::Result<Vec<BatchResponseItem>> {
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .ok_or(crate::Error::InvalidBoundary)?;

    let boundary = extract_boundary(content_type).ok_or(crate::Error::InvalidBoundary)?;

    // Convert response body to stream for multer
    let stream = response
        .bytes_stream()
        .map(|result| result.map_err(std::io::Error::other));

    let mut multipart = multer::Multipart::new(stream, boundary);

    let mut items = Vec::new();
    let mut idx = 0;

    while let Some(field) = multipart.next_field().await? {
        let headers = field.headers().clone();

        let key = headers
            .get(HEADER_BATCH_OPERATION_KEY)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let status: u16 = headers
            .get(HEADER_BATCH_OPERATION_STATUS)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);

        let body = field.bytes().await?;

        let item = parse_response_item(
            key,
            status,
            &headers,
            body,
            original_operations.get(idx),
        )
        .await?;

        items.push(item);
        idx += 1;
    }

    Ok(items)
}

async fn parse_response_item(
    key: Option<String>,
    status: u16,
    headers: &HeaderMap,
    body: Bytes,
    original_op: Option<&OperationMeta>,
) -> crate::Result<BatchResponseItem> {
    let key = key.unwrap_or_else(|| {
        original_op
            .map(|op| match op {
                OperationMeta::Get { key, .. } => key.clone(),
                OperationMeta::Put { key } => key.clone(),
                OperationMeta::Delete { key } => key.clone(),
            })
            .unwrap_or_default()
    });

    // Check for error status (but 404 is not an error for GET)
    if status >= 400 && !(status == 404 && matches!(original_op, Some(OperationMeta::Get { .. }))) {
        let error = parse_error_body(&body, status);
        return Ok(match original_op {
            Some(OperationMeta::Get { .. }) => BatchResponseItem::Get {
                key,
                result: Err(error),
            },
            Some(OperationMeta::Put { .. }) => BatchResponseItem::Put {
                key,
                result: Err(error),
            },
            Some(OperationMeta::Delete { .. }) => BatchResponseItem::Delete {
                key,
                result: Err(error),
            },
            None => {
                // Unknown operation type, but we have an error - use Put as fallback
                BatchResponseItem::Put {
                    key,
                    result: Err(error),
                }
            }
        });
    }

    match original_op {
        Some(OperationMeta::Get { decompress, .. }) => {
            if status == 404 {
                Ok(BatchResponseItem::Get {
                    key,
                    result: Ok(BatchGetResult::NotFound),
                })
            } else {
                let mut metadata = Metadata::from_headers(headers, "")?;
                let payload = maybe_decompress(body, &mut metadata, *decompress).await?;
                Ok(BatchResponseItem::Get {
                    key,
                    result: Ok(BatchGetResult::Found { metadata, payload }),
                })
            }
        }
        Some(OperationMeta::Put { .. }) => Ok(BatchResponseItem::Put {
            key: key.clone(),
            result: Ok(BatchPutResult { key }),
        }),
        Some(OperationMeta::Delete { .. }) => Ok(BatchResponseItem::Delete {
            key,
            result: Ok(BatchDeleteResult),
        }),
        None => {
            // Unknown operation type - treat as error
            Ok(BatchResponseItem::Put {
                key,
                result: Err(BatchOperationError {
                    status,
                    detail: "Unknown operation type in response".into(),
                    causes: vec![],
                }),
            })
        }
    }
}

fn parse_error_body(body: &Bytes, status: u16) -> BatchOperationError {
    match serde_json::from_slice::<ErrorResponseBody>(body) {
        Ok(error) => BatchOperationError {
            status,
            detail: error.detail,
            causes: error.causes,
        },
        Err(_) => BatchOperationError {
            status,
            detail: String::from_utf8_lossy(body).to_string(),
            causes: vec![],
        },
    }
}

async fn maybe_decompress(
    body: Bytes,
    metadata: &mut Metadata,
    decompress: bool,
) -> crate::Result<Bytes> {
    match (metadata.compression, decompress) {
        (Some(Compression::Zstd), true) => {
            metadata.compression = None;
            let cursor = Cursor::new(body);
            let decoder = ZstdDecoder::new(cursor);
            let stream = ReaderStream::new(decoder);
            let decompressed: BytesMut = stream.try_collect().await?;
            Ok(decompressed.freeze())
        }
        _ => Ok(body),
    }
}
