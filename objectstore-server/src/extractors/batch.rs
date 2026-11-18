//! Axum extractor for batch operation streams.
//!
//! [`BatchOperationStream`] reads a multipart request body one part at a time, detects the
//! [`BatchFormat`] from the first part, and maps the parts onto a lazy stream of [`Operation`]s.
//!
//! All insert metadata uses the shared request timestamp, even for fields parsed
//! later while streaming the body.
//!
//! An invalid operation fails on its own, and reading continues with the next part. A broken
//! multipart body, such as a truncated body or unparseable part headers, yields one error and
//! ends the stream, because no further part can be found after it.
//!
//! # JSON layout
//!
//! Each operation is a descriptor part named by its index (`"0"`, `"1"`, …), whose body is a
//! JSON object with `kind`, `key` (optional for inserts) and, for inserts, an optional
//! [`WriteMetadata`] object. An insert is directly followed by its payload part
//! `"<i>.payload"`. The parser applies these rules:
//!
//! - A part whose name ends in `.payload` is a payload part. Every other part is a descriptor.
//! - The index of an operation is the position of its descriptor among descriptor parts. A
//!   descriptor with another name, invalid JSON, or a missing required field fails with `400`.
//! - An insert without its payload part fails with `400`, and the next part is read as the next
//!   descriptor. Any other payload part is skipped.
//! - The stored content type is `metadata.content_type`. If that is absent and the payload is
//!   not encoded, it is the payload part's `Content-Type`, and otherwise
//!   `application/octet-stream`.
//!
//! Both layouts allow 1000 operations per request and 1 MiB per part. The JSON layout counts
//! descriptor parts toward the operation limit and caps each descriptor at 64 KiB.

use std::fmt::Debug;
use std::pin::Pin;

use axum::RequestExt;
use axum::extract::{
    FromRequest, Multipart, Request,
    multipart::{Field, MultipartError, MultipartRejection},
};
use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream, Peekable};
use futures::{Stream, StreamExt};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, StatusCode};
use objectstore_service::id::ObjectKey;
use objectstore_service::streaming::{Delete, Get, Head, Insert, Operation};
use objectstore_types::batch::{
    self, Descriptor, MAX_BATCH_OPERATIONS, MAX_BATCH_PART_SIZE, MAX_DESCRIPTOR_SIZE,
    OperationKind, PAYLOAD_SUFFIX,
};
use objectstore_types::headers;
use objectstore_types::metadata::{Metadata, WriteMetadata};
use objectstore_types::time::Timestamp;
use thiserror::Error;

use crate::batch::{HEADER_BATCH_OPERATION_KEY, HEADER_BATCH_OPERATION_KIND};
use crate::extractors::request_time::RequestTime;
use crate::rate_limits::LimitExceeded;

/// Errors that can occur when processing or executing batch operations.
#[derive(Debug, Error)]
pub enum BatchError {
    /// Malformed request.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// Errors in parsing or reading a multipart request body.
    #[error("multipart error: {0}")]
    Multipart(#[from] MultipartError),

    /// An operation descriptor of the JSON layout that cannot be deserialized.
    #[error("invalid operation descriptor: {0}")]
    Descriptor(#[from] serde_json::Error),

    /// Errors related to de/serialization and parsing of object metadata.
    #[error("metadata error: {0}")]
    Metadata(#[from] objectstore_types::metadata::Error),

    /// Size or cardinality limit exceeded.
    #[error("batch limit exceeded: {0}")]
    LimitExceeded(String),

    /// Operation rejected due to rate limiting.
    #[error("{0}")]
    RateLimited(LimitExceeded),

    /// Errors encountered when serializing batch response parts.
    #[error("response part serialization error: {context}")]
    ResponseSerialization {
        /// Context describing what was being serialized.
        context: String,
        /// The underlying error.
        #[source]
        cause: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl BatchError {
    /// Returns the HTTP status code reported for the failed operation.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) | Self::Multipart(_) | Self::Descriptor(_) | Self::Metadata(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::LimitExceeded(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RateLimited(rejection) => rejection.status(),
            Self::ResponseSerialization { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn too_many_operations() -> Self {
        Self::LimitExceeded(format!(
            "exceeded {MAX_BATCH_OPERATIONS} operations per batch request"
        ))
    }
}

/// The part layout of a batch request. The response uses the same layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchFormat {
    /// One part per operation, described by `x-sn-batch-operation-*` and metadata part headers.
    ///
    /// Detected when the first part has an
    /// [`x-sn-batch-operation-kind`](HEADER_BATCH_OPERATION_KIND) header.
    Headers,
    /// JSON descriptor parts plus payload parts, as described in the [module docs](self).
    ///
    /// Used for every other request, including an empty one.
    Json,
}

/// A multipart part, read into memory.
#[derive(Debug)]
struct Part {
    name: Option<String>,
    headers: HeaderMap,
    /// The body, or [`BatchError::LimitExceeded`] if it is larger than 1 MiB.
    body: Result<Bytes, BatchError>,
}

impl Part {
    /// Returns whether this is a payload part, named `"<i>.payload"`.
    fn is_payload(&self) -> bool {
        self.name
            .as_deref()
            .is_some_and(|name| name.ends_with(PAYLOAD_SUFFIX))
    }

    /// Returns whether this part describes an operation of the header layout.
    fn has_kind_header(&self) -> bool {
        self.headers.contains_key(HEADER_BATCH_OPERATION_KIND)
    }

    /// Returns the part's `Content-Type`, if it is a valid string.
    fn content_type(&self) -> Option<String> {
        self.headers
            .get(CONTENT_TYPE)?
            .to_str()
            .ok()
            .map(str::to_owned)
    }

    /// Parses this part as the descriptor of the operation at `index`.
    fn into_descriptor(self, index: usize) -> Result<Descriptor, BatchError> {
        if index >= MAX_BATCH_OPERATIONS {
            return Err(BatchError::too_many_operations());
        }

        if self.name.as_deref().and_then(|name| name.parse().ok()) != Some(index) {
            return Err(BatchError::BadRequest(format!(
                "expected descriptor part named \"{index}\", found {:?}",
                self.name
            )));
        }

        let body = self.body?;
        if body.len() > MAX_DESCRIPTOR_SIZE {
            return Err(BatchError::LimitExceeded(format!(
                "operation descriptor exceeds size limit of {MAX_DESCRIPTOR_SIZE} bytes"
            )));
        }

        Ok(serde_json::from_slice(&body)?)
    }
}

/// The parts of a request, read one at a time. Ends after the first broken part.
type Parts = Pin<Box<Peekable<BoxStream<'static, Result<Part, BatchError>>>>>;

/// Reads the body of a field, failing once it exceeds `MAX_BATCH_PART_SIZE`.
async fn read_body(mut field: Field<'_>) -> Result<Bytes, BatchError> {
    let mut body = BytesMut::new();
    while let Some(chunk) = field.chunk().await? {
        if body.len() + chunk.len() > MAX_BATCH_PART_SIZE {
            return Err(BatchError::LimitExceeded(format!(
                "individual request in batch exceeds body size limit of {MAX_BATCH_PART_SIZE} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

/// Streams the parts of a multipart body, each read into memory.
fn stream_parts(multipart: Multipart) -> Parts {
    let parts = stream::unfold(Some(multipart), |multipart| async move {
        let mut multipart = multipart?;
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => return None,
            Err(error) => return Some((Err(error.into()), None)),
        };

        let name = field.name().map(str::to_owned);
        let headers = field.headers().clone();
        match read_body(field).await {
            Err(error @ BatchError::Multipart(_)) => Some((Err(error), None)),
            body => Some((
                Ok(Part {
                    name,
                    headers,
                    body,
                }),
                Some(multipart),
            )),
        }
    });

    Box::pin(parts.boxed().peekable())
}

/// Parses a part of the header layout into an operation.
fn parse_header_part(part: Part, access_time: Timestamp) -> Result<Operation, BatchError> {
    let kind_name = part
        .headers
        .get(HEADER_BATCH_OPERATION_KIND)
        .ok_or_else(|| {
            BatchError::BadRequest(format!("missing {HEADER_BATCH_OPERATION_KIND} header"))
        })?;
    let kind_name = kind_name
        .to_str()
        .map_err(|_| {
            BatchError::BadRequest(format!(
                "unable to convert {HEADER_BATCH_OPERATION_KIND} header value to string"
            ))
        })?
        .to_lowercase();
    let kind = OperationKind::from(kind_name.as_str());

    let key = part
        .headers
        .get(HEADER_BATCH_OPERATION_KEY)
        .map(|v| {
            headers::decode_header_value(v).map_err(|_| {
                BatchError::BadRequest(format!(
                    "unable to percent-decode {HEADER_BATCH_OPERATION_KEY} header value"
                ))
            })
        })
        .transpose()?;
    let required_key = |key: Option<ObjectKey>| {
        key.ok_or_else(|| {
            BatchError::BadRequest(format!(
                "missing {HEADER_BATCH_OPERATION_KEY} header for {kind_name} operation"
            ))
        })
    };

    let operation = match kind {
        OperationKind::Get => Operation::Get(Get {
            key: required_key(key)?,
        }),
        OperationKind::Delete => Operation::Delete(Delete {
            key: required_key(key)?,
        }),
        OperationKind::Head => Operation::Head(Head {
            key: required_key(key)?,
        }),
        OperationKind::Insert => Operation::Insert(Box::new(Insert {
            metadata: Metadata::from_insert_headers(&part.headers, "", access_time)?,
            payload: part.body?,
            key,
        })),
        OperationKind::Unknown => {
            return Err(BatchError::BadRequest(format!(
                "invalid operation kind: {kind_name}"
            )));
        }
    };
    Ok(operation)
}

/// Streams the operations of a header-layout request.
fn stream_header_ops(
    parts: Parts,
    access_time: Timestamp,
) -> impl Stream<Item = Result<Operation, BatchError>> {
    parts.enumerate().map(move |(index, part)| {
        if index >= MAX_BATCH_OPERATIONS {
            return Err(BatchError::too_many_operations());
        }
        parse_header_part(part?, access_time)
    })
}

/// Builds an insert from its descriptor and the part that follows it, if that is its payload.
fn parse_insert(
    key: Option<ObjectKey>,
    mut metadata: WriteMetadata,
    payload: Option<Result<Part, BatchError>>,
    access_time: Timestamp,
) -> Result<Operation, BatchError> {
    let payload = payload.ok_or_else(|| {
        BatchError::BadRequest("missing payload part for insert operation".to_owned())
    })??;

    if metadata.content_type.is_none() && metadata.content_encoding.is_none() {
        metadata.content_type = payload.content_type();
    }

    Ok(Operation::Insert(Box::new(Insert {
        key,
        metadata: metadata.into_metadata(access_time)?,
        payload: payload.body?,
    })))
}

/// Streams the operations of a JSON-layout request.
fn stream_json_ops(
    parts: Parts,
    access_time: Timestamp,
) -> impl Stream<Item = Result<Operation, BatchError>> {
    stream::unfold((parts, 0), move |(mut parts, index)| async move {
        // Payload parts that do not directly follow their insert are silently skipped.
        let part = loop {
            match parts.next().await? {
                Ok(part) if part.is_payload() => continue,
                part => break part,
            }
        };

        let operation = match part.and_then(|part| part.into_descriptor(index)) {
            Ok(Descriptor::Get { key }) => Ok(Operation::Get(Get { key })),
            Ok(Descriptor::Head { key }) => Ok(Operation::Head(Head { key })),
            Ok(Descriptor::Delete { key }) => Ok(Operation::Delete(Delete { key })),
            Ok(Descriptor::Insert { key, metadata }) => {
                // The next part is the payload, unless it belongs elsewhere. A broken body fails
                // the insert.
                let payload_name = batch::payload_part_name(index);
                let is_payload = |next: &Result<Part, BatchError>| match next {
                    Ok(part) => part.name.as_deref() == Some(payload_name.as_str()),
                    Err(_) => true,
                };
                let payload = parts.as_mut().next_if(is_payload).await;
                parse_insert(key, metadata, payload, access_time)
            }
            Err(error) => Err(error),
        };

        Some((operation, (parts, index + 1)))
    })
}

/// A lazily-parsed stream of batch operations extracted from a multipart request body.
///
/// The stream yields exactly one item per operation in index order, so the position of an item
/// is the index of its operation. An empty body yields an empty stream.
pub struct BatchOperationStream {
    /// The layout of the request, which the response uses as well.
    pub format: BatchFormat,
    /// The operations in index order.
    pub operations: BoxStream<'static, Result<Operation, BatchError>>,
}

impl Debug for BatchOperationStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchOperationStream")
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

impl<S> FromRequest<S> for BatchOperationStream
where
    S: Send + Sync,
{
    type Rejection = MultipartRejection;

    async fn from_request(mut request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Ok(RequestTime(access_time)) = request.extract_parts::<RequestTime>().await;

        let mut parts = stream_parts(Multipart::from_request(request, state).await?);
        let format = match parts.as_mut().peek().await {
            Some(Ok(part)) if part.has_kind_header() => BatchFormat::Headers,
            _ => BatchFormat::Json,
        };

        let operations = match format {
            BatchFormat::Headers => stream_header_ops(parts, access_time).boxed(),
            BatchFormat::Json => stream_json_ops(parts, access_time).boxed(),
        };

        Ok(Self { format, operations })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use axum::body::Body;
    use axum::http::{Request, header::CONTENT_TYPE};
    use futures::StreamExt;
    use objectstore_service::streaming::Operation;
    use objectstore_types::headers;
    use objectstore_types::metadata::{ExpirationPolicy, HEADER_EXPIRATION, HEADER_ORIGIN};

    #[tokio::test]
    async fn test_valid_request_works() {
        let insert1_data = b"first blob data";
        let insert2_data = b"second blob data";
        let expiration = ExpirationPolicy::TimeToLive(Duration::from_hours(1));
        let body = format!(
            "--boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: {key0}\r\n\
             {HEADER_BATCH_OPERATION_KIND}: get\r\n\
             \r\n\
             \r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: {key1}\r\n\
             {HEADER_BATCH_OPERATION_KIND}: insert\r\n\
             Content-Type: application/octet-stream\r\n\
             \r\n\
             {insert1}\r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: {key2}\r\n\
             {HEADER_BATCH_OPERATION_KIND}: insert\r\n\
             {HEADER_EXPIRATION}: {expiration}\r\n\
             {HEADER_ORIGIN}: 203.0.113.42\r\n\
             Content-Type: text/plain\r\n\
             \r\n\
             {insert2}\r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: {key3}\r\n\
             {HEADER_BATCH_OPERATION_KIND}: delete\r\n\
             \r\n\
             \r\n\
             --boundary--\r\n",
            key0 = "test%2F0", // "test/0" percent-encoded
            key1 = "test1",
            key2 = "test2",
            key3 = "test3",
            insert1 = String::from_utf8_lossy(insert1_data),
            insert2 = String::from_utf8_lossy(insert2_data),
        );

        let access_time = Timestamp::UNIX_EPOCH;
        let request = Request::builder()
            .extension(RequestTime(access_time))
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchOperationStream::from_request(request, &())
            .await
            .unwrap();

        let operations: Vec<_> = batch_request.operations.collect().await;
        assert_eq!(operations.len(), 4);

        let Operation::Get(get_op) = &operations[0].as_ref().unwrap() else {
            panic!("expected get operation");
        };
        assert_eq!(get_op.key, "test/0");

        let Operation::Insert(insert_op1) = &operations[1].as_ref().unwrap() else {
            panic!("expected insert operation");
        };
        assert_eq!(insert_op1.key.as_deref(), Some("test1"));
        assert_eq!(insert_op1.metadata.content_type, "application/octet-stream");
        assert_eq!(insert_op1.metadata.origin, None);
        assert_eq!(insert_op1.metadata.time_created, Some(access_time));
        assert_eq!(insert_op1.payload.as_ref(), insert1_data);

        let Operation::Insert(insert_op2) = &operations[2].as_ref().unwrap() else {
            panic!("expected insert operation");
        };
        assert_eq!(insert_op2.key.as_deref(), Some("test2"));
        assert_eq!(insert_op2.metadata.content_type, "text/plain");
        assert_eq!(insert_op2.metadata.expiration_policy, expiration);
        assert_eq!(insert_op2.metadata.time_created, Some(access_time));
        assert_eq!(
            insert_op2.metadata.time_expires,
            Some(access_time + Duration::from_hours(1))
        );
        assert_eq!(insert_op2.metadata.origin.as_deref(), Some("203.0.113.42"));
        assert_eq!(insert_op2.payload.as_ref(), insert2_data);

        let Operation::Delete(delete_op) = &operations[3].as_ref().unwrap() else {
            panic!("expected delete operation");
        };
        assert_eq!(delete_op.key, "test3");
    }

    #[tokio::test]
    async fn test_insert_without_key_header() {
        let body = format!(
            "--boundary\r\n\
             {HEADER_BATCH_OPERATION_KIND}: insert\r\n\
             Content-Type: application/octet-stream\r\n\
             \r\n\
             keyless payload\r\n\
             --boundary--\r\n",
        );

        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchOperationStream::from_request(request, &())
            .await
            .unwrap();

        let operations: Vec<_> = batch_request.operations.collect().await;
        assert_eq!(operations.len(), 1);

        let Operation::Insert(insert_op) = &operations[0].as_ref().unwrap() else {
            panic!("expected insert operation");
        };
        assert!(insert_op.key.is_none());
        assert_eq!(insert_op.payload.as_ref(), b"keyless payload");
    }

    #[tokio::test]
    async fn test_individual_errors_with_isolation() {
        let large_payload = "x".repeat(MAX_BATCH_PART_SIZE + 1);
        let valid_key = headers::encode_header_str("valid");
        let body = format!(
            "--boundary\r\n\
             {HEADER_BATCH_OPERATION_KIND}: get\r\n\
             \r\n\
             \r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: {valid_key}\r\n\
             {HEADER_BATCH_OPERATION_KIND}: get\r\n\
             \r\n\
             \r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KIND}: delete\r\n\
             \r\n\
             \r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: {valid_key}\r\n\
             {HEADER_BATCH_OPERATION_KIND}: insert\r\n\
             Content-Type: application/octet-stream\r\n\
             \r\n\
             {large_payload}\r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: {valid_key}\r\n\
             {HEADER_BATCH_OPERATION_KIND}: delete\r\n\
             \r\n\
             \r\n\
             --boundary--\r\n",
        );

        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchOperationStream::from_request(request, &())
            .await
            .unwrap();

        let operations: Vec<_> = batch_request.operations.collect().await;
        assert_eq!(operations.len(), 5);

        // get without key → BadRequest
        assert!(matches!(&operations[0], Err(BatchError::BadRequest(_))));
        // valid get
        assert!(matches!(
            &operations[1].as_ref().unwrap(),
            Operation::Get(g) if g.key == "valid"
        ));
        // delete without key → BadRequest
        assert!(matches!(&operations[2], Err(BatchError::BadRequest(_))));
        // oversized insert → LimitExceeded
        assert!(matches!(&operations[3], Err(BatchError::LimitExceeded(_))));
        // valid delete still succeeds after prior errors
        assert!(matches!(
            &operations[4].as_ref().unwrap(),
            Operation::Delete(d) if d.key == "valid"
        ));
    }

    #[tokio::test]
    async fn test_max_operations_limit_enforced() {
        let mut body = String::new();
        for i in 0..(MAX_BATCH_OPERATIONS + 1) {
            let key = headers::encode_header_str(&format!("test{i}")).to_string();
            body.push_str(&format!(
                "--boundary\r\n\
                 {HEADER_BATCH_OPERATION_KEY}: {key}\r\n\
                 {HEADER_BATCH_OPERATION_KIND}: get\r\n\
                 \r\n\
                 \r\n"
            ));
        }
        body.push_str("--boundary--\r\n");

        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchOperationStream::from_request(request, &())
            .await
            .unwrap();
        let operations: Vec<_> = batch_request.operations.collect().await;

        assert_eq!(operations.len(), MAX_BATCH_OPERATIONS + 1);
        assert!(matches!(
            &operations[MAX_BATCH_OPERATIONS],
            Err(BatchError::LimitExceeded(_))
        ));
    }

    #[tokio::test]
    async fn test_head_operation() {
        let key = headers::encode_header_str("head-key");
        let body = format!(
            "--boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: {key}\r\n\
             {HEADER_BATCH_OPERATION_KIND}: head\r\n\
             \r\n\
             \r\n\
             --boundary--\r\n",
        );

        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchOperationStream::from_request(request, &())
            .await
            .unwrap();
        let operations: Vec<_> = batch_request.operations.collect().await;
        assert_eq!(operations.len(), 1);

        let Operation::Head(head_op) = &operations[0].as_ref().unwrap() else {
            panic!("expected head operation");
        };
        assert_eq!(head_op.key, "head-key");
    }

    #[tokio::test]
    async fn test_head_without_key_is_error() {
        let body = format!(
            "--boundary\r\n\
             {HEADER_BATCH_OPERATION_KIND}: head\r\n\
             \r\n\
             \r\n\
             --boundary--\r\n",
        );

        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchOperationStream::from_request(request, &())
            .await
            .unwrap();
        let operations: Vec<_> = batch_request.operations.collect().await;
        assert_eq!(operations.len(), 1);
        assert!(matches!(&operations[0], Err(BatchError::BadRequest(_))));
    }

    // --- JSON layout -------------------------------------------------------------------------

    const END: &str = "--boundary--\r\n";

    fn descriptor(name: &str, json: &str) -> String {
        format!("--boundary\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{json}\r\n")
    }

    fn payload(name: &str, content_type: Option<&str>, body: &str) -> String {
        let content_type = content_type
            .map(|ct| format!("Content-Type: {ct}\r\n"))
            .unwrap_or_default();
        format!(
            "--boundary\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"blob\"\r\n\
             {content_type}\r\n{body}\r\n"
        )
    }

    async fn parse(body: String) -> (BatchFormat, Vec<Result<Operation, BatchError>>) {
        let request = Request::builder()
            .extension(RequestTime(Timestamp::UNIX_EPOCH))
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();
        let stream = BatchOperationStream::from_request(request, &())
            .await
            .unwrap();
        (stream.format, stream.operations.collect().await)
    }

    #[track_caller]
    fn insert(item: &Result<Operation, BatchError>) -> &Insert {
        match item {
            Ok(Operation::Insert(insert)) => insert,
            other => panic!("expected insert, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_detects_layout() {
        let header = format!(
            "--boundary\r\n{HEADER_BATCH_OPERATION_KIND}: get\r\n\
             {HEADER_BATCH_OPERATION_KEY}: a\r\n\r\n\r\n{END}"
        );
        assert_eq!(parse(header).await.0, BatchFormat::Headers);

        let json = descriptor("0", r#"{"kind":"get","key":"a"}"#) + END;
        assert_eq!(parse(json).await.0, BatchFormat::Json);

        let (format, operations) = parse(END.to_owned()).await;
        assert_eq!(format, BatchFormat::Json);
        assert!(operations.is_empty());
    }

    #[tokio::test]
    async fn test_json_all_kinds() {
        let body = [
            descriptor("0", r#"{"kind":"get","key":"a/résumé.png"}"#),
            descriptor("1", r#"{"kind":"head","key":"b","ignored":true}"#),
            // `metadata` is ignored for other kinds.
            descriptor("2", r#"{"kind":"delete","key":"c","metadata":{"origin":5}}"#),
            descriptor(
                "3",
                r#"{"kind":"insert","key":"d","metadata":{"content_type":"image/png","expiration":"ttl:1h"}}"#,
            ),
            payload("3.payload", None, "png"),
            descriptor("4", r#"{"kind":"insert"}"#),
            payload("4.payload", None, "keyless"),
        ]
        .concat()
            + END;

        let (_, operations) = parse(body).await;
        assert_eq!(operations.len(), 5);
        assert!(matches!(&operations[0], Ok(Operation::Get(op)) if op.key == "a/résumé.png"));
        assert!(matches!(&operations[1], Ok(Operation::Head(op)) if op.key == "b"));
        assert!(matches!(&operations[2], Ok(Operation::Delete(op)) if op.key == "c"));

        let inserted = insert(&operations[3]);
        assert_eq!(inserted.key.as_deref(), Some("d"));
        assert_eq!(inserted.payload.as_ref(), b"png");
        assert_eq!(inserted.metadata.content_type, "image/png");
        assert_eq!(
            inserted.metadata.time_expires,
            Some(Timestamp::UNIX_EPOCH + Duration::from_hours(1))
        );

        assert_eq!(insert(&operations[4]).key, None);
    }

    #[tokio::test]
    async fn test_json_content_type_resolution() {
        let body = [
            // Metadata wins over the payload part.
            descriptor(
                "0",
                r#"{"kind":"insert","metadata":{"content_type":"image/png"}}"#,
            ),
            payload("0.payload", Some("text/plain"), "a"),
            // Without metadata, the payload part's type is used.
            descriptor("1", r#"{"kind":"insert"}"#),
            payload("1.payload", Some("text/plain"), "b"),
            // Encoded payloads do not use the payload part's type.
            descriptor(
                "2",
                r#"{"kind":"insert","metadata":{"content_encoding":"gzip"}}"#,
            ),
            payload("2.payload", Some("text/plain"), "c"),
        ]
        .concat()
            + END;

        let (_, operations) = parse(body).await;
        let content_types: Vec<_> = operations
            .iter()
            .map(|item| insert(item).metadata.content_type.as_ref())
            .collect();
        assert_eq!(
            content_types,
            ["image/png", "text/plain", "application/octet-stream"]
        );
    }

    #[tokio::test]
    async fn test_json_invalid_operations() {
        let body = [
            // The index is the position, so "1" at position 0 fails.
            descriptor("1", r#"{"kind":"get","key":"a"}"#),
            descriptor("1", "not json"),
            descriptor("2", r#"{"kind":"fetch","key":"a"}"#),
            descriptor("3", r#"{"kind":"get"}"#),
            descriptor(
                "4",
                r#"{"kind":"insert","metadata":{"expiration":"forever"}}"#,
            ),
            payload("4.payload", None, "x"),
            descriptor("5", r#"{"kind":"get","key":"ok"}"#),
        ]
        .concat()
            + END;

        let (_, operations) = parse(body).await;
        assert_eq!(operations.len(), 6);
        for item in &operations[..5] {
            let error = item.as_ref().expect_err("expected an error");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        }
        assert!(operations[5].is_ok());
    }

    #[tokio::test]
    async fn test_json_payload_pairing() {
        let body = [
            // The payload after a failed descriptor is skipped.
            descriptor("x", r#"{"kind":"get","key":"a"}"#),
            payload("0.payload", None, r#"{"kind":"get","key":"b"}"#),
            // An insert without its payload fails; the next part is the next descriptor.
            descriptor("1", r#"{"kind":"insert","key":"c"}"#),
            descriptor("2", r#"{"kind":"get","key":"d"}"#),
            // Payloads that do not follow their insert are skipped.
            payload("other.payload", None, "stray"),
            descriptor("3", r#"{"kind":"insert","key":"e"}"#),
            payload("3.payload", None, "ok"),
        ]
        .concat()
            + END;

        let (_, operations) = parse(body).await;
        assert_eq!(operations.len(), 4);
        assert!(operations[0].is_err());
        assert!(operations[1].is_err());
        assert!(matches!(&operations[2], Ok(Operation::Get(op)) if op.key == "d"));
        assert_eq!(insert(&operations[3]).payload.as_ref(), b"ok");
    }

    #[tokio::test]
    async fn test_json_broken_body() {
        let get = descriptor("0", r#"{"kind":"get","key":"a"}"#);
        let broken = "--boundary\r\nnot a header line\r\n\r\n{}\r\n";
        let cases = [
            // The error goes to the insert waiting for its payload.
            get.clone() + &descriptor("1", r#"{"kind":"insert"}"#) + broken,
            // Otherwise it goes to the next index, and later parts are never read.
            get.clone() + broken + &descriptor("2", r#"{"kind":"get","key":"c"}"#) + END,
        ];

        for body in cases {
            let (_, operations) = parse(body).await;
            assert_eq!(operations.len(), 2);
            assert!(operations[0].is_ok());
            assert!(matches!(operations[1], Err(BatchError::Multipart(_))));
        }
    }

    #[tokio::test]
    async fn test_json_size_limits() {
        let large_payload = "x".repeat(MAX_BATCH_PART_SIZE + 1);
        let large_key = "k".repeat(MAX_DESCRIPTOR_SIZE);
        let body = [
            descriptor("0", r#"{"kind":"insert"}"#),
            payload("0.payload", None, &large_payload),
            descriptor("1", &format!(r#"{{"kind":"get","key":"{large_key}"}}"#)),
        ]
        .concat()
            + END;

        let (_, operations) = parse(body).await;
        assert!(matches!(
            operations[..],
            [
                Err(BatchError::LimitExceeded(_)),
                Err(BatchError::LimitExceeded(_))
            ]
        ));
    }

    #[tokio::test]
    async fn test_json_operation_limit_counts_descriptors() {
        let mut body = String::new();
        for index in 0..=MAX_BATCH_OPERATIONS {
            body += &descriptor(&index.to_string(), r#"{"kind":"insert"}"#);
            body += &payload(&batch::payload_part_name(index), None, "x");
        }
        body += END;

        let (_, operations) = parse(body).await;
        assert_eq!(operations.len(), MAX_BATCH_OPERATIONS + 1);
        assert!(operations[..MAX_BATCH_OPERATIONS].iter().all(Result::is_ok));
        assert!(matches!(
            operations[MAX_BATCH_OPERATIONS],
            Err(BatchError::LimitExceeded(_))
        ));
    }
}
