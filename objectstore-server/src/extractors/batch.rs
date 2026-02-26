use std::fmt::Debug;

use axum::extract::{
    FromRequest, Multipart, Request,
    multipart::{Field, MultipartError, MultipartRejection},
};
use futures::{StreamExt, stream::BoxStream};
use objectstore_service::streaming::{Delete, Get, Insert, Operation};
use objectstore_types::metadata::Metadata;
use percent_encoding::percent_decode_str;
use thiserror::Error;

use crate::batch::{HEADER_BATCH_OPERATION_KEY, HEADER_BATCH_OPERATION_KIND};

/// Errors that can occur when processing or executing batch operations.
#[derive(Debug, Error)]
pub enum BatchError {
    /// Malformed request.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// Errors in parsing or reading a multipart request body.
    #[error("multipart error: {0}")]
    Multipart(#[from] MultipartError),

    /// Errors related to de/serialization and parsing of object metadata.
    #[error("metadata error: {0}")]
    Metadata(#[from] objectstore_types::metadata::Error),

    /// Size or cardinality limit exceeded.
    #[error("batch limit exceeded: {0}")]
    LimitExceeded(String),

    /// Operation rejected due to rate limiting.
    #[error("rate limited")]
    RateLimited,

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

async fn try_operation_from_field(field: Field<'_>) -> Result<Operation, BatchError> {
    let kind = field
        .headers()
        .get(HEADER_BATCH_OPERATION_KIND)
        .ok_or_else(|| {
            BatchError::BadRequest(format!("missing {HEADER_BATCH_OPERATION_KIND} header"))
        })?;
    let kind = kind
        .to_str()
        .map_err(|_| {
            BatchError::BadRequest(format!(
                "unable to convert {HEADER_BATCH_OPERATION_KIND} header value to string"
            ))
        })?
        .to_lowercase();

    let key = field
        .headers()
        .get(HEADER_BATCH_OPERATION_KEY)
        .map(|v| {
            let s = v.to_str().map_err(|_| {
                BatchError::BadRequest(format!(
                    "unable to convert {HEADER_BATCH_OPERATION_KEY} header value to string"
                ))
            })?;
            percent_decode_str(s)
                .decode_utf8()
                .map(|decoded| decoded.into_owned())
                .map_err(|_| {
                    BatchError::BadRequest(format!(
                        "unable to percent-decode {HEADER_BATCH_OPERATION_KEY} header value"
                    ))
                })
        })
        .transpose()?;

    let operation = match kind.as_str() {
        "get" => Operation::Get(Get {
            key: key.ok_or_else(|| {
                BatchError::BadRequest(format!(
                    "missing {HEADER_BATCH_OPERATION_KEY} header for {kind} operation"
                ))
            })?,
        }),
        "delete" => Operation::Delete(Delete {
            key: key.ok_or_else(|| {
                BatchError::BadRequest(format!(
                    "missing {HEADER_BATCH_OPERATION_KEY} header for {kind} operation"
                ))
            })?,
        }),
        "insert" => {
            let metadata = Metadata::from_headers(field.headers(), "")?;
            let payload = field.bytes().await?;
            if payload.len() > MAX_FIELD_SIZE {
                return Err(BatchError::LimitExceeded(format!(
                    "individual request in batch exceeds body size limit of {MAX_FIELD_SIZE} bytes"
                )));
            }
            Operation::Insert(Insert {
                key,
                metadata,
                payload,
            })
        }
        _ => {
            return Err(BatchError::BadRequest(format!(
                "invalid operation kind: {kind}"
            )));
        }
    };
    Ok(operation)
}

/// A lazily-parsed stream of batch operations extracted from a multipart request body.
pub struct BatchOperationStream(pub BoxStream<'static, Result<Operation, BatchError>>);

impl Debug for BatchOperationStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchOperationStream").finish()
    }
}

const MAX_FIELD_SIZE: usize = 1024 * 1024; // 1 MB
const MAX_OPERATIONS: usize = 1000;

impl<S> FromRequest<S> for BatchOperationStream
where
    S: Send + Sync,
{
    type Rejection = MultipartRejection;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let mut multipart = Multipart::from_request(request, state).await?;

        let requests = async_stream::stream! {
            let mut count = 0;
            loop {
                let field = match multipart.next_field().await {
                    Ok(Some(field)) => field,
                    Ok(None) => break,
                    Err(e) => {
                        yield Err(BatchError::from(e));
                        continue;
                    }
                };
                if count >= MAX_OPERATIONS {
                    yield Err(BatchError::LimitExceeded(format!(
                        "exceeded {MAX_OPERATIONS} operations per batch request"
                    )));
                    continue;
                }
                count += 1;
                yield try_operation_from_field(field).await;
            }
        }
        .boxed();

        Ok(Self(requests))
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
    use objectstore_types::metadata::{ExpirationPolicy, HEADER_EXPIRATION, HEADER_ORIGIN};
    use percent_encoding::{NON_ALPHANUMERIC, percent_encode};

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

        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchOperationStream::from_request(request, &())
            .await
            .unwrap();

        let operations: Vec<_> = batch_request.0.collect().await;
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
        assert_eq!(insert_op1.payload.as_ref(), insert1_data);

        let Operation::Insert(insert_op2) = &operations[2].as_ref().unwrap() else {
            panic!("expected insert operation");
        };
        assert_eq!(insert_op2.key.as_deref(), Some("test2"));
        assert_eq!(insert_op2.metadata.content_type, "text/plain");
        assert_eq!(insert_op2.metadata.expiration_policy, expiration);
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

        let operations: Vec<_> = batch_request.0.collect().await;
        assert_eq!(operations.len(), 1);

        let Operation::Insert(insert_op) = &operations[0].as_ref().unwrap() else {
            panic!("expected insert operation");
        };
        assert!(insert_op.key.is_none());
        assert_eq!(insert_op.payload.as_ref(), b"keyless payload");
    }

    #[tokio::test]
    async fn test_individual_errors_with_isolation() {
        let large_payload = "x".repeat(MAX_FIELD_SIZE + 1);
        let valid_key = percent_encode(b"valid", NON_ALPHANUMERIC);
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

        let operations: Vec<_> = batch_request.0.collect().await;
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
        for i in 0..(MAX_OPERATIONS + 1) {
            let key = percent_encode(format!("test{i}").as_bytes(), NON_ALPHANUMERIC).to_string();
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
        let operations: Vec<_> = batch_request.0.collect().await;

        assert_eq!(operations.len(), MAX_OPERATIONS + 1);
        assert!(matches!(
            &operations[MAX_OPERATIONS],
            Err(BatchError::LimitExceeded(_))
        ));
    }
}
