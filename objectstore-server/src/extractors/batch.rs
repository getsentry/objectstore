use std::fmt::Debug;

use anyhow::Context;
use axum::{
    extract::{FromRequest, Multipart, Request, multipart::Field},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use objectstore_service::id::ObjectKey;
use objectstore_types::Metadata;

#[derive(Debug)]
pub struct GetOperation {
    pub key: ObjectKey,
}

#[derive(Debug)]
pub struct InsertOperation {
    pub key: Option<ObjectKey>,
    pub metadata: Metadata,
    pub payload: Bytes,
}

#[derive(Debug)]
pub struct DeleteOperation {
    pub key: ObjectKey,
}

#[derive(Debug)]
pub enum Operation {
    Get(GetOperation),
    Insert(InsertOperation),
    Delete(DeleteOperation),
}

impl Operation {
    async fn try_from_field(field: Field<'_>) -> anyhow::Result<Self> {
        let kind = field
            .headers()
            .get(HEADER_BATCH_OPERATION_KIND)
            .ok_or(anyhow::anyhow!(
                "missing {HEADER_BATCH_OPERATION_KIND} header"
            ))?;
        let kind = kind
            .to_str()
            .context(format!("invalid {HEADER_BATCH_OPERATION_KIND} header"))?
            .to_lowercase();

        let key = match field.headers().get(HEADER_BATCH_OPERATION_KEY) {
            Some(key) => Some(key.to_str().context("invalid object key")?.to_owned()),
            None => None,
        };

        let operation = match kind.as_str() {
            "get" => {
                let key = key.context("missing object key for get operation")?;
                Operation::Get(GetOperation { key })
            }
            "insert" => {
                let metadata = Metadata::from_headers(field.headers(), "")?;
                let payload = field.bytes().await.map_err(|e| anyhow::anyhow!("{e}"))?;
                if payload.len() > MAX_FIELD_SIZE {
                    anyhow::bail!("field size exceeds {MAX_FIELD_SIZE} bytes limit");
                }
                Operation::Insert(InsertOperation {
                    key,
                    metadata,
                    payload,
                })
            }
            "delete" => {
                let key = key.context("missing object key for delete operation")?;
                Operation::Delete(DeleteOperation { key })
            }
            _ => anyhow::bail!("invalid {HEADER_BATCH_OPERATION_KIND} header"),
        };
        Ok(operation)
    }
}

pub struct BatchRequest {
    pub operations: BoxStream<'static, anyhow::Result<Operation>>,
}

impl Debug for BatchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchRequest").finish()
    }
}

pub const HEADER_BATCH_OPERATION_KIND: &str = "x-sn-batch-operation-kind";
pub const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";

const MAX_FIELD_SIZE: usize = 1024 * 1024; // 1 MB
const MAX_OPERATIONS: usize = 1000;

impl<S> FromRequest<S> for BatchRequest
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let mut multipart = Multipart::from_request(request, state)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.body_text()).into_response())?;

        let operations = async_stream::try_stream! {
            let mut count = 0;
            while let Some(field) = multipart.next_field().await.map_err(|e| anyhow::anyhow!("{e}"))? {
                if count >= MAX_OPERATIONS {
                    Err(anyhow::anyhow!("exceeded limit of {MAX_OPERATIONS} operations per batch request"))?;
                }
                count += 1;
                yield Operation::try_from_field(field).await?;
            }
        }
        .boxed();

        Ok(Self { operations })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use axum::body::Body;
    use axum::http::{Request, header::CONTENT_TYPE};
    use futures::StreamExt;
    use objectstore_types::{ExpirationPolicy, HEADER_EXPIRATION};

    #[tokio::test]
    async fn test_valid_request_works() {
        let insert1_data = b"first blob data";
        let insert2_data = b"second blob data";
        let expiration = ExpirationPolicy::TimeToLive(Duration::from_hours(1));
        let body = format!(
            "--boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: test0\r\n\
             {HEADER_BATCH_OPERATION_KIND}: get\r\n\
             \r\n\
             \r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: test1\r\n\
             {HEADER_BATCH_OPERATION_KIND}: insert\r\n\
             Content-Type: application/octet-stream\r\n\
             \r\n\
             {insert1}\r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: test2\r\n\
             {HEADER_BATCH_OPERATION_KIND}: insert\r\n\
             {HEADER_EXPIRATION}: {expiration}\r\n\
             Content-Type: text/plain\r\n\
             \r\n\
             {insert2}\r\n\
             --boundary\r\n\
             {HEADER_BATCH_OPERATION_KEY}: test3\r\n\
             {HEADER_BATCH_OPERATION_KIND}: delete\r\n\
             \r\n\
             \r\n\
             --boundary--\r\n",
            insert1 = String::from_utf8_lossy(insert1_data),
            insert2 = String::from_utf8_lossy(insert2_data),
        );

        let request = Request::builder()
            .header(CONTENT_TYPE, "multipart/form-data; boundary=boundary")
            .body(Body::from(body))
            .unwrap();

        let batch_request = BatchRequest::from_request(request, &()).await.unwrap();

        let operations: Vec<_> = batch_request.operations.collect().await;
        assert_eq!(operations.len(), 4);

        let Operation::Get(get_op) = &operations[0].as_ref().unwrap() else {
            panic!("expected get operation");
        };
        assert_eq!(get_op.key, "test0");

        let Operation::Insert(insert_op1) = &operations[1].as_ref().unwrap() else {
            panic!("expected insert operation");
        };
        assert_eq!(insert_op1.key.as_ref().unwrap(), "test1");
        assert_eq!(insert_op1.metadata.content_type, "application/octet-stream");
        assert_eq!(insert_op1.payload.as_ref(), insert1_data);

        let Operation::Insert(insert_op2) = &operations[2].as_ref().unwrap() else {
            panic!("expected insert operation");
        };
        assert_eq!(insert_op2.key.as_ref().unwrap(), "test2");
        assert_eq!(insert_op2.metadata.content_type, "text/plain");
        assert_eq!(insert_op2.metadata.expiration_policy, expiration);
        assert_eq!(insert_op2.payload.as_ref(), insert2_data);

        let Operation::Delete(delete_op) = &operations[3].as_ref().unwrap() else {
            panic!("expected delete operation");
        };
        assert_eq!(delete_op.key, "test3");
    }
}
