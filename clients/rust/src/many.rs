use std::io;

use futures_util::StreamExt as _;
use multer::Field;
use reqwest::header::CONTENT_TYPE;

use crate::error::Error;
use crate::{
    DeleteBuilder, DeleteResponse, GetBuilder, GetResponse, ObjectKey, PutBuilder, PutResponse,
    Session,
};

const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";
const HEADER_BATCH_OPERATION_KIND: &str = "x-sn-batch-operation-kind";
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

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
///
/// You can construct a [`BatchOperation`] out of a [`GetBuilder`], [`PutBuilder`], or
/// [`DeleteBuilder`] using their [`From<T>`] implementation.
#[derive(Debug)]
pub struct BatchOperation {}

impl From<GetBuilder> for BatchOperation {
    fn from(_value: GetBuilder) -> Self {
        todo!()
    }
}

impl From<PutBuilder> for BatchOperation {
    fn from(_value: PutBuilder) -> Self {
        todo!()
    }
}

impl From<DeleteBuilder> for BatchOperation {
    fn from(_value: DeleteBuilder) -> Self {
        todo!()
    }
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
    async fn from_field(field: Field<'_>) -> Self {
        match Self::try_from_field(field).await {
            Ok(result) => result,
            Err(e) => OperationResult::Error(e),
        }
    }

    async fn try_from_field(field: Field<'_>) -> Result<Self, Error> {
        let mut headers = field.headers().clone();
        let key = headers
            .remove(HEADER_BATCH_OPERATION_KEY)
            .and_then(|v| v.to_str().ok().map(|s| s.to_owned()))
            .ok_or_else(|| {
                Error::MalformedResponse(format!(
                    "missing or invalid {HEADER_BATCH_OPERATION_KEY} header"
                ))
            })?;
        let kind = headers
            .remove(HEADER_BATCH_OPERATION_KIND)
            .and_then(|v| v.to_str().ok().map(|s| s.to_owned()))
            .ok_or_else(|| {
                Error::MalformedResponse(format!(
                    "missing or invalid {HEADER_BATCH_OPERATION_KIND} header"
                ))
            })?;
        let status: u16 = headers
            .remove(HEADER_BATCH_OPERATION_STATUS)
            .and_then(|v| v.to_str().ok().and_then(|s| s.parse().ok()))
            .ok_or_else(|| {
                Error::MalformedResponse(format!(
                    "missing or invalid {HEADER_BATCH_OPERATION_STATUS} header"
                ))
            })?;

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
                    let metadata = objectstore_types::Metadata::from_headers(&headers, "")?;
                    let stream = futures_util::stream::once(async move { Ok(body) }).boxed();

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
    pub async fn send(self) -> crate::Result<impl Iterator<Item = OperationResult>> {
        let request = self.session.batch_request()?;
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
            results.push(OperationResult::from_field(field).await);
        }

        Ok(results.into_iter())
    }

    /// Enqueues an operation.
    pub fn push<B: Into<BatchOperation>>(mut self, builder: B) -> Self {
        self.operations.push(builder.into());
        self
    }
}
