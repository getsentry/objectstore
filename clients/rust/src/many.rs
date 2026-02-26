use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_stream::stream;
use futures_util::{Stream, StreamExt as _};
use multer::Field;
use objectstore_types::metadata::Metadata;
use percent_encoding::NON_ALPHANUMERIC;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::multipart::Part;

use crate::error::Error;
use crate::put::PutBody;
use crate::{
    DeleteBuilder, DeleteResponse, GetBuilder, GetResponse, ObjectKey, PutBuilder, PutResponse,
    Session, get, put,
};

const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";
const HEADER_BATCH_OPERATION_KIND: &str = "x-sn-batch-operation-kind";
const HEADER_BATCH_OPERATION_INDEX: &str = "x-sn-batch-operation-index";
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

// TODO: guard agains too large operations (parts) and whole requests
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
    /// Creates a [`ManyBuilder`] associated with this session.
    ///
    /// A [`ManyBuilder`] can be used to enqueue multiple operations, which the client can choose to
    /// send as batch requests via a dedicated endpoint, minimizing network overhead.
    pub fn many(&self) -> ManyBuilder {
        ManyBuilder {
            session: self.clone(),
            operations: vec![],
        }
    }
}

#[derive(Debug)]
enum BatchOperation {
    Get {
        key: ObjectKey,
        decompress: bool,
    },
    Insert {
        key: Option<ObjectKey>,
        metadata: Metadata,
        body: PutBody,
    },
    Delete {
        key: ObjectKey,
    },
}

impl From<GetBuilder> for BatchOperation {
    fn from(value: GetBuilder) -> Self {
        let GetBuilder {
            key,
            decompress,
            session: _session,
        } = value;
        BatchOperation::Get { key, decompress }
    }
}

impl From<PutBuilder> for BatchOperation {
    fn from(value: PutBuilder) -> Self {
        let PutBuilder {
            key,
            metadata,
            body,
            session: _session,
        } = value;
        BatchOperation::Insert {
            key,
            metadata,
            body,
        }
    }
}

impl From<DeleteBuilder> for BatchOperation {
    fn from(value: DeleteBuilder) -> Self {
        let DeleteBuilder {
            key,
            session: _session,
        } = value;
        BatchOperation::Delete { key }
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
                    key_to_header_value(&key),
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
                if let Some(key) = &key {
                    headers.insert(
                        HeaderName::from_static(HEADER_BATCH_OPERATION_KEY),
                        key_to_header_value(key),
                    );
                }
                headers.extend(metadata.to_headers("")?);

                let body = put::maybe_compress(body, metadata.compression);
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
                    key_to_header_value(&key),
                );
                Ok(Part::text("").headers(headers))
            }
        }
    }
}

fn key_to_header_value(key: &str) -> HeaderValue {
    let encoded = percent_encoding::percent_encode(key.as_bytes(), NON_ALPHANUMERIC).to_string();
    HeaderValue::try_from(encoded).expect("percent-encoded string is always a valid header value")
}

/// The result of an individual operation.
#[derive(Debug)]
pub enum OperationResult {
    /// The result of a get operation.
    ///
    /// Returns `Ok(None)` if the object was not found.
    Get(ObjectKey, Result<Option<GetResponse>, Error>),
    /// The result of a put operation.
    Put(ObjectKey, Result<PutResponse, Error>),
    /// The result of a delete operation.
    Delete(ObjectKey, Result<DeleteResponse, Error>),
    /// An error occurred while parsing or correlating a response part.
    ///
    /// This makes it impossible to attribute the error to a specific operation.
    /// It can happen if the response contains malformed or missing headers, references
    /// unknown operation indices, or if a network error occurs while reading a response part.
    Error(Error),
}

/// Context for an operation, used to map a response part to a proper `OperationResult`.
enum OperationContext {
    Get { key: ObjectKey, decompress: bool },
    Insert { key: Option<ObjectKey> },
    Delete { key: ObjectKey },
}

impl From<&BatchOperation> for OperationContext {
    fn from(op: &BatchOperation) -> Self {
        match op {
            BatchOperation::Get { key, decompress } => OperationContext::Get {
                key: key.clone(),
                decompress: *decompress,
            },
            BatchOperation::Insert { key, .. } => OperationContext::Insert { key: key.clone() },
            BatchOperation::Delete { key } => OperationContext::Delete { key: key.clone() },
        }
    }
}

impl OperationContext {
    fn key(&self) -> Option<&str> {
        match self {
            OperationContext::Get { key, .. } | OperationContext::Delete { key } => Some(key),
            OperationContext::Insert { key } => key.as_deref(),
        }
    }
}

impl OperationResult {
    async fn from_field(
        field: Field<'_>,
        context_map: &HashMap<usize, OperationContext>,
    ) -> (Option<usize>, Self) {
        match Self::try_from_field(field, context_map).await {
            Ok((index, result)) => (Some(index), result),
            Err(e) => (None, OperationResult::Error(e)),
        }
    }

    async fn try_from_field(
        field: Field<'_>,
        context_map: &HashMap<usize, OperationContext>,
    ) -> Result<(usize, Self), Error> {
        let mut headers = field.headers().clone();

        let index: usize = headers
            .remove(HEADER_BATCH_OPERATION_INDEX)
            .and_then(|v| v.to_str().ok().and_then(|s| s.parse().ok()))
            .ok_or_else(|| {
                Error::MalformedResponse(format!(
                    "missing or invalid {HEADER_BATCH_OPERATION_INDEX} header"
                ))
            })?;

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

        let ctx = context_map.get(&index).ok_or_else(|| {
            Error::MalformedResponse(format!(
                "response references unknown operation index {index}"
            ))
        })?;

        // Prioritize the server-provided key, fall back to the one from context.
        let key = headers
            .remove(HEADER_BATCH_OPERATION_KEY)
            .and_then(|v| {
                v.to_str()
                    .ok()
                    .and_then(|encoded| {
                        percent_encoding::percent_decode_str(encoded)
                            .decode_utf8()
                            .ok()
                    })
                    .map(|s| s.into_owned())
            })
            .or_else(|| ctx.key().map(str::to_owned));

        let body = field.bytes().await?;

        let is_error =
            status >= 400 && !(matches!(ctx, OperationContext::Get { .. }) && status == 404);

        // For error responses, the key may be absent (e.g., server-generated key inserts
        // that fail before execution — the server never generated a key and the client
        // never provided one). Use a sentinel fallback since there is no key to report.
        // For success responses, the key is always required.
        let key = match key {
            Some(key) => key,
            None if is_error => "<unknown>".to_owned(),
            None => {
                return Err(Error::MalformedResponse(format!(
                    "missing or invalid {HEADER_BATCH_OPERATION_KEY} header"
                )));
            }
        };
        if is_error {
            let message = String::from_utf8_lossy(&body).into_owned();
            let error = Error::OperationFailure { status, message };

            return Ok((
                index,
                match ctx {
                    OperationContext::Get { .. } => OperationResult::Get(key, Err(error)),
                    OperationContext::Insert { .. } => OperationResult::Put(key, Err(error)),
                    OperationContext::Delete { .. } => OperationResult::Delete(key, Err(error)),
                },
            ));
        }

        let result = match ctx {
            OperationContext::Get { decompress, .. } => {
                if status == 404 {
                    OperationResult::Get(key, Ok(None))
                } else {
                    let mut metadata = Metadata::from_headers(&headers, "")?;

                    let stream =
                        futures_util::stream::once(async move { Ok::<_, io::Error>(body) }).boxed();
                    let stream = get::maybe_decompress(stream, &mut metadata, *decompress);

                    OperationResult::Get(key, Ok(Some(GetResponse { metadata, stream })))
                }
            }
            OperationContext::Insert { .. } => {
                OperationResult::Put(key.clone(), Ok(PutResponse { key }))
            }
            OperationContext::Delete { .. } => OperationResult::Delete(key, Ok(())),
        };
        Ok((index, result))
    }
}

/// Container for the results of all operations in a many request.
pub struct OperationResults(Pin<Box<dyn Stream<Item = OperationResult> + Send>>);

impl fmt::Debug for OperationResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OperationResults([Stream])")
    }
}

impl Stream for OperationResults {
    type Item = OperationResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.as_mut().poll_next(cx)
    }
}

impl OperationResults {
    /// Drains the stream and collects any per-operation errors.
    ///
    /// Returns an error containing an iterator of all individual errors for the operations
    /// that failed, if any.
    pub async fn error_for_failures(
        mut self,
    ) -> crate::Result<(), impl Iterator<Item = crate::Error>> {
        let mut errs = Vec::new();
        while let Some(res) = self.next().await {
            match res {
                OperationResult::Get(_, get) => {
                    if let Err(e) = get {
                        errs.push(e);
                    }
                }
                OperationResult::Put(_, put) => {
                    if let Err(e) = put {
                        errs.push(e);
                    }
                }
                OperationResult::Delete(_, delete) => {
                    if let Err(e) = delete {
                        errs.push(e);
                    }
                }
                OperationResult::Error(error) => errs.push(error),
            }
        }
        if errs.is_empty() {
            return Ok(());
        }
        Err(errs.into_iter())
    }
}

async fn send_batch(
    session: &Session,
    operations: Vec<BatchOperation>,
) -> crate::Result<Vec<OperationResult>> {
    let context_map: HashMap<usize, OperationContext> = operations
        .iter()
        .enumerate()
        .map(|(idx, op)| (idx, OperationContext::from(op)))
        .collect();
    let num_operations = operations.len();

    let mut form = reqwest::multipart::Form::new();
    for op in operations.into_iter() {
        let part = op.into_part().await?;
        form = form.part("part", part);
    }

    let request = session.batch_request()?.multipart(form);
    let response = request.send().await?.error_for_status()?;

    let boundary = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::MalformedResponse("missing Content-Type header".to_owned()))
        .map(multer::parse_boundary)??;

    let byte_stream = response.bytes_stream().map(|r| r.map_err(io::Error::other));
    let mut multipart = multer::Multipart::new(byte_stream, boundary);

    let mut results = Vec::new();
    let mut seen_indices = HashSet::new();
    while let Some(field) = multipart.next_field().await? {
        let (index, result) = OperationResult::from_field(field, &context_map).await;
        if let Some(idx) = index {
            seen_indices.insert(idx);
        }
        results.push(result);
    }

    for idx in 0..num_operations {
        if !seen_indices.contains(&idx) {
            let error = Error::MalformedResponse(format!(
                "server did not return a response for operation at index {idx}"
            ));
            let result = match context_map.get(&idx) {
                Some(ctx) => {
                    let key = ctx.key().unwrap_or("<unknown>").to_owned();
                    match ctx {
                        OperationContext::Get { .. } => OperationResult::Get(key, Err(error)),
                        OperationContext::Insert { .. } => OperationResult::Put(key, Err(error)),
                        OperationContext::Delete { .. } => OperationResult::Delete(key, Err(error)),
                    }
                }
                None => OperationResult::Error(error),
            };
            results.push(result);
        }
    }

    Ok(results)
}

impl ManyBuilder {
    /// Executes all enqueued operations, returning a stream over their results.
    ///
    /// The results are not guaranteed to be in the order they were originally enqueued in.
    pub fn send(self) -> OperationResults {
        let session = self.session;
        let mut operations = self.operations;

        let inner = stream! {
            while !operations.is_empty() {
                let chunk_size = operations.len().min(MAX_BATCH_SIZE);
                let chunk: Vec<_> = operations.drain(..chunk_size).collect();

                // Extract operation context before send_batch consumes the chunk,
                // so we can create typed error results if the batch fails.
                let contexts: Vec<_> =
                    chunk.iter().map(OperationContext::from).collect();

                match send_batch(&session, chunk).await {
                    Ok(results) => {
                        for result in results {
                            yield result;
                        }
                    }
                    Err(e) => {
                        let shared = std::sync::Arc::new(e);
                        for ctx in contexts {
                            let error = Error::Batch(shared.clone());
                            let key = ctx.key().unwrap_or("<unknown>").to_owned();
                            yield match ctx {
                                OperationContext::Get { .. } => OperationResult::Get(key, Err(error)),
                                OperationContext::Insert { .. } => {
                                    OperationResult::Put(key, Err(error))
                                }
                                OperationContext::Delete { .. } => {
                                    OperationResult::Delete(key, Err(error))
                                }
                            };
                        }
                    }
                }
            }
        };

        OperationResults(Box::pin(inner))
    }

    /// Enqueues an operation.
    ///
    /// This method takes a [`GetBuilder`]/[`PutBuilder`]/[`DeleteBuilder`], which you can
    /// construct using [`Session::get`]/[`Session::put`]/[`Session::delete`].
    ///
    /// **Important**: All pushed builders must originate from the same [`Session`] that was used
    /// to create this [`ManyBuilder`]. Mixing builders from different sessions is not supported
    /// and will result in all operations being executed against this [`ManyBuilder`]'s session,
    /// silently ignoring the original builder's session.
    #[allow(private_bounds)]
    pub fn push<B: Into<BatchOperation>>(mut self, builder: B) -> Self {
        self.operations.push(builder.into());
        self
    }
}
