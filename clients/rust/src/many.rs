use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::{Stream, StreamExt as _};
use multer::Field;
use objectstore_types::metadata::{Compression, Metadata};
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

/// Maximum number of operations to send in a batch request.
const MAX_BATCH_OPS: usize = 1000;

/// Maximum amount of bytes to send as a part's body in a batch request.
const MAX_BATCH_PART_SIZE: u32 = 1024 * 1024; // 1 MB

/// Operations that are guaranteed to exceed `MAX_BATCH_PART_SIZE` are executed using requests to
/// the individual object endpoint, rather than the batch endpoint.
/// This determines the maximum number of such requests that can be executed concurrently.
const MAX_INDIVIDUAL_CONCURRENCY: usize = 5;

/// Maximum number of requests to the batch endpoint that can be executed concurrently.
const MAX_BATCH_CONCURRENCY: usize = 3;

// TODO: add limit and logic for whole batch request body size

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
#[allow(clippy::large_enum_variant)]
enum BatchOperation {
    Get {
        key: ObjectKey,
        decompress: bool,
        accept_encoding: Vec<Compression>,
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
            accept_encoding,
            session: _session,
        } = value;
        BatchOperation::Get {
            key,
            decompress,
            accept_encoding,
        }
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
                let headers = operation_headers("get", Some(&key));
                Ok(Part::text("").headers(headers))
            }
            BatchOperation::Insert {
                key,
                metadata,
                body,
            } => {
                let mut headers = operation_headers("insert", key.as_deref());
                headers.extend(metadata.to_headers("")?);

                let body = put::maybe_compress(body, metadata.compression).await?;
                Ok(Part::stream(body).headers(headers))
            }
            BatchOperation::Delete { key } => {
                let headers = operation_headers("delete", Some(&key));
                Ok(Part::text("").headers(headers))
            }
        }
    }
}

fn operation_headers(operation: &str, key: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static(HEADER_BATCH_OPERATION_KIND),
        HeaderValue::from_str(operation).expect("operation kind is always a valid header value"),
    );
    if let Some(key) = key {
        let encoded =
            percent_encoding::percent_encode(key.as_bytes(), NON_ALPHANUMERIC).to_string();
        headers.insert(
            HeaderName::from_static(HEADER_BATCH_OPERATION_KEY),
            HeaderValue::try_from(encoded)
                .expect("percent-encoded string is always a valid header value"),
        );
    }
    headers
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
    Get {
        key: ObjectKey,
        decompress: bool,
        accept_encoding: Vec<Compression>,
    },
    Insert {
        key: Option<ObjectKey>,
    },
    Delete {
        key: ObjectKey,
    },
}

impl From<&BatchOperation> for OperationContext {
    fn from(op: &BatchOperation) -> Self {
        match op {
            BatchOperation::Get {
                key,
                decompress,
                accept_encoding,
            } => OperationContext::Get {
                key: key.clone(),
                decompress: *decompress,
                accept_encoding: accept_encoding.clone(),
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

/// The result of classifying a single operation for batch processing.
enum Classified {
    /// The operation can be included in a batch request.
    Batchable(BatchOperation),
    /// The operation must be executed as an individual request (e.g., oversized file body).
    Individual(BatchOperation),
    /// An error was encountered during classification.
    Failed(OperationResult),
}

/// Creates a typed error [`OperationResult`] for the given operation context.
fn error_result(ctx: OperationContext, error: Error) -> OperationResult {
    let key = ctx.key().unwrap_or("<unknown>").to_owned();
    match ctx {
        OperationContext::Get { .. } => OperationResult::Get(key, Err(error)),
        OperationContext::Insert { .. } => OperationResult::Put(key, Err(error)),
        OperationContext::Delete { .. } => OperationResult::Delete(key, Err(error)),
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
            OperationContext::Get {
                decompress,
                accept_encoding,
                ..
            } => {
                if status == 404 {
                    OperationResult::Get(key, Ok(None))
                } else {
                    let mut metadata = Metadata::from_headers(&headers, "")?;

                    let stream =
                        futures_util::stream::once(async move { Ok::<_, io::Error>(body) }).boxed();
                    let stream =
                        get::maybe_decompress(stream, &mut metadata, *decompress, accept_encoding);

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
    let mut context_map: HashMap<usize, OperationContext> = operations
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
            let result = match context_map.remove(&idx) {
                Some(ctx) => error_result(ctx, error),
                None => OperationResult::Error(error),
            };
            results.push(result);
        }
    }

    Ok(results)
}

fn classify_fail(key: Option<ObjectKey>, error: Error) -> Classified {
    Classified::Failed(OperationResult::Put(
        key.unwrap_or_else(|| "<unknown>".to_owned()),
        Err(error),
    ))
}

/// Classifies a single operation for batch processing.
///
/// Insert operations whose body exceeds [`MAX_BATCH_PART_SIZE`] are marked as
/// [`Classified::Individual`]. Everything else is [`Classified::Batchable`].
async fn classify(op: BatchOperation) -> Classified {
    match op {
        BatchOperation::Insert {
            key,
            metadata,
            body,
        } => {
            let size = match &body {
                PutBody::Buffer(bytes) => Some(bytes.len() as u64),
                PutBody::File(file) => match file.metadata().await {
                    Ok(meta) => Some(meta.len()),
                    Err(err) => return classify_fail(key, err.into()),
                },
                PutBody::Path(path) => match tokio::fs::metadata(path).await {
                    Ok(meta) => Some(meta.len()),
                    Err(err) => return classify_fail(key, err.into()),
                },
                // Streams have unknown size and must not go through the batch endpoint.
                PutBody::Stream(_) => None,
            };

            let op = BatchOperation::Insert {
                key,
                metadata,
                body,
            };

            if size.is_some_and(|s| s <= MAX_BATCH_PART_SIZE as u64) {
                Classified::Batchable(op)
            } else {
                Classified::Individual(op)
            }
        }
        // TODO: similar handling for other `PutBody` variants
        other => Classified::Batchable(other),
    }
}

/// Classifies all operations, partitioning them into batchable, individual, and failed.
///
/// Classification is parallelized since it may involve FS I/O (e.g., stat calls).
async fn partition(
    operations: Vec<BatchOperation>,
) -> (
    Vec<BatchOperation>,
    Vec<BatchOperation>,
    Vec<OperationResult>,
) {
    let classified = futures_util::future::join_all(operations.into_iter().map(classify)).await;
    let mut batchable = Vec::new();
    let mut individual = Vec::new();
    let mut failed = Vec::new();
    for item in classified {
        match item {
            Classified::Batchable(op) => batchable.push(op),
            Classified::Individual(op) => individual.push(op),
            Classified::Failed(result) => failed.push(result),
        }
    }
    (batchable, individual, failed)
}

/// Executes a single operation as an individual (non-batch) request.
async fn execute_individual(op: BatchOperation, session: &Session) -> OperationResult {
    match op {
        BatchOperation::Get {
            key,
            decompress,
            accept_encoding,
        } => {
            let get = GetBuilder {
                session: session.clone(),
                key: key.clone(),
                decompress,
                accept_encoding,
            };
            OperationResult::Get(key, get.send().await)
        }
        BatchOperation::Insert {
            key,
            metadata,
            body,
        } => {
            let error_key = key.clone().unwrap_or_else(|| "<unknown>".to_owned());
            let put = PutBuilder {
                session: session.clone(),
                metadata,
                key,
                body,
            };
            match put.send().await {
                Ok(response) => OperationResult::Put(response.key.clone(), Ok(response)),
                Err(err) => OperationResult::Put(error_key, Err(err)),
            }
        }
        BatchOperation::Delete { key } => {
            let delete = DeleteBuilder {
                session: session.clone(),
                key: key.clone(),
            };
            OperationResult::Delete(key, delete.send().await)
        }
    }
}

/// Sends a chunk of operations as a single batch request.
///
/// On batch-level failure, produces per-operation error results.
async fn execute_batch(operations: Vec<BatchOperation>, session: &Session) -> Vec<OperationResult> {
    let contexts: Vec<_> = operations.iter().map(OperationContext::from).collect();
    match send_batch(session, operations).await {
        Ok(results) => results,
        Err(e) => {
            let shared = Arc::new(e);
            contexts
                .into_iter()
                .map(|ctx| error_result(ctx, Error::Batch(shared.clone())))
                .collect()
        }
    }
}

impl ManyBuilder {
    /// Consumes this builder, returning a lazy stream over all the enqueued operations' results.
    ///
    /// The results are not guaranteed to be in the order they were originally enqueued in.
    pub async fn send(self) -> OperationResults {
        let session = self.session;

        // Classify all operations
        let (batchable, individual, failed) = partition(self.operations).await;

        // Execute individual requests for items that are too large, concurrently
        let individual_results = futures_util::stream::iter(individual)
            .map({
                let session = session.clone();
                move |op| {
                    let session = session.clone();
                    async move { execute_individual(op, &session).await }
                }
            })
            .buffer_unordered(MAX_INDIVIDUAL_CONCURRENCY);

        // Lazily chunk batchable operations and execute as batch requests, concurrently
        let batch_results = futures_util::stream::unfold(batchable, |mut remaining| async {
            if remaining.is_empty() {
                return None;
            }
            let at = remaining.len().min(MAX_BATCH_OPS);
            let chunk: Vec<_> = remaining.drain(..at).collect();
            Some((chunk, remaining))
        })
        .map(move |chunk| {
            let session = session.clone();
            async move { execute_batch(chunk, &session).await }
        })
        .buffer_unordered(MAX_BATCH_CONCURRENCY)
        .flat_map(futures_util::stream::iter);

        let results = futures_util::stream::iter(failed)
            .chain(individual_results)
            .chain(batch_results);

        OperationResults(results.boxed())
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
