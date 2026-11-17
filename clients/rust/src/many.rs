use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::{Stream, StreamExt as _};
use multer::Multipart;
use objectstore_types::batch::{
    self, Descriptor, MAX_BATCH_OPERATIONS, MAX_BATCH_PART_SIZE, PAYLOAD_SUFFIX, ResultPart,
};
use objectstore_types::metadata::{Compression, Metadata, ReadMetadata, WriteMetadata};
use reqwest::header::CONTENT_TYPE;
use reqwest::multipart::{Form, Part};

use crate::error::Error;
use crate::instrument::OperationSpan;
use crate::metadata;
use crate::put::{self, CompressionMode, PutBody};
use crate::response::ResponseExt as _;
use crate::{
    DeleteBuilder, DeleteResponse, GetBuilder, GetResponse, HeadBuilder, HeadResponse, ObjectKey,
    PutBuilder, PutResponse, Session, get,
};

/// Default maximum number of concurrent individual (non-batch) requests.
///
/// Can be overridden via [`ManyBuilder::max_individual_concurrency`].
const DEFAULT_INDIVIDUAL_CONCURRENCY: usize = 5;

/// Default maximum number of concurrent batch requests.
///
/// Can be overridden via [`ManyBuilder::max_batch_concurrency`].
const DEFAULT_BATCH_CONCURRENCY: usize = 3;

/// Maximum total body (post-compression, estimated) size to include in a single batch request.
const MAX_BATCH_BODY_SIZE: u64 = 100 * 1024 * 1024; // 100 MB

/// A builder that can be used to enqueue multiple operations.
///
/// The client can optionally execute the operations as batch requests, leading to
/// reduced network overhead.
#[derive(Debug)]
pub struct ManyBuilder {
    session: Session,
    operations: Vec<BatchOperation>,
    max_individual_concurrency: Option<usize>,
    max_batch_concurrency: Option<usize>,
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
            max_individual_concurrency: None,
            max_batch_concurrency: None,
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
        compression: Option<CompressionMode>,
        body: PutBody,
    },
    Delete {
        key: ObjectKey,
    },
    Head {
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
            compression,
            body,
            session: _session,
            resumable_threshold_bytes: _,
        } = value;
        BatchOperation::Insert {
            key,
            metadata,
            compression,
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

impl From<HeadBuilder> for BatchOperation {
    fn from(value: HeadBuilder) -> Self {
        let HeadBuilder {
            key,
            session: _session,
        } = value;
        BatchOperation::Head { key }
    }
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
    /// The result of a head (metadata-only) operation.
    ///
    /// Returns `Ok(None)` if the object was not found.
    Head(ObjectKey, Result<HeadResponse, Error>),
    /// An error occurred while parsing or correlating a response part.
    ///
    /// This makes it impossible to attribute the error to a specific operation.
    /// It can happen if the response contains a part with an invalid name, references
    /// an unknown operation index, or if a network error occurs while reading a response part.
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
    Head {
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
            BatchOperation::Head { key } => OperationContext::Head { key: key.clone() },
        }
    }
}

impl OperationContext {
    fn key(&self) -> Option<&str> {
        match self {
            OperationContext::Get { key, .. }
            | OperationContext::Delete { key }
            | OperationContext::Head { key } => Some(key),
            OperationContext::Insert { key } => key.as_deref(),
        }
    }
}

/// The result of classifying a single operation for batch processing.
#[derive(Debug)]
enum Classified {
    /// The operation can be included in a batch request, with its estimated body size in bytes.
    Batchable(BatchOperation, u64),
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
        OperationContext::Head { .. } => OperationResult::Head(key, Err(error)),
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
    pub async fn error_for_failures(mut self) -> crate::Result<(), impl Iterator<Item = Error>> {
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
                OperationResult::Head(_, head) => {
                    if let Err(e) = head {
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
    let contexts: BTreeMap<usize, OperationContext> = operations
        .iter()
        .enumerate()
        .map(|(idx, op)| (idx, OperationContext::from(op)))
        .collect();

    let form = build_form(operations).await?;
    let request = session.batch_request()?.multipart(form);
    let response = request.send().await?.error_for_status_and_drain().await?;

    let boundary = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::MalformedResponse("missing Content-Type header".to_owned()))
        .map(multer::parse_boundary)??;

    // XXX: We don't use `drain_body` here, as that would require buffering the entire batch
    // response. That's because `multer` doesn't drain nor expose its underlying stream after a
    // parse error.
    let byte_stream = response.bytes_stream().map(|r| r.map_err(io::Error::other));
    let multipart = multer::Multipart::new(byte_stream, boundary);

    Ok(collect_results(multipart, contexts).await)
}

// --- Wire format ---------------------------------------------------------------------------
//
// Each operation `i` is sent as a JSON descriptor part named `"<i>"` with `kind`, `key` and, for
// inserts, a `WriteMetadata` object. An insert is followed by a payload part named
// `"<i>.payload"`. Parts carry no headers besides `Content-Disposition` and `Content-Type`.
//
// The server answers with one JSON result part named `"<i>"` per operation, in completion order.
// A successful get is followed by its payload part `"<i>.payload"`. A missing result counts as a
// failure of that operation.

/// Serializes a descriptor into a part. Like `FormData` does for strings, it sets no
/// `Content-Type`, which the server ignores.
fn descriptor_part(descriptor: &Descriptor) -> Part {
    // INVARIANT: The descriptor only holds strings and string maps, which always serialize.
    Part::bytes(serde_json::to_vec(descriptor).expect("descriptor serializes to JSON"))
}

impl BatchOperation {
    /// Adds the descriptor part and, for inserts, the payload part of the operation at `index`.
    async fn add_to_form(self, form: Form, index: usize) -> crate::Result<Form> {
        let descriptor = match self {
            BatchOperation::Get { key, .. } => Descriptor::Get { key },
            BatchOperation::Delete { key } => Descriptor::Delete { key },
            BatchOperation::Head { key } => Descriptor::Head { key },
            BatchOperation::Insert {
                key,
                metadata,
                compression,
                body,
            } => return add_insert_to_form(form, index, key, metadata, compression, body).await,
        };

        Ok(form.part(index.to_string(), descriptor_part(&descriptor)))
    }
}

/// Adds the descriptor and payload parts of the insert at `index`.
async fn add_insert_to_form(
    form: Form,
    index: usize,
    key: Option<ObjectKey>,
    mut metadata: Metadata,
    compression: Option<CompressionMode>,
    body: PutBody,
) -> crate::Result<Form> {
    metadata.compression = compression.map(CompressionMode::compression);
    let descriptor = Descriptor::Insert {
        key,
        metadata: WriteMetadata::from(metadata),
    };

    // The batch request carries a `Content-Length` only if every part's length is known up front.
    // Compressed parts are streamed, so their length is unknown.
    let encoded = put::encode_body(body, compression).await?;
    let payload = match encoded.length {
        Some(length) => Part::stream_with_length(encoded.body, length),
        None => Part::stream(encoded.body),
    };

    Ok(form
        .part(index.to_string(), descriptor_part(&descriptor))
        .part(batch::payload_part_name(index), payload))
}

/// Builds the request body for `operations`, numbering them by their position.
async fn build_form(operations: Vec<BatchOperation>) -> crate::Result<Form> {
    let mut form = Form::new();
    for (index, operation) in operations.into_iter().enumerate() {
        form = operation.add_to_form(form, index).await?;
    }
    Ok(form)
}

/// The name of a part of the response, identifying the operation it belongs to.
#[derive(Debug, PartialEq, Eq)]
enum PartName {
    /// The result part `"<i>"`.
    Result(usize),
    /// The payload part `"<i>.payload"`.
    Payload(usize),
}

impl PartName {
    fn parse(name: &str) -> Option<Self> {
        match name.strip_suffix(PAYLOAD_SUFFIX) {
            Some(index) => index.parse().ok().map(Self::Payload),
            None => name.parse().ok().map(Self::Result),
        }
    }
}

/// The outcome of interpreting a result part.
enum Resolved {
    Done(OperationResult),
    /// A successful get, which completes with the payload part that follows.
    Get {
        key: ObjectKey,
        metadata: Metadata,
    },
}

/// Returns the metadata of a result, which successful gets and heads require.
fn require_metadata(metadata: Option<ReadMetadata>) -> crate::Result<Metadata> {
    let metadata = metadata
        .ok_or_else(|| Error::MalformedResponse("result is missing object metadata".to_owned()))?;
    Ok(Metadata::try_from(metadata)?)
}

/// Interprets the result of the operation that was started with `ctx`.
fn resolve(ctx: &OperationContext, part: ResultPart) -> crate::Result<Resolved> {
    let ResultPart {
        status,
        key,
        metadata,
        error,
        kind: _,
    } = part;

    let not_found_is_ok = matches!(
        ctx,
        OperationContext::Get { .. } | OperationContext::Head { .. }
    );
    if status >= 400 && !(not_found_is_ok && status == 404) {
        // The message is the JSON error body, as the header layout reported it.
        // INVARIANT: The error body only holds strings, which always serialize.
        let message = error
            .map(|error| serde_json::to_string(&error).expect("error body serializes to JSON"))
            .unwrap_or_default();
        return Err(Error::OperationFailure { status, message });
    }

    // Prioritize the server-provided key, which is the only one of an insert without a key.
    let key = key
        .or_else(|| ctx.key().map(str::to_owned))
        .ok_or_else(|| Error::MalformedResponse("result is missing the object key".to_owned()))?;

    let result = match ctx {
        OperationContext::Get { .. } if status == 404 => OperationResult::Get(key, Ok(None)),
        OperationContext::Get { .. } => {
            let metadata = require_metadata(metadata)?;
            return Ok(Resolved::Get { key, metadata });
        }
        OperationContext::Insert { .. } => {
            OperationResult::Put(key.clone(), Ok(PutResponse { key }))
        }
        OperationContext::Delete { .. } => OperationResult::Delete(key, Ok(())),
        OperationContext::Head { .. } if status == 404 => OperationResult::Head(key, Ok(None)),
        OperationContext::Head { .. } => {
            OperationResult::Head(key, Ok(Some(require_metadata(metadata)?)))
        }
    };
    Ok(Resolved::Done(result))
}

/// Completes a get with its payload, decompressing it as the operation asked for.
fn complete_get(
    ctx: &OperationContext,
    key: ObjectKey,
    mut metadata: Metadata,
    payload: Bytes,
) -> OperationResult {
    let (decompress, accept_encoding) = match ctx {
        OperationContext::Get {
            decompress,
            accept_encoding,
            ..
        } => (*decompress, accept_encoding.as_slice()),
        // `resolve` only yields gets for get operations, so this is never reached.
        _ => (false, [].as_slice()),
    };

    let stream = futures_util::stream::once(async move { Ok::<_, io::Error>(payload) }).boxed();
    let stream = get::maybe_decompress(stream, &mut metadata, decompress, accept_encoding);
    OperationResult::Get(key, Ok(Some(GetResponse { metadata, stream })))
}

/// A part of the response, read into memory.
struct ResponsePart {
    /// The parsed name, or `None` if it is missing or invalid.
    name: Option<PartName>,
    body: Bytes,
}

/// Streams the parts of the response, each read into memory. Ends after the first broken part.
fn stream_parts(
    multipart: Multipart<'_>,
) -> impl Stream<Item = Result<ResponsePart, multer::Error>> + '_ {
    futures_util::stream::unfold(Some(multipart), |multipart| async move {
        let mut multipart = multipart?;
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => return None,
            Err(error) => return Some((Err(error), None)),
        };

        let name = field.name().and_then(PartName::parse);
        match field.bytes().await {
            Ok(body) => Some((Ok(ResponsePart { name, body }), Some(multipart))),
            Err(error) => Some((Err(error), None)),
        }
    })
}

/// Reads all results from a batch response in the order they arrive.
///
/// A successful get takes the payload part that directly follows its result. Every operation in
/// `contexts` is accounted for: operations without a result fail. A broken response body ends the
/// stream with one [`OperationResult::Error`], keeping the results read before it.
async fn collect_results(
    multipart: Multipart<'_>,
    mut contexts: BTreeMap<usize, OperationContext>,
) -> Vec<OperationResult> {
    let mut parts = Box::pin(stream_parts(multipart).peekable());
    let mut results = Vec::new();

    while let Some(part) = parts.next().await {
        let part = match part {
            Ok(part) => part,
            Err(error) => {
                results.push(OperationResult::Error(error.into()));
                break;
            }
        };

        let Some(PartName::Result(index)) = part.name else {
            let error =
                Error::MalformedResponse(format!("unexpected response part {:?}", part.name));
            results.push(OperationResult::Error(error));
            continue;
        };
        let Some(ctx) = contexts.remove(&index) else {
            let error = Error::MalformedResponse(format!(
                "response references unknown or duplicate operation index {index}"
            ));
            results.push(OperationResult::Error(error));
            continue;
        };

        let resolved = serde_json::from_slice::<ResultPart>(&part.body)
            .map_err(|error| {
                Error::MalformedResponse(format!(
                    "invalid result for the operation at index {index}: {error}"
                ))
            })
            .and_then(|part| resolve(&ctx, part));

        let result = match resolved {
            Ok(Resolved::Done(result)) => result,
            Ok(Resolved::Get { key, metadata }) => {
                let is_payload = |next: &Result<ResponsePart, _>| matches!(next, Ok(part) if part.name == Some(PartName::Payload(index)));
                match parts.as_mut().next_if(is_payload).await {
                    Some(Ok(payload)) => complete_get(&ctx, key, metadata, payload.body),
                    _ => {
                        let error = Error::MalformedResponse(format!(
                            "response has no payload part for the operation at index {index}"
                        ));
                        error_result(ctx, error)
                    }
                }
            }
            Err(error) => error_result(ctx, error),
        };
        results.push(result);
    }

    for (index, ctx) in contexts {
        let error = Error::MalformedResponse(format!(
            "server did not return a response for operation at index {index}"
        ));
        results.push(error_result(ctx, error));
    }
    results
}

/// Returns the worst-case size of `size` bytes after compressing them with `compression`.
fn compress_bound(compression: Compression, size: usize) -> usize {
    match compression {
        Compression::Zstd => zstd_safe::compress_bound(size),
        Compression::Gzip => deflate_bound(size) + GZIP_FRAMING_SIZE,
    }
}

/// Size of the gzip header and trailer around the deflate stream (RFC 1952).
const GZIP_FRAMING_SIZE: usize = 18;

/// Returns the worst-case size of a raw deflate stream for `size` input bytes.
fn deflate_bound(size: usize) -> usize {
    // Neither `flate2` nor `miniz_oxide` expose a bound, so this mirrors zlib's
    // `compressBound` without its 6-byte zlib wrapper. Incompressible input is emitted
    // as stored blocks with a 5-byte header each, which this conservatively bounds at
    // 5 bytes per 16 KiB of input plus a constant for the final block.
    size + (size >> 12) + (size >> 14) + (size >> 25) + 7
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
            mut metadata,
            compression,
            body,
        } => {
            // Fail only this operation, before it can reach a batch request.
            if let Err(err) = metadata::normalize_keys(&mut metadata) {
                return classify_fail(key, err);
            }

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

            // Client-side compression happens while streaming the body, so the on-wire size is
            // only known up to its worst case. Precompressed bodies are sent as-is.
            let size = match (compression, size) {
                (Some(CompressionMode::Compress(compression)), Some(size)) => usize::try_from(size)
                    .ok()
                    .map(|size| compress_bound(compression, size)),
                (Some(CompressionMode::Precompressed(_)) | None, Some(size)) => {
                    usize::try_from(size).ok()
                }
                (_, None) => None,
            };

            let op = BatchOperation::Insert {
                key,
                metadata,
                compression,
                body,
            };

            match size {
                Some(s) if s <= MAX_BATCH_PART_SIZE => Classified::Batchable(op, s as u64),
                _ => Classified::Individual(op),
            }
        }
        other => Classified::Batchable(other, 0),
    }
}

/// Classifies all operations, partitioning them into batchable, individual, and failed.
///
/// Classification is parallelized since it may involve FS I/O (e.g., stat calls).
async fn partition(
    operations: Vec<BatchOperation>,
) -> (
    Vec<(BatchOperation, u64)>,
    Vec<BatchOperation>,
    Vec<OperationResult>,
) {
    let classified = futures_util::future::join_all(operations.into_iter().map(classify)).await;
    let mut batchable = Vec::new();
    let mut individual = Vec::new();
    let mut failed = Vec::new();
    for item in classified {
        match item {
            Classified::Batchable(op, size) => batchable.push((op, size)),
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
            compression,
            body,
        } => {
            let error_key = key.clone().unwrap_or_else(|| "<unknown>".to_owned());
            let put = PutBuilder {
                // Automatic resumables are currently limited to standalone uploads.
                resumable_threshold_bytes: None,
                session: session.clone(),
                metadata,
                compression,
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
        BatchOperation::Head { key } => {
            let head = HeadBuilder {
                session: session.clone(),
                key: key.clone(),
            };
            OperationResult::Head(key, head.send().await)
        }
    }
}

/// Sends a chunk of operations as a single batch request.
///
/// On batch-level failure, produces per-operation error results.
async fn execute_batch(operations: Vec<BatchOperation>, session: &Session) -> Vec<OperationResult> {
    let contexts: Vec<_> = operations.iter().map(OperationContext::from).collect();
    let span = OperationSpan::batch(session, operations.len());
    match span.instrument(send_batch(session, operations)).await {
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

/// Returns a lazy iterator over batches of operations.
///
/// Each batch respects both the operation-count limit ([`MAX_BATCH_OPERATIONS`]) and the total body-size
/// limit ([`MAX_BATCH_BODY_SIZE`]).
fn iter_batches(ops: Vec<(BatchOperation, u64)>) -> impl Iterator<Item = Vec<BatchOperation>> {
    let mut remaining = ops.into_iter().peekable();

    std::iter::from_fn(move || {
        remaining.peek()?;
        let mut batch_size = 0;
        let mut batch = Vec::new();

        while let Some((_, op_size)) = remaining.peek() {
            if batch.len() >= MAX_BATCH_OPERATIONS
                || (!batch.is_empty() && batch_size + op_size > MAX_BATCH_BODY_SIZE)
            {
                break;
            }

            let (op, op_size) = remaining.next().expect("peeked above");
            batch_size += op_size;
            batch.push(op);
        }

        Some(batch)
    })
}

impl ManyBuilder {
    /// Consumes this builder, returning a lazy stream over all the enqueued operations' results.
    ///
    /// The results are not guaranteed to be in the order they were originally enqueued in.
    pub async fn send(self) -> OperationResults {
        let session = self.session;
        let individual_concurrency = self
            .max_individual_concurrency
            .unwrap_or(DEFAULT_INDIVIDUAL_CONCURRENCY)
            .max(1);
        let batch_concurrency = self
            .max_batch_concurrency
            .unwrap_or(DEFAULT_BATCH_CONCURRENCY)
            .max(1);

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
            .buffer_unordered(individual_concurrency);

        // Chunk batchable operations and execute as batch requests, concurrently
        let batch_results = futures_util::stream::iter(iter_batches(batchable))
            .map(move |chunk| {
                let session = session.clone();
                async move { execute_batch(chunk, &session).await }
            })
            .buffer_unordered(batch_concurrency)
            .flat_map(futures_util::stream::iter);

        let results = futures_util::stream::iter(failed)
            .chain(individual_results)
            .chain(batch_results);

        OperationResults(results.boxed())
    }

    /// Sets the maximum number of concurrent individual (non-batch) requests.
    ///
    /// Operations that exceed the per-part size limit are sent as individual requests.
    /// This controls how many such requests can be in-flight simultaneously.
    /// Defaults to 5 if not set.
    pub fn max_individual_concurrency(mut self, concurrency: usize) -> Self {
        self.max_individual_concurrency = Some(concurrency);
        self
    }

    /// Sets the maximum number of concurrent batch requests.
    ///
    /// Batchable operations are grouped into chunks and sent as multipart batch requests.
    /// This controls how many such batch requests can be in-flight simultaneously.
    /// Defaults to 3 if not set.
    pub fn max_batch_concurrency(mut self, concurrency: usize) -> Self {
        self.max_batch_concurrency = Some(concurrency);
        self
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

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt as _;

    use super::*;

    /// Creates a dummy sized op for use in `iter_batches` tests.
    fn op(size: u64) -> (BatchOperation, u64) {
        (
            BatchOperation::Delete {
                key: "k".to_owned(),
            },
            size,
        )
    }

    fn batch_sizes(batches: &[Vec<BatchOperation>]) -> Vec<usize> {
        batches.iter().map(Vec::len).collect()
    }

    fn batches(ops: Vec<(BatchOperation, u64)>) -> Vec<Vec<BatchOperation>> {
        iter_batches(ops).collect()
    }

    fn put_with_compression(size: usize, compression: CompressionMode) -> BatchOperation {
        BatchOperation::Insert {
            key: Some("k".to_owned()),
            metadata: Metadata::default(),
            compression: Some(compression),
            body: PutBody::Buffer(vec![0; size].into()),
        }
    }

    fn put_with_zstd(size: usize) -> BatchOperation {
        put_with_compression(size, CompressionMode::Compress(Compression::Zstd))
    }

    #[tokio::test]
    async fn zstd_put_at_limit_is_batchable() {
        let size = 1_044_496;
        let post_compression = zstd_safe::compress_bound(size);
        assert!(post_compression == MAX_BATCH_PART_SIZE);

        core::assert_matches!(
            classify(put_with_zstd(size)).await,
            Classified::Batchable(_, s) if s == post_compression as u64
        );
    }

    #[tokio::test]
    async fn zstd_put_above_limit_is_individual() {
        let size = 1_044_497;
        let post_compression = zstd_safe::compress_bound(size);
        assert!(post_compression > MAX_BATCH_PART_SIZE);

        core::assert_matches!(
            classify(put_with_zstd(size)).await,
            Classified::Individual(_)
        );
    }

    #[tokio::test]
    async fn gzip_put_bound() {
        let size = 1_000_000;
        let op = put_with_compression(size, CompressionMode::Compress(Compression::Gzip));
        std::assert_matches!(
            classify(op).await,
            Classified::Batchable(_, s) if s == compress_bound(Compression::Gzip, size) as u64
        );
    }

    #[tokio::test]
    async fn precompressed_put_uses_exact_size() {
        // A size that would exceed the limit once inflated by `compress_bound`, but is sent
        // verbatim because the payload is already compressed.
        let size = MAX_BATCH_PART_SIZE;
        assert!(zstd_safe::compress_bound(size) > MAX_BATCH_PART_SIZE);

        let op = put_with_compression(size, CompressionMode::Precompressed(Compression::Zstd));
        core::assert_matches!(
            classify(op).await,
            Classified::Batchable(_, s) if s == size as u64
        );
    }

    #[test]
    fn iter_batches_empty() {
        assert!(batches(vec![]).is_empty());
    }

    #[test]
    fn iter_batches_single_batch_count_limit() {
        // 1000 tiny ops → exactly one batch
        let ops: Vec<_> = (0..1000).map(|_| op(1)).collect();
        assert_eq!(batch_sizes(&batches(ops)), vec![1000]);
    }

    #[test]
    fn iter_batches_splits_on_count_limit() {
        // 1001 tiny ops → two batches: 1000 + 1
        let ops: Vec<_> = (0..1001).map(|_| op(1)).collect();
        assert_eq!(batch_sizes(&batches(ops)), vec![1000, 1]);
    }

    #[test]
    fn iter_batches_exactly_at_size_limit() {
        // 100 ops of 1 MB each = exactly 100 MB → one batch
        let ops: Vec<_> = (0..100).map(|_| op(1024 * 1024)).collect();
        assert_eq!(batch_sizes(&batches(ops)), vec![100]);
    }

    #[test]
    fn iter_batches_splits_on_size_limit() {
        // 101 ops of 1 MB each = 101 MB → two batches: 100 + 1
        let ops: Vec<_> = (0..101).map(|_| op(1024 * 1024)).collect();
        assert_eq!(batch_sizes(&batches(ops)), vec![100, 1]);
    }

    #[test]
    fn iter_batches_size_limit_hits_before_count_limit() {
        // 200 ops of ~600 KB each → size limit triggers before the 1000-op count limit
        let op_size = 600 * 1024;
        let ops: Vec<_> = (0..200).map(|_| op(op_size)).collect();
        let result = batches(ops);
        // Each batch holds floor(100 MB / 600 KB) ops
        let per_batch = (MAX_BATCH_BODY_SIZE / op_size) as usize;
        assert!(result.len() > 1, "expected multiple batches");
        for batch in &result[..result.len() - 1] {
            assert_eq!(batch.len(), per_batch);
        }
    }

    fn get(key: &str) -> BatchOperation {
        BatchOperation::Get {
            key: key.to_owned(),
            decompress: true,
            accept_encoding: vec![],
        }
    }

    fn insert(key: Option<&str>, metadata: Metadata, body: &'static [u8]) -> BatchOperation {
        BatchOperation::Insert {
            key: key.map(str::to_owned),
            metadata,
            compression: None,
            body: PutBody::Buffer(Bytes::from_static(body)),
        }
    }

    /// Encodes `operations` into the body of a batch request.
    async fn encode(operations: Vec<BatchOperation>) -> String {
        let form = build_form(operations).await.unwrap();
        let mut request = reqwest::Client::new()
            .post("http://localhost/")
            .multipart(form)
            .build()
            .unwrap();
        let body = request.body_mut().take().unwrap();
        String::from_utf8(body.collect().await.unwrap().to_bytes().to_vec()).unwrap()
    }

    #[tokio::test]
    async fn request_wire_format() {
        let metadata = Metadata {
            content_type: "image/png".into(),
            custom: [("build".to_owned(), "123".to_owned())].into(),
            ..Metadata::default()
        };
        let operations = vec![
            get("a/résumé"),
            insert(Some("b"), metadata, b"png"),
            insert(None, Metadata::default(), b"keyless"),
        ];

        let body = encode(operations).await;

        // Descriptors and payloads are named by index, with nothing but `Content-Disposition`.
        let parts = [
            "name=\"0\"\r\n\r\n{\"kind\":\"get\",\"key\":\"a/résumé\"}",
            "name=\"1\"\r\n\r\n{\"kind\":\"insert\",\"key\":\"b\",\"metadata\":{\"content_type\":\"image/png\",\"custom\":{\"build\":\"123\"}}}",
            "name=\"1.payload\"\r\n\r\npng",
            "name=\"2\"\r\n\r\n{\"kind\":\"insert\",\"metadata\":{}}",
            "name=\"2.payload\"\r\n\r\nkeyless",
        ];
        let mut rest = body.as_str();
        for part in parts {
            let expected = format!("Content-Disposition: form-data; {part}");
            let start = rest
                .find(&expected)
                .unwrap_or_else(|| panic!("{expected} in {body}"));
            rest = &rest[start + expected.len()..];
        }
        assert!(!body.to_lowercase().contains("x-sn-"));
    }

    /// Builds a part of a canned response. Payload parts get a filename, like the server's.
    fn part(name: &str, body: &str) -> String {
        let filename = match name.strip_suffix(PAYLOAD_SUFFIX) {
            Some(index) => format!("; filename=\"{index}\""),
            None => String::new(),
        };
        format!(
            "--b\r\nContent-Disposition: form-data; name=\"{name}\"{filename}\r\n\r\n{body}\r\n"
        )
    }

    /// Runs `collect_results` over the parts of a response.
    async fn collect(parts: &[String], operations: &[BatchOperation]) -> Vec<OperationResult> {
        let body = Bytes::from(parts.concat() + "--b--\r\n");
        let contexts = operations.iter().map(OperationContext::from).enumerate();
        let stream = futures_util::stream::once(async { Ok::<_, io::Error>(body) });
        collect_results(Multipart::new(stream, "b"), contexts.collect()).await
    }

    #[tokio::test]
    async fn response_in_completion_order() {
        let operations = [
            insert(Some("a"), Metadata::default(), b""),
            get("b"),
            get("c"),
            insert(None, Metadata::default(), b""),
            BatchOperation::Delete {
                key: "e".to_owned(),
            },
            get("f"),
        ];
        let metadata = r#""metadata":{"content_type":"text/plain","custom":{"build":"1"}}"#;
        let parts = [
            part("1", &format!(r#"{{"status":200,"key":"b",{metadata}}}"#)),
            part("1.payload", "hello"),
            part("0", r#"{"status":201,"key":"a"}"#),
            part("2", r#"{"status":404,"key":"c"}"#),
            part(
                "3",
                r#"{"status":400,"error":{"detail":"bad","causes":["why"]}}"#,
            ),
            // A get without its payload part fails. Operation 4 has no result at all.
            part("5", &format!(r#"{{"status":200,"key":"f",{metadata}}}"#)),
        ];

        let mut results = collect(&parts, &operations).await;

        let OperationResult::Get(_, Ok(Some(object))) = results.remove(0) else {
            panic!("expected an object");
        };
        assert_eq!(object.metadata.custom["build"], "1");
        assert_eq!(object.payload().await.unwrap().as_ref(), b"hello");

        assert!(matches!(
            results.as_slice(),
            [
                OperationResult::Put(a, Ok(_)),
                OperationResult::Get(c, Ok(None)),
                OperationResult::Put(d, Err(Error::OperationFailure { status: 400, .. })),
                OperationResult::Get(f, Err(Error::MalformedResponse(_))),
                OperationResult::Delete(e, Err(Error::MalformedResponse(_))),
            ] if [a, c, d, e, f] == ["a", "c", "<unknown>", "e", "f"]
        ));
    }

    #[tokio::test]
    async fn response_broken_body_fails_remaining_operations() {
        let operations = [get("a"), get("b")];
        let parts = [
            part("0", r#"{"status":404,"key":"a"}"#),
            "--b\r\nnot a header line\r\n\r\n".to_owned(),
        ];

        let results = collect(&parts, &operations).await;
        assert!(matches!(
            results.as_slice(),
            [
                OperationResult::Get(_, Ok(None)),
                OperationResult::Error(Error::Multipart(_)),
                OperationResult::Get(_, Err(_)),
            ]
        ));
    }
}
