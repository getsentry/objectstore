use std::collections::{HashMap, HashSet};
use std::io;

use futures_util::StreamExt as _;
use multer::Field;
use objectstore_types::metadata::Metadata;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, percent_encode};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::multipart::Part;

use crate::error::Error;
use crate::get::maybe_decompress;
use crate::put::{PutBody, maybe_compress};
use crate::{
    DeleteBuilder, DeleteResponse, GetBuilder, GetResponse, PutBuilder, PutResponse, Session,
};

const HEADER_BATCH_OPERATION_INDEX: &str = "x-sn-batch-operation-index";
const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";
const HEADER_BATCH_OPERATION_KIND: &str = "x-sn-batch-operation-kind";
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

/// Context about a request operation, used to interpret response parts without
/// relying on server-provided kind/key headers.
struct OperationContext {
    kind: &'static str,
    key: Option<String>,
    decompress: bool,
}

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
        key: String,
        decompress: bool,
    },
    Insert {
        key: Option<String>,
        metadata: Metadata,
        body: PutBody,
    },
    Delete {
        key: String,
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

                let body = maybe_compress(body, metadata.compression);
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
    let encoded = percent_encode(key.as_bytes(), NON_ALPHANUMERIC).to_string();
    HeaderValue::try_from(encoded).expect("percent-encoded string is always a valid header value")
}

/// The result of an individual operation.
#[derive(Debug)]
pub enum OperationResult {
    /// The result of a get operation. Returns `Ok(None)` if the object was not found.
    Get(String, Result<Option<GetResponse>, Error>),
    /// The result of a put operation.
    Put(String, Result<PutResponse, Error>),
    /// The result of a delete operation.
    Delete(String, Result<DeleteResponse, Error>),
    /// An error occurred while parsing or correlating a response part, making it impossible
    /// to attribute the error to a specific operation.
    /// This can happen if the response contains malformed or missing headers, references
    /// unknown operation indices, or if a network error occurs while reading a response part.
    Error(Error),
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
                    .and_then(|encoded| percent_decode_str(encoded).decode_utf8().ok())
                    .map(|s| s.into_owned())
            })
            .or_else(|| ctx.key.clone())
            .ok_or_else(|| {
                Error::MalformedResponse(format!(
                    "missing or invalid {HEADER_BATCH_OPERATION_KEY} header"
                ))
            })?;

        let kind = ctx.kind;

        let body = field.bytes().await?;

        let is_error = status >= 400 && !(kind == "get" && status == 404);
        if is_error {
            let message = String::from_utf8_lossy(&body).into_owned();
            let error = Error::OperationFailure { status, message };

            return match kind {
                "get" => Ok((index, OperationResult::Get(key, Err(error)))),
                "insert" => Ok((index, OperationResult::Put(key, Err(error)))),
                "delete" => Ok((index, OperationResult::Delete(key, Err(error)))),
                _ => Ok((index, OperationResult::Error(error))),
            };
        }

        let result = match kind {
            "get" => {
                if status == 404 {
                    OperationResult::Get(key, Ok(None))
                } else {
                    let mut metadata = Metadata::from_headers(&headers, "")?;

                    let stream =
                        futures_util::stream::once(async move { Ok::<_, io::Error>(body) }).boxed();
                    let stream = maybe_decompress(stream, &mut metadata, ctx.decompress);

                    OperationResult::Get(key, Ok(Some(GetResponse { metadata, stream })))
                }
            }
            "insert" => OperationResult::Put(key.clone(), Ok(PutResponse { key })),
            "delete" => OperationResult::Delete(key, Ok(())),
            _ => {
                return Err(Error::MalformedResponse(format!(
                    "unknown operation kind: {kind}, body: {}",
                    String::from_utf8_lossy(&body)
                )));
            }
        };
        Ok((index, result))
    }
}

/// Container for the results of all operations in a many request.
#[derive(Debug)]
pub struct OperationResults(Vec<OperationResult>);

impl IntoIterator for OperationResults {
    type Item = OperationResult;
    type IntoIter = std::vec::IntoIter<OperationResult>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl OperationResults {
    /// Consumes the results, returning an error if any of the operations failed.
    pub fn error_for_failures(self) -> crate::Result<(), Vec<crate::Error>> {
        let errs: Vec<_> = self
            .0
            .into_iter()
            .filter_map(|res| match res {
                OperationResult::Get(_, get) => get.err(),
                OperationResult::Put(_, put) => put.err(),
                OperationResult::Delete(_, delete) => delete.err(),
                OperationResult::Error(error) => Some(error),
            })
            .collect();
        if errs.is_empty() {
            return Ok(());
        }
        Err(errs)
    }
}

impl ManyBuilder {
    /// Executes all enqueued operations, returning an iterator over their results.
    /// The results are not guaranteed to be in the order they were originally enqueued in.
    pub async fn send(mut self) -> OperationResults {
        let mut all_results = Vec::new();

        while !self.operations.is_empty() {
            let chunk_size = self.operations.len().min(MAX_BATCH_SIZE);
            let chunk: Vec<_> = self.operations.drain(..chunk_size).collect();

            // Extract operation context before send_batch consumes the chunk,
            // so we can create typed error results if the batch fails.
            let contexts: Vec<_> = chunk
                .iter()
                .map(|op| match op {
                    BatchOperation::Get { key, .. } => ("get", key.clone()),
                    BatchOperation::Insert { key, .. } => {
                        ("insert", key.clone().unwrap_or_default())
                    }
                    BatchOperation::Delete { key } => ("delete", key.clone()),
                })
                .collect();

            match self.send_batch(chunk).await {
                Ok(results) => all_results.extend(results),
                Err(e) => {
                    let msg = e.to_string();
                    all_results.extend(contexts.into_iter().map(|(kind, key)| {
                        let error = Error::MalformedResponse(msg.clone());
                        match kind {
                            "get" => OperationResult::Get(key, Err(error)),
                            "insert" => OperationResult::Put(key, Err(error)),
                            "delete" => OperationResult::Delete(key, Err(error)),
                            _ => OperationResult::Error(error),
                        }
                    }));
                }
            }
        }

        OperationResults(all_results)
    }

    async fn send_batch(
        &self,
        operations: Vec<BatchOperation>,
    ) -> crate::Result<Vec<OperationResult>> {
        let context_map: HashMap<usize, OperationContext> = operations
            .iter()
            .enumerate()
            .map(|(idx, op)| {
                let ctx = match op {
                    BatchOperation::Get { key, decompress } => OperationContext {
                        kind: "get",
                        key: Some(key.clone()),
                        decompress: *decompress,
                    },
                    BatchOperation::Insert { key, .. } => OperationContext {
                        kind: "insert",
                        key: key.clone(),
                        decompress: false,
                    },
                    BatchOperation::Delete { key } => OperationContext {
                        kind: "delete",
                        key: Some(key.clone()),
                        decompress: false,
                    },
                };
                (idx, ctx)
            })
            .collect();
        let num_operations = operations.len();

        let mut form = reqwest::multipart::Form::new();
        for op in operations.into_iter() {
            let part = op.into_part().await?;
            form = form.part("part", part);
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
                        let key = ctx.key.clone().unwrap_or_default();
                        match ctx.kind {
                            "get" => OperationResult::Get(key, Err(error)),
                            "insert" => OperationResult::Put(key, Err(error)),
                            "delete" => OperationResult::Delete(key, Err(error)),
                            _ => OperationResult::Error(error),
                        }
                    }
                    None => OperationResult::Error(error),
                };
                results.push(result);
            }
        }

        Ok(results)
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
