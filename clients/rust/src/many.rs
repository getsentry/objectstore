use std::collections::{HashMap, HashSet};
use std::io;

use async_compression::tokio::bufread::ZstdDecoder;
use futures_util::StreamExt as _;
use multer::Field;
use objectstore_types::metadata::{Compression, Metadata};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, percent_encode};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::multipart::Part;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::error::Error;
use crate::put::{PutBody, maybe_compress_body};
use crate::{
    DeleteBuilder, DeleteResponse, GetBuilder, GetResponse, PutBuilder, PutResponse, Session,
};

const HEADER_BATCH_OPERATION_INDEX: &str = "x-sn-batch-operation-index";
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
        BatchOperation::Get {
            key: value.key,
            decompress: value.decompress,
        }
    }
}

impl From<PutBuilder> for BatchOperation {
    fn from(value: PutBuilder) -> Self {
        BatchOperation::Insert {
            key: value.key,
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

                let body = maybe_compress_body(body, metadata.compression);
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
    /// The server returned an error for one of the operations, but it's impossible to determine
    /// which one exactly.
    /// This can happen if either the client or the server encounter parsing errors, batch size
    /// limits  or unexpected request/response formats.
    Error(Error),
}

impl OperationResult {
    async fn from_field(
        field: Field<'_>,
        decompress_map: &HashMap<usize, bool>,
    ) -> (Option<usize>, Self) {
        match Self::try_from_field(field, decompress_map).await {
            Ok((index, result)) => (index, result),
            Err(e) => (None, OperationResult::Error(e)),
        }
    }

    async fn try_from_field(
        field: Field<'_>,
        decompress_map: &HashMap<usize, bool>,
    ) -> Result<(Option<usize>, Self), Error> {
        let mut headers = field.headers().clone();
        let index: Option<usize> = headers
            .remove(HEADER_BATCH_OPERATION_INDEX)
            .and_then(|v| v.to_str().ok().and_then(|s| s.parse().ok()));
        let key = headers.remove(HEADER_BATCH_OPERATION_KEY).and_then(|v| {
            v.to_str()
                .ok()
                .and_then(|encoded| percent_decode_str(encoded).decode_utf8().ok())
                .map(|s| s.into_owned())
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

        let (key, kind) = match (key, kind) {
            (Some(k), Some(kd)) => (k, kd),
            (None, None) => {
                let body = field.bytes().await?;
                let message = String::from_utf8_lossy(&body).into_owned();
                return Ok((
                    index,
                    OperationResult::Error(Error::OperationError { status, message }),
                ));
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
                "get" => Ok((index, OperationResult::Get(key, Err(error)))),
                "insert" => Ok((index, OperationResult::Put(key, Err(error)))),
                "delete" => Ok((index, OperationResult::Delete(key, Err(error)))),
                _ => Ok((index, OperationResult::Error(error))),
            };
        }

        let result = match kind.as_str() {
            "get" => {
                if status == 404 {
                    OperationResult::Get(key, Ok(None))
                } else {
                    let mut metadata = Metadata::from_headers(&headers, "")?;
                    let should_decompress = index
                        .and_then(|idx| decompress_map.get(&idx).copied())
                        .unwrap_or(false);

                    let stream = match (metadata.compression, should_decompress) {
                        (Some(Compression::Zstd), true) => {
                            metadata.compression = None;
                            let stream =
                                futures_util::stream::once(async move { Ok::<_, io::Error>(body) });
                            ReaderStream::new(ZstdDecoder::new(StreamReader::new(stream))).boxed()
                        }
                        _ => futures_util::stream::once(async move { Ok(body) }).boxed(),
                    };

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
    /// The results are not guaranteed to be in the same order as the original enqueuing order.
    pub async fn send(mut self) -> crate::Result<OperationResults> {
        let mut all_results = Vec::new();

        while !self.operations.is_empty() {
            let chunk_size = self.operations.len().min(MAX_BATCH_SIZE);
            let chunk: Vec<_> = self.operations.drain(..chunk_size).collect();
            let results = self.send_batch(chunk).await?;
            all_results.extend(results);
        }

        Ok(OperationResults(all_results))
    }

    async fn send_batch(
        &self,
        operations: Vec<BatchOperation>,
    ) -> crate::Result<Vec<OperationResult>> {
        let decompress_map: HashMap<usize, bool> = operations
            .iter()
            .enumerate()
            .filter_map(|(idx, op)| match op {
                BatchOperation::Get { decompress, .. } => Some((idx, *decompress)),
                _ => None,
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
            let (index, result) = OperationResult::from_field(field, &decompress_map).await;
            if let Some(idx) = index {
                seen_indices.insert(idx);
            }
            results.push(result);
        }

        for idx in 0..num_operations {
            if !seen_indices.contains(&idx) {
                results.push(OperationResult::Error(Error::MalformedResponse(format!(
                    "server did not return a response for operation at index {idx}"
                ))));
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
