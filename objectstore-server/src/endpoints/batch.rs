use std::time::SystemTime;

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::response::Response;
use axum::routing;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use http::{HeaderMap, StatusCode};
use objectstore_service::id::{ObjectContext, ObjectId, ObjectKey};
use objectstore_types::Metadata;

use crate::auth::AuthAwareService;
use crate::batch::HEADER_BATCH_OPERATION_KEY;
use crate::endpoints::common::{ApiError, ApiErrorResponse, ApiResult};
use crate::extractors::Xt;
use crate::extractors::batch::{BatchError, BatchOperationStream, Operation};
use crate::multipart::{IntoMultipartResponse, Part};
use crate::rate_limits::MeteredPayloadStream;
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

struct GetResponse {
    pub key: ObjectKey,
    pub result: ApiResult<Option<(Metadata, Bytes)>>,
}

impl std::fmt::Debug for GetResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchGetResponse")
            .field("key", &self.key)
            .finish()
    }
}

#[derive(Debug)]
struct InsertResponse {
    pub key: ObjectKey,
    pub result: ApiResult<objectstore_service::InsertResponse>,
}

#[derive(Debug)]
struct DeleteResponse {
    pub key: ObjectKey,
    pub result: ApiResult<objectstore_service::DeleteResponse>,
}

#[derive(Debug)]
struct ErrorResponse {
    pub error: ApiError,
}

enum OperationResponse {
    Get(GetResponse),
    Insert(InsertResponse),
    Delete(DeleteResponse),
    Error(ErrorResponse),
}

async fn batch(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    mut requests: BatchOperationStream,
) -> Response {
    let responses: BoxStream<OperationResponse> = async_stream::stream! {
            while let Some(operation) = requests.0.next().await {
                if !state.rate_limiter.check(&context) {
                    tracing::debug!("Batch operation rejected due to rate limits");
                    yield OperationResponse::Error(ErrorResponse {
                        error: BatchError::RateLimited.into(),
                    });
                    continue;
                }

                let result = match operation {
                    Ok(operation) => match operation {
                        Operation::Get(get) => {
                            let key = get.key.clone();
                            let result = service
                                .get_object(&ObjectId::new(context.clone(), get.key))
                                .await;

                            let result = match result {
                                Ok(Some((metadata, stream))) => {
                                    let metered_stream = MeteredPayloadStream::from(
                                        stream,
                                        state.rate_limiter.bytes_accumulator()
                                    );
                                    match metered_stream.try_collect::<BytesMut>().await {
                                        Ok(bytes) => Ok(Some((metadata, bytes.freeze()))),
                                        Err(e) => Err(ApiError::Service(e.into())),
                                    }
                                }
                                Ok(None) => Ok(None),
                                Err(e) => Err(e),
                            };

                            OperationResponse::Get(GetResponse{ key, result })
                        }
                        Operation::Insert(insert) => {
                            let key = insert.key.clone();
                            let mut metadata = insert.metadata;
                            metadata.time_created = Some(SystemTime::now());

                            let payload_len = insert.payload.len() as u64;
                            state.rate_limiter.bytes_accumulator()
                                .fetch_add(payload_len, std::sync::atomic::Ordering::Relaxed);

                            let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                            let result = service
                                .insert_object(context.clone(), Some(insert.key), &metadata, stream)
                                .await;
                            OperationResponse::Insert(InsertResponse { key, result })
                        }
                        Operation::Delete(delete) => {
                            let key = delete.key.clone();
                            let result = service
                                .delete_object(&ObjectId::new(context.clone(), delete.key))
                                .await;
                            OperationResponse::Delete(DeleteResponse { key, result })
                        }
                    },
                    Err(error) => OperationResponse::Error(ErrorResponse { error: error.into() }),
                };
                yield result;
            }
        }.boxed();

    let r = rand::random::<u128>();
    responses.into_multipart_response(r)
}

fn create_success_part(
    key: &ObjectKey,
    status: StatusCode,
    content_type: Option<&str>,
    body: Bytes,
    additional_headers: Option<HeaderMap>,
) -> Result<Part, BatchError> {
    let mut headers = HeaderMap::new();

    headers.insert(
        HEADER_BATCH_OPERATION_KEY,
        key.parse().map_err(|e: http::header::InvalidHeaderValue| {
            BatchError::ResponseSerialization {
                context: "parsing operation key header".to_string(),
                cause: Box::new(e),
            }
        })?,
    );

    let status_str = format!(
        "{} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    )
    .trim()
    .to_string();

    headers.insert(
        HEADER_BATCH_OPERATION_STATUS,
        status_str
            .parse()
            .map_err(
                |e: http::header::InvalidHeaderValue| BatchError::ResponseSerialization {
                    context: "parsing status code header".to_string(),
                    cause: Box::new(e),
                },
            )?,
    );

    if let Some(additional) = additional_headers {
        headers.extend(additional);
    }

    Part::new(body, headers, content_type).map_err(|e| BatchError::ResponseSerialization {
        context: "creating multipart Part".to_string(),
        cause: Box::new(e),
    })
}

fn create_error_part(key: Option<&ObjectKey>, error: &ApiError) -> Result<Part, BatchError> {
    let mut headers = HeaderMap::new();

    if let Some(key) = key {
        headers.insert(
            HEADER_BATCH_OPERATION_KEY,
            key.parse().map_err(|e: http::header::InvalidHeaderValue| {
                BatchError::ResponseSerialization {
                    context: "parsing operation key header".to_string(),
                    cause: Box::new(e),
                }
            })?,
        );
    }

    let status = error.status();
    let status_str = format!(
        "{} {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    )
    .trim()
    .to_string();

    headers.insert(
        HEADER_BATCH_OPERATION_STATUS,
        status_str
            .parse()
            .map_err(
                |e: http::header::InvalidHeaderValue| BatchError::ResponseSerialization {
                    context: "parsing status code header".to_string(),
                    cause: Box::new(e),
                },
            )?,
    );

    let error_body = serde_json::to_vec(&ApiErrorResponse::from_error(error)).map_err(|e| {
        BatchError::ResponseSerialization {
            context: "serializing error response to JSON".to_string(),
            cause: Box::new(e),
        }
    })?;

    Part::new(Bytes::from(error_body), headers, None).map_err(|e| {
        BatchError::ResponseSerialization {
            context: "creating multipart Part for error".to_string(),
            cause: Box::new(e),
        }
    })
}

impl TryFrom<OperationResponse> for Part {
    type Error = BatchError;

    fn try_from(value: OperationResponse) -> Result<Self, Self::Error> {
        match value {
            OperationResponse::Get(GetResponse { key, result }) => match result {
                Ok(Some((metadata, bytes))) => {
                    let metadata_headers = metadata.to_headers("", false)?;
                    create_success_part(
                        &key,
                        StatusCode::OK,
                        Some(&metadata.content_type),
                        bytes,
                        Some(metadata_headers),
                    )
                }
                Ok(None) => {
                    create_success_part(&key, StatusCode::NOT_FOUND, None, Bytes::new(), None)
                }
                Err(error) => create_error_part(Some(&key), &error),
            },
            OperationResponse::Insert(InsertResponse { key, result }) => match result {
                Ok(_) => create_success_part(&key, StatusCode::CREATED, None, Bytes::new(), None),
                Err(error) => create_error_part(Some(&key), &error),
            },
            OperationResponse::Delete(DeleteResponse { key, result }) => match result {
                Ok(_) => {
                    create_success_part(&key, StatusCode::NO_CONTENT, None, Bytes::new(), None)
                }
                Err(error) => create_error_part(Some(&key), &error),
            },
            OperationResponse::Error(ErrorResponse { error }) => create_error_part(None, &error),
        }
    }
}
