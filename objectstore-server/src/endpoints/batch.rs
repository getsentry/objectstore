use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::response::Response;
use axum::routing;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use http::HeaderMap;
use objectstore_service::id::{ObjectContext, ObjectId, ObjectKey};
use objectstore_service::{DeleteResponse, InsertResponse};
use objectstore_types::Metadata;

use crate::auth::AuthAwareService;
use crate::batch::HEADER_BATCH_OPERATION_KEY;
use crate::endpoints::common::{ApiError, ApiErrorResponse, ApiResult};
use crate::extractors::Xt;
use crate::extractors::batch::{BatchError, BatchRequest, BatchRequestStream};
use crate::multipart::{IntoMultipartResponse, Part};
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

struct BatchGetResponse {
    pub key: ObjectKey,
    pub result: ApiResult<Option<(Metadata, Bytes)>>,
}

impl std::fmt::Debug for BatchGetResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchGetResponse")
            .field("key", &self.key)
            .finish()
    }
}

#[derive(Debug)]
pub struct BatchInsertResponse {
    pub key: ObjectKey,
    pub result: ApiResult<InsertResponse>,
}

#[derive(Debug)]
pub struct BatchDeleteResponse {
    pub key: ObjectKey,
    pub result: ApiResult<DeleteResponse>,
}

#[derive(Debug)]
pub struct BatchErrorResponse {
    pub error: ApiError,
}

enum BatchResponse {
    Get(BatchGetResponse),
    Insert(BatchInsertResponse),
    Delete(BatchDeleteResponse),
    Error(BatchErrorResponse),
}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    mut requests: BatchRequestStream,
) -> Response {
    let responses: BoxStream<BatchResponse> = async_stream::stream! {
            while let Some(operation) = requests.0.next().await {
                let result = match operation {
                    Ok(operation) => match operation {
                        BatchRequest::Get(get) => {
                            let key = get.key.clone();
                            let result = service
                                .get_object(&ObjectId::new(context.clone(), get.key))
                                .await;

                            let result = match result {
                                Ok(Some((metadata, stream))) => {
                                    match stream.try_collect::<BytesMut>().await {
                                        Ok(bytes) => Ok(Some((metadata, bytes.freeze()))),
                                        Err(e) => Err(ApiError::Service(e.into())),
                                    }
                                }
                                Ok(None) => Ok(None),
                                Err(e) => Err(e),
                            };

                            BatchResponse::Get(BatchGetResponse{ key, result })
                        }
                        BatchRequest::Insert(insert) => {
                            let key = insert.key.clone();
                            let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                            let result = service
                                .insert_object(context.clone(), Some(insert.key), &insert.metadata, stream)
                                .await;
                            BatchResponse::Insert(BatchInsertResponse { key, result })
                        }
                        BatchRequest::Delete(delete) => {
                            let key = delete.key.clone();
                            let result = service
                                .delete_object(&ObjectId::new(context.clone(), delete.key))
                                .await;
                            BatchResponse::Delete(BatchDeleteResponse { key, result })
                        }
                    },
                    Err(error) => BatchResponse::Error(BatchErrorResponse { error: error.into() }),
                };
                yield result;
            }
        }.boxed();

    let r = rand::random::<u128>();
    responses.into_multipart_response(r)
}

fn create_success_part(
    key: &ObjectKey,
    status: u16,
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

    headers.insert(
        HEADER_BATCH_OPERATION_STATUS,
        status
            .to_string()
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

    Part::new("part", content_type, body, None, headers).map_err(|e| {
        BatchError::ResponseSerialization {
            context: "creating multipart Part".to_string(),
            cause: Box::new(e),
        }
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

    headers.insert(
        HEADER_BATCH_OPERATION_STATUS,
        error.status().as_u16().to_string().parse().map_err(
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

    Part::new(
        "part",
        Some("application/json"),
        Bytes::from(error_body),
        None,
        headers,
    )
    .map_err(|e| BatchError::ResponseSerialization {
        context: "creating multipart Part for error".to_string(),
        cause: Box::new(e),
    })
}

impl TryFrom<BatchResponse> for Part {
    type Error = BatchError;

    fn try_from(value: BatchResponse) -> Result<Self, Self::Error> {
        match value {
            BatchResponse::Get(BatchGetResponse { key, result }) => match result {
                Ok(Some((metadata, bytes))) => {
                    let metadata_headers = metadata.to_headers("", false)?;
                    create_success_part(
                        &key,
                        200,
                        Some(&metadata.content_type),
                        bytes,
                        Some(metadata_headers),
                    )
                }
                Ok(None) => create_success_part(&key, 404, None, Bytes::new(), None),
                Err(error) => create_error_part(Some(&key), &error),
            },
            BatchResponse::Insert(BatchInsertResponse { key, result }) => match result {
                Ok(_) => create_success_part(&key, 201, None, Bytes::new(), None),
                Err(error) => create_error_part(Some(&key), &error),
            },
            BatchResponse::Delete(BatchDeleteResponse { key, result }) => match result {
                Ok(_) => create_success_part(&key, 204, None, Bytes::new(), None),
                Err(error) => create_error_part(Some(&key), &error),
            },
            BatchResponse::Error(BatchErrorResponse { error }) => create_error_part(None, &error),
        }
    }
}
