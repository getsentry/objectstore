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
use crate::endpoints::common::{ApiError, ApiErrorResponse, ApiResult};
use crate::extractors::Xt;
use crate::extractors::batch::{BatchRequest, BatchRequestStream, HEADER_BATCH_OPERATION_KEY};
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

impl From<BatchResponse> for Part {
    fn from(value: BatchResponse) -> Self {
        match value {
            BatchResponse::Get(BatchGetResponse { key, result }) => {
                let mut headers = HeaderMap::new();

                match result {
                    Ok(Some((metadata, bytes))) => {
                        headers.insert(HEADER_BATCH_OPERATION_KEY, key.parse().expect("valid header value"));
                        headers.insert(HEADER_BATCH_OPERATION_STATUS, "200".parse().expect("valid header value"));

                        let metadata_headers = metadata.to_headers("", false).expect("valid metadata headers");
                        headers.extend(metadata_headers);

                        Part::new("operation", &metadata.content_type, bytes, None, headers).expect("valid part")
                    }
                    Ok(None) => {
                        headers.insert(HEADER_BATCH_OPERATION_KEY, key.parse().expect("valid header value"));
                        headers.insert(HEADER_BATCH_OPERATION_STATUS, "404".parse().expect("valid header value"));

                        Part::new("operation", "application/octet-stream", Bytes::new(), None, headers).expect("valid part")
                    }
                    Err(error) => {
                        headers.insert(HEADER_BATCH_OPERATION_KEY, key.parse().expect("valid header value"));
                        headers.insert(HEADER_BATCH_OPERATION_STATUS, error.status().as_u16().to_string().parse().expect("valid header value"));

                        let error_body = serde_json::to_vec(&ApiErrorResponse::from_error(&error))
                            .expect("serializable error");

                        Part::new("operation", "application/json", Bytes::from(error_body), None, headers)
                            .expect("valid part")
                    }
                }
            }
            BatchResponse::Insert(BatchInsertResponse { key, result }) => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER_BATCH_OPERATION_KEY, key.parse().expect("valid header value"));

                match result {
                    Ok(_) => {
                        headers.insert(HEADER_BATCH_OPERATION_STATUS, "201".parse().expect("valid header value"));
                        Part::new("operation", "application/octet-stream", Bytes::new(), None, headers)
                            .expect("valid part")
                    }
                    Err(error) => {
                        headers.insert(HEADER_BATCH_OPERATION_STATUS, error.status().as_u16().to_string().parse().expect("valid header value"));

                        let error_body = serde_json::to_vec(&ApiErrorResponse::from_error(&error))
                            .expect("serializable error");

                        Part::new("operation", "application/json", Bytes::from(error_body), None, headers)
                            .expect("valid part")
                    }
                }
            }
            BatchResponse::Delete(BatchDeleteResponse { key, result }) => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER_BATCH_OPERATION_KEY, key.parse().expect("valid header value"));

                match result {
                    Ok(_) => {
                        headers.insert(HEADER_BATCH_OPERATION_STATUS, "204".parse().expect("valid header value"));
                        Part::new("operation", "application/octet-stream", Bytes::new(), None, headers)
                            .expect("valid part")
                    }
                    Err(error) => {
                        headers.insert(HEADER_BATCH_OPERATION_STATUS, error.status().as_u16().to_string().parse().expect("valid header value"));

                        let error_body = serde_json::to_vec(&ApiErrorResponse::from_error(&error))
                            .expect("serializable error");

                        Part::new("operation", "application/json", Bytes::from(error_body), None, headers)
                            .expect("valid part")
                    }
                }
            }
            BatchResponse::Error(BatchErrorResponse { error }) => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER_BATCH_OPERATION_STATUS, error.status().as_u16().to_string().parse().expect("valid header value"));
                let body = serde_json::to_vec(&ApiErrorResponse::from_error(&error)).expect("serializable error");
                Part::new("operation", "application/json", Bytes::from(body), None, headers).expect("valid part")
            }
        }
    }
}
