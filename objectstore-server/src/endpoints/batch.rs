use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use http::HeaderMap;
use http::header::CONTENT_TYPE;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::Metadata;

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiErrorResponse, ApiResult};
use crate::extractors::{BatchError, BatchOperation, BatchRequest, HEADER_BATCH_OPERATION_KEY, Xt};
use crate::multipart::{IntoMultipartResponse, IntoPart, Part};
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB

/// Header name for the HTTP status code in batch response parts.
const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-status";

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

#[derive(Debug)]
enum BatchResult {
    Get {
        key: String,
        result: ApiResult<Option<(Metadata, Bytes)>>,
    },
    Insert(ApiResult<ObjectId>),
    Delete {
        key: String,
        result: ApiResult<()>,
    },
    Error(BatchError),
}

impl IntoPart for BatchError {
    fn into_part(self) -> Part {
        let mut headers = HeaderMap::new();
        headers.insert(
            HEADER_BATCH_OPERATION_STATUS,
            StatusCode::BAD_REQUEST.as_u16().into(),
        );
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let body = serde_json::json!({
            "detail": self.to_string(),
        });
        let body = serde_json::to_vec(&body).unwrap().into();
        Part::new(headers, body)
    }
}

impl IntoPart for BatchResult {
    fn into_part(self) -> Part {
        match self {
            BatchResult::Get { key, result } => {
                let mut part = result.into_part();
                part.add_header(HEADER_BATCH_OPERATION_KEY, key.parse().unwrap());
                part
            }
            BatchResult::Insert(result) => result.into_part(),
            BatchResult::Delete { key, result } => {
                let mut part = result.into_part();
                part.add_header(HEADER_BATCH_OPERATION_KEY, key.parse().unwrap());
                part
            }
            BatchResult::Error(err) => err.into_part(),
        }
    }
}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    mut request: BatchRequest,
) -> Response {
    let results: BoxStream<BatchResult> = async_stream::stream! {
            while let Some(operation) = request.operations.next().await {
                let result = match operation {
                    Ok(operation) => match operation {
                        BatchOperation::Get(get) => {
                            let key = get.key.clone();
                            let result = service
                                .get_object(&ObjectId::new(context.clone(), get.key))
                                .await;

                            // Buffer the stream before wrapping in BatchResult
                            let buffered_result = match result {
                                Ok(Some((metadata, stream))) => {
                                    match stream.try_collect::<Vec<Bytes>>().await.map(|chunks| chunks.concat()) {
                                        Ok(data) => Ok(Some((metadata, Bytes::from(data)))),
                                        Err(err) => Err(ApiError::Service(err.into())),
                                    }
                                }
                                Ok(None) => Ok(None),
                                Err(err) => Err(err),
                            };

                            BatchResult::Get { key, result: buffered_result }
                        }
                        BatchOperation::Insert(insert) => {
                            let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                            let result = service
                                .insert_object(context.clone(), insert.key, &insert.metadata, stream)
                                .await;
                            BatchResult::Insert(result)
                        }
                        BatchOperation::Delete(delete) => {
                            let key = delete.key.clone();
                            let result = service
                                .delete_object(&ObjectId::new(context.clone(), delete.key))
                                .await;
                            BatchResult::Delete { key, result }
                        }
                    },
                    Err(e) => BatchResult::Error(e)
                };
                yield result;
            }
        }.boxed();

    let r = rand::random::<u128>();
    let boundary = format!("os-boundary-{r:032x}");
    results.into_response(boundary)
}

/// Implementation for API errors.
impl IntoPart for ApiError {
    fn into_part(self) -> Part {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_BATCH_OPERATION_STATUS, self.status().as_u16().into());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let body = serde_json::to_vec(&ApiErrorResponse::from_error(&self))
            .unwrap()
            .into();
        Part::new(headers, body)
    }
}

impl IntoPart for ApiResult<ObjectId> {
    fn into_part(self) -> Part {
        match self {
            Ok(id) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    StatusCode::CREATED.as_u16().into(),
                );
                headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
                // Add the key header for the created object
                headers.insert(
                    HEADER_BATCH_OPERATION_KEY,
                    id.key().to_string().parse().unwrap(),
                );
                let body = serde_json::json!({ "key": id.key().to_string() });
                let body = serde_json::to_vec(&body).unwrap().into();
                Part::new(headers, body)
            }
            Err(err) => err.into_part(),
        }
    }
}

impl IntoPart for ApiResult<Option<(Metadata, Bytes)>> {
    fn into_part(self) -> Part {
        match self {
            Ok(Some((metadata, body))) => {
                let mut headers = match metadata.to_headers("", false) {
                    Ok(h) => h,
                    Err(err) => {
                        return ApiError::Service(err.into()).into_part();
                    }
                };
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    StatusCode::OK.as_u16().into(),
                );
                Part::new(headers, body)
            }
            Ok(None) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    StatusCode::NOT_FOUND.as_u16().into(),
                );
                Part::headers_only(headers)
            }
            Err(err) => err.into_part(),
        }
    }
}

impl IntoPart for ApiResult<()> {
    fn into_part(self) -> Part {
        match self {
            Ok(()) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    StatusCode::NO_CONTENT.as_u16().into(),
                );
                Part::headers_only(headers)
            }
            Err(err) => err.into_part(),
        }
    }
}
