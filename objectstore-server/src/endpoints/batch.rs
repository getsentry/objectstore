use async_trait::async_trait;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing;
use axum_extra::response::multiple::MultipartForm;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use http::HeaderMap;
use http::header::CONTENT_TYPE;
use objectstore_service::PayloadStream;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::Metadata;

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiErrorResponse, ApiResult};
use crate::extractors::{BatchOperation, BatchRequest, HEADER_BATCH_OPERATION_KEY, Xt};
use crate::multipart::{IntoPart, Part};
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB

/// Header name for the HTTP status code in batch response parts.
const HEADER_BATCH_STATUS: &str = "x-sn-batch-status";

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    mut request: BatchRequest,
) -> ApiResult<MultipartForm> {
    let parts: BoxStream<anyhow::Result<Part>> = async_stream::try_stream! {
            while let Some(operation) = request.operations.next().await {
                let part = match operation {
                    Ok(operation) => match operation {
                        BatchOperation::Get(get) => {
                            let key = get.key.clone();
                            let result = service
                                .get_object(&ObjectId::new(context.clone(), get.key))
                                .await;
                            let mut part = result.into_part().await;
                            // Add the requested key to the response
                            part.add_header(HEADER_BATCH_OPERATION_KEY, key.parse().unwrap());
                            part
                        }
                        BatchOperation::Insert(insert) => {
                            let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                            let result = service
                                .insert_object(context.clone(), insert.key, &insert.metadata, stream)
                                .await;
                            // Insert already includes the key in into_batch_part
                            result.into_part().await
                        }
                        BatchOperation::Delete(delete) => {
                            let key = delete.key.clone();
                            let result = service
                                .delete_object(&ObjectId::new(context.clone(), delete.key))
                                .await;
                            let mut part = result.into_part().await;
                            // Add the deleted key to the response
                            part.add_header(HEADER_BATCH_OPERATION_KEY, key.parse().unwrap());
                            part
                        }
                    },
                    Err(_e) => {
                        // TODO: Handle batch parsing errors
                        todo!()
                    }
                };
                yield part;
            }
        }.boxed();

    Ok(MultipartForm::from_iter(vec![].into_iter()))
}

/// Implementation for API errors.
#[async_trait]
impl IntoPart for ApiError {
    async fn into_part(self) -> Part {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_BATCH_STATUS, self.status().as_u16().into());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let body = serde_json::to_vec(&ApiErrorResponse::from_error(&self))
            .unwrap()
            .into();
        Part::new(headers, body)
    }
}

#[async_trait]
impl IntoPart for ApiResult<ObjectId> {
    async fn into_part(self) -> Part {
        match self {
            Ok(id) => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER_BATCH_STATUS, StatusCode::CREATED.as_u16().into());
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
            Err(err) => err.into_part().await,
        }
    }
}

#[async_trait]
impl IntoPart for ApiResult<Option<(Metadata, PayloadStream)>> {
    async fn into_part(self) -> Part {
        match self {
            Ok(Some((metadata, stream))) => {
                // Buffer the stream since Part requires Bytes
                let body = match stream
                    .try_collect::<Vec<Bytes>>()
                    .await
                    .map(|chunks| chunks.concat())
                {
                    Ok(data) => Bytes::from(data),
                    Err(err) => {
                        // Stream read error - convert to error part
                        return ApiError::Service(err.into()).into_part().await;
                    }
                };

                let mut headers = match metadata.to_headers("", false) {
                    Ok(h) => h,
                    Err(err) => {
                        return ApiError::Service(err.into()).into_part().await;
                    }
                };
                headers.insert(HEADER_BATCH_STATUS, StatusCode::OK.as_u16().into());
                Part::new(headers, body)
            }
            Ok(None) => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER_BATCH_STATUS, StatusCode::NOT_FOUND.as_u16().into());
                Part::headers_only(headers)
            }
            Err(err) => err.into_part().await,
        }
    }
}

#[async_trait]
impl IntoPart for ApiResult<()> {
    async fn into_part(self) -> Part {
        match self {
            Ok(()) => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER_BATCH_STATUS, StatusCode::NO_CONTENT.as_u16().into());
                Part::headers_only(headers)
            }
            Err(err) => err.into_part().await,
        }
    }
}
