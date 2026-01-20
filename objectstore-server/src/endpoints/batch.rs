use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::response::Response;
use axum::routing;
use futures::StreamExt;
use futures::stream::BoxStream;
use objectstore_service::id::{ObjectContext, ObjectId, ObjectKey};
use objectstore_service::{DeleteResponse, GetResponse, InsertResponse};

use crate::auth::AuthAwareService;
use crate::endpoints::common::ApiResult;
use crate::extractors::Xt;
use crate::extractors::batch::{BatchError, BatchRequest, BatchRequestStream};
use crate::multipart::{IntoMultipartResponse, Part};
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

struct BatchGetResponse {
    pub key: ObjectKey,
    pub result: ApiResult<GetResponse>,
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
    pub error: BatchError,
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
                    Err(error) => BatchResponse::Error(BatchErrorResponse { error }),
                };
                yield result;
            }
        }.boxed();

    let r = rand::random::<u128>();
    responses.into_multipart_response(r)
}

impl From<BatchResponse> for Part {
    fn from(_value: BatchResponse) -> Self {
        todo!()
    }
}
