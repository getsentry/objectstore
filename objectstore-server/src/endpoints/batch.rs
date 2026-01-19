use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum_extra::response::multiple::MultipartForm;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use objectstore_service::id::{ObjectContext, ObjectId};

use crate::auth::AuthAwareService;
use crate::endpoints::common::ApiResult;
use crate::extractors::{BatchOperation, BatchRequest, Xt};
use crate::state::ServiceState;

const MAX_BODY_SIZE: usize = 1024 * 1024 * 1024; // 1 GB

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        // Enforced by https://github.com/tokio-rs/axum/blob/4404f27cea206b0dca63637b1c76dff23772a5cc/axum/src/extract/multipart.rs#L78
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
}

struct Part {}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    mut request: BatchRequest,
) -> ApiResult<MultipartForm> {
    let parts: BoxStream<anyhow::Result<Part>> = async_stream::try_stream! {
            while let Some(operation) = request.operations.next().await {
                let res = match operation {
                    Ok(operation) => match operation {
                        BatchOperation::Get(get) => {
                            let res = service
                                .get_object(&ObjectId::new(context.clone(), get.key))
                                .await;
                            Part {}
                        }
                        BatchOperation::Insert(insert) => {
                            let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                            let res = service
                                .insert_object(context.clone(), insert.key, &insert.metadata, stream)
                                .await;
                            Part {}
                        }
                        BatchOperation::Delete(delete) => {
                            let res = service
                                .delete_object(&ObjectId::new(context.clone(), delete.key))
                                .await;
                            Part {}
                        }
                    },
                    Err(_) => todo!()
                };
                yield res;
            }
        }.boxed();

    Ok(MultipartForm::from_iter(vec![].into_iter()))
}
