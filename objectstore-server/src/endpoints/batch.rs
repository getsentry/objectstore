use std::fmt::Debug;
use std::pin::Pin;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::routing;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue};
use objectstore_service::id::{ObjectContext, ObjectId, ObjectKey};
use objectstore_service::{DeleteResult, GetResult, InsertResult};
use objectstore_types::Metadata;
use serde::Serialize;

use crate::auth::AuthAwareService;
use crate::error::{AnyhowResponse, ApiResult};
use crate::extractors::Operation;
use crate::extractors::{BatchRequest, Xt};
use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new().route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
}

pub trait IntoPart {
    fn into_part(&self) -> Bytes;
}

impl IntoPart for GetResult {
    fn into_part(&self) -> Bytes {
        todo!()
    }
}

impl IntoPart for InsertResult {
    fn into_part(&self) -> Bytes {
        todo!()
    }
}

impl IntoPart for DeleteResult {
    fn into_part(&self) -> Bytes {
        todo!()
    }
}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    mut request: BatchRequest,
) -> ApiResult<Response> {
    let r = rand::random::<u128>();
    let boundary = format!("os-boundary-{r:032x}");
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&format!("multipart/mixed; boundary={boundary}")).unwrap(),
    );

    let body_stream: Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send>> = async_stream::try_stream! {
            while let Some(operation) = request.operations.next().await {
                let res = match operation {
                    Ok(operation) => match operation {
                        Operation::Get(get) => {
                            let res = service
                                .get_object(&ObjectId::new(context.clone(), get.key))
                                .await;
                            res.into_part()
                        }
                        Operation::Insert(insert) => {
                            let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                            let res = service
                                .insert_object(context.clone(), insert.key, &insert.metadata, stream)
                                .await;
                            res.into_part()
                        }
                        Operation::Delete(delete) => {
                            let res = service
                                .delete_object(&ObjectId::new(context.clone(), delete.key))
                                .await;
                            res.into_part()
                        }
                    },
                    Err(_) => todo!(),
                };
                yield res;
            }
        }.boxed();

    Ok((headers, Body::from_stream(body_stream)).into_response())
}
