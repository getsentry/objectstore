use std::pin::Pin;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::routing;
use bytes::{BytesMut};
use futures::{Stream, StreamExt, TryStreamExt};
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue};
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::{DeleteResult, GetResult, InsertResult};

use crate::auth::AuthAwareService;
use crate::error::{ApiResult};
use crate::extractors::Operation;
use crate::extractors::{BatchRequest, Xt};
use crate::multipart::{IntoBytesStream, Part};
use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new().route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
}

const HEADER_BATCH_OPERATION_STATUS: &str = "x-sn-batch-operation-status";
const HEADER_BATCH_OPERATION_KEY: &str = "x-sn-batch-operation-key";

#[async_trait]
pub trait IntoPart {
    async fn into_part(mut self) -> Part;
}

#[async_trait]
impl IntoPart for GetResult {
    async fn into_part(mut self) -> Part {
        match self {
            Ok(Some((metadata, payload))) => {
                let payload = payload
                    .try_fold(BytesMut::new(), |mut acc, chunk| async move {
                        acc.extend_from_slice(&chunk);
                        Ok(acc)
                    })
                    .await
                    .unwrap()
                    .freeze();

                let mut headers = metadata.to_headers("", false).unwrap();
                headers.insert(HEADER_BATCH_OPERATION_STATUS, HeaderValue::from_static("200"));

                Part::new(headers, payload)
            },
            Ok(None) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("404"),
                );
                Part::headers_only(headers)
            },
            Err(_) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("500"),
                );
                Part::headers_only(headers)
            },
        }
    }
}

#[async_trait]
impl IntoPart for InsertResult {
    async fn into_part(mut self) -> Part {
        match self {
            Ok(id) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_KEY,
                    HeaderValue::from_str(id.key()).unwrap(),
                );
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("200"),
                );
                Part::headers_only(headers)
            },
            Err(_) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("500"),
                );
                Part::headers_only(headers)
            },
        }
    }
}

#[async_trait]
impl IntoPart for DeleteResult {
    async fn into_part(mut self) -> Part {
        match self {
            Ok(()) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("200"),
                );
                Part::headers_only(headers)
            },
            Err(_) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HEADER_BATCH_OPERATION_STATUS,
                    HeaderValue::from_static("500"),
                );
                Part::headers_only(headers)
            },
        }
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

    let parts: Pin<Box<dyn Stream<Item = anyhow::Result<Part>> + Send>> = async_stream::try_stream! {
            while let Some(operation) = request.operations.next().await {
                let res = match operation {
                    Ok(operation) => match operation {
                        Operation::Get(get) => {
                            let res = service
                                .get_object(&ObjectId::new(context.clone(), get.key))
                                .await;
                            res.into_part().await
                        }
                        Operation::Insert(insert) => {
                            let stream = futures_util::stream::once(async { Ok(insert.payload) }).boxed();
                            let res = service
                                .insert_object(context.clone(), insert.key, &insert.metadata, stream)
                                .await;
                            res.into_part().await
                        }
                        Operation::Delete(delete) => {
                            let res = service
                                .delete_object(&ObjectId::new(context.clone(), delete.key))
                                .await;
                            res.into_part().await
                        }
                    },
                    Err(_) => todo!()
                };
                yield res;
            }
        }.boxed();

    Ok((headers, Body::from_stream(parts.into_bytes_stream(boundary))).into_response())
}
