use std::fmt::Debug;

use axum::Router;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::routing;
use futures::StreamExt;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue};
use objectstore_service::id::{ObjectContext, ObjectId, ObjectKey};
use objectstore_types::Metadata;
use serde::Serialize;

use crate::auth::AuthAwareService;
use crate::error::ApiResult;
use crate::extractors::Operation;
use crate::extractors::{BatchRequest, Xt};
use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new().route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
}

#[derive(Serialize, Debug, PartialEq, Clone)]
pub enum OperationResult {
    Get {
        status: u8,
        metadata: Option<Metadata>,
    },
    Insert {
        status: u8,
    },
    Delete {
        status: u8,
    },
}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    request: BatchRequest,
) -> ApiResult<Response> {
    let r = rand::random::<u128>();
    let boundary = format!("os-boundary-{r:032x}");

    let headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&format!("multipart/mixed; boundary={boundary}")).unwrap(),
    );

    while let Some(operation) = request.operations.next().await {
        match operation {
            Ok(operation) => match operation {
                Operation::Get(get) => {
                    let res = service
                        .get_object(&ObjectId::new(context.clone(), get.key))
                        .await;
                    res.into_part()
                }
                Operation::Insert(insert) => {
                    let res = service
                        .insert_object(context.clone(), insert.key, &insert.metadata, insert.stream)
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
            Err(err) => todo!(),
        }
    }
    let body_stream = async_stream::stream! {};

    Ok((headers, Body::from_stream(body_stream)).into_response())
}
