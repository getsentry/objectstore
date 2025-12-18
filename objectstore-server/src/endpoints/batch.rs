use std::fmt::Debug;

use axum::Router;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::routing;
use futures::StreamExt;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue};
use objectstore_service::id::{ObjectContext, ObjectKey};
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

#[derive(Serialize, Debug, PartialEq)]
#[serde(tag = "results")]
pub struct ResponseManifest {
    pub results: Vec<OperationResult>,
}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    request: BatchRequest,
) -> ApiResult<Response> {
    let (gets, inserts, deletes): (Vec<_>, Vec<_>, Vec<_>) =
        request.manifest.operations.into_iter().enumerate().fold(
            (vec![], vec![], vec![]),
            |mut acc, (i, op)| {
                match op {
                    Operation::Get { key } => acc.0.push((i, key)),
                    Operation::Insert { key } => acc.1.push((i, key)),
                    Operation::Delete { key } => acc.2.push((i, key)),
                }
                acc
            },
        );

    let get_keys: Vec<ObjectKey> = gets.iter().map(|(_, key)| key.clone()).collect();
    let get_results = service.get_objects(&context, &get_keys).await?;

    let insert_keys: Vec<Option<ObjectKey>> = inserts.iter().map(|(_, key)| key.clone()).collect();
    let insert_results = service
        .insert_objects(&context, &insert_keys, request.inserts)
        .await?;

    let delete_keys: Vec<ObjectKey> = deletes.iter().map(|(_, key)| key.clone()).collect();
    let delete_results = service.delete_objects(&context, &delete_keys).await?;

    let mut results = vec![None; request.manifest.operations.len()];
    let mut streams = vec![];
    for ((i, _), res) in gets.into_iter().zip(get_results) {
        results[i] = None;
    }
    for ((i, _), res) in inserts.into_iter().zip(insert_results) {
        results[i] = None;
    }
    for ((i, _), res) in deletes.into_iter().zip(delete_results) {
        results[i] = None;
    }
    let results = results.into_iter().map(|r| r.unwrap()).collect();
    let manifest = ResponseManifest { results };

    let r = rand::random::<u128>();
    let boundary = format!("os-boundary-{r:032x}");

    let headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&format!("multipart/mixed; boundary={boundary}")).unwrap(),
    );

    let body_stream = async_stream::stream! {
        yield Ok(serde_json::to_vec(&manifest)?);
        for result in get_results {
            //yield result.unwrap();
            todo!();
        }
    };

    Ok((headers, Body::from_stream(body_stream)).into_response())
}
