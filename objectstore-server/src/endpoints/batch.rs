use std::os::unix::fs::OpenOptionsExt;

use axum::extract::{DefaultBodyLimit, Multipart};
use axum::http::StatusCode;
use axum::response::{IntoResponse, IntoResponseParts};
use axum::routing;
use axum::{Json, Router};
use axum_extra::response::multiple::MultipartForm;
use futures::TryStreamExt;
use futures_util::StreamExt;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::{DeleteResult, GetResult};
use objectstore_types::Metadata;
use serde::{Deserialize, Serialize};

use crate::auth::AuthAwareService;
use crate::error::ApiResult;
use crate::extractors::Xt;
use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    Router::new()
        .route("/objects:batch/{usecase}/{scopes}/", routing::post(batch))
        .layer(DefaultBodyLimit::max(500 * 1_000_000))
}

#[derive(Deserialize, Debug)]
#[serde(tag = "op")]
enum Operation {
    Get(String),
    Insert(Option<String>),
    Delete(String),
}

#[derive(Deserialize, Debug)]
struct RequestManifest {
    operations: Vec<Operation>,
}

#[derive(Serialize, Debug)]
struct Response {}

pub trait IntoPart {
    fn into_part(self) -> axum_extra::response::multiple::Part;
}

impl IntoPart for GetResult {
    fn into_part(self) -> axum_extra::response::multiple::Part {
        todo!()
    }
}

#[derive(Deserialize, Debug)]
struct ResultManifest {}

impl IntoPart for ResultManifest {
    fn into_part(self) -> axum_extra::response::multiple::Part {
        todo!()
    }
}

async fn batch(
    service: AuthAwareService,
    Xt(context): Xt<ObjectContext>,
    mut multipart: Multipart,
) -> ApiResult<axum::response::Response> {
    let Some(manifest) = multipart.next_field().await? else {
        return Ok((StatusCode::BAD_REQUEST, "expected manifest").into_response());
    };

    // TODO: enforce max size on the manifest
    let manifest = manifest.bytes().await?;
    let manifest = str::from_utf8(&manifest)
        .map_err(|err| anyhow::Error::new(err).context("failed to parse manifest as UTF-8"))?;
    let manifest: RequestManifest = serde_json::from_str(manifest)
        .map_err(|err| anyhow::Error::new(err).context("failed to deserialize manifest"))?;

    let result_manifest = ResultManifest {};
    let mut parts = vec![];
    for operation in manifest.operations {
        let result = match operation {
            Operation::Get(key) => {
                let result = service
                    .get_object(&ObjectId::new(context.clone(), key))
                    .await;
                parts.push(result.into_part());
            }
            Operation::Insert(key) => {
                service.insert_object(
                    context.clone(),
                    key,
                    &Metadata::default(),
                    multipart
                        .next_field()
                        .await
                        .unwrap()
                        .unwrap()
                        .map_err(|err| std::io::Error::other(err))
                        .boxed(),
                );
            }
            Operation::Delete(key) => {
                service
                    .delete_object(&ObjectId::new(context.clone(), key))
                    .await;
            }
        };
    }
    parts.insert(0, result_manifest.into_part());

    Ok(MultipartForm::with_parts(parts).into_response())
}
