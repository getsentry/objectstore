use std::os::unix::fs::OpenOptionsExt;

use axum::extract::{DefaultBodyLimit, Multipart};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing;
use axum::{Json, Router};
use axum_extra::response::multiple::{MultipartForm, Part};
use futures::TryStreamExt;
use futures::stream::{self, StreamExt, unfold};
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::{DeleteResult, GetResult, InsertResult};
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

    let inserts = unfold(multipart, |mut m| async move {
        match m.next_field().await {
            Ok(Some(field)) => {
                let metadata = Metadata::from_headers(field.headers(), "").unwrap();
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|e| anyhow::Error::new(e))
                    .unwrap();
                Some((Ok((metadata, bytes)), m))
            }
            Ok(None) => None,
            Err(_) => todo!(),
        }
    });
    let insert_results = service.insert_objects(context.clone(), inserts).await;

    for operation in manifest.operations.into_iter() {
        match operation {
            Operation::Get(key) => {
                let result = service
                    .get_object(&ObjectId::new(context.clone(), key))
                    .await;
            }
            Operation::Delete(key) => {
                let result = service
                    .delete_object(&ObjectId::new(context.clone(), key))
                    .await;
            }
            _ => (),
        };
    }
    Ok(StatusCode::OK.into_response())
}
