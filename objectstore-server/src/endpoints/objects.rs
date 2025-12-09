use std::borrow::Cow;
use std::io;
use std::time::SystemTime;

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::id::{ObjectContext, ObjectId, Scope, Scopes};
use objectstore_types::Metadata;
use serde::{Deserialize, Serialize, de};

use crate::auth::AuthAwareService;
use crate::endpoints::helpers;
use crate::error::ApiResult;
use crate::state::ServiceState;

/// Used in place of scopes in the URL to represent an empty set of scopes.
const EMPTY_SCOPES: &str = "_";

pub fn router() -> Router<ServiceState> {
    let collection_routes = routing::post(objects_post);
    let object_routes = routing::get(object_get)
        .head(object_head)
        .put(object_put)
        // TODO(ja): Implement PATCH (metadata update w/o body)
        .delete(object_delete);

    Router::new()
        .route("/objects/{usecase}/{scopes}", collection_routes.clone())
        .route("/objects/{usecase}/{scopes}/", collection_routes)
        .route("/objects/{usecase}/{scopes}/{*key}", object_routes)
}

/// Response returned when inserting an object.
#[derive(Debug, Serialize)]
pub struct InsertObjectResponse {
    pub key: String,
}

async fn objects_post(
    service: AuthAwareService,
    Path(params): Path<CollectionParams>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Response> {
    let context = params.into_context();
    helpers::populate_sentry_context(&context);

    let mut metadata =
        Metadata::from_headers(&headers, "").context("extracting metadata from headers")?;
    metadata.time_created = Some(SystemTime::now());

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let response_id = service
        .insert_object(context, None, &metadata, stream)
        .await?;
    let response = Json(InsertObjectResponse {
        key: response_id.key().to_string(),
    });

    Ok((StatusCode::CREATED, response).into_response())
}

async fn object_get(
    service: AuthAwareService,
    Path(params): Path<ObjectParams>,
) -> ApiResult<Response> {
    let id = params.into_object_id();
    helpers::populate_sentry_object_id(&id);

    let Some((metadata, stream)) = service.get_object(&id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata
        .to_headers("", false)
        .context("extracting metadata from headers")?;
    Ok((headers, Body::from_stream(stream)).into_response())
}

async fn object_head(
    service: AuthAwareService,
    Path(params): Path<ObjectParams>,
) -> ApiResult<Response> {
    let id = params.into_object_id();
    helpers::populate_sentry_object_id(&id);

    let Some((metadata, _stream)) = service.get_object(&id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata
        .to_headers("", false)
        .context("extracting metadata from headers")?;

    Ok((StatusCode::NO_CONTENT, headers).into_response())
}

async fn object_put(
    service: AuthAwareService,
    Path(params): Path<ObjectParams>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Response> {
    let id = params.into_object_id();
    helpers::populate_sentry_object_id(&id);

    let mut metadata =
        Metadata::from_headers(&headers, "").context("extracting metadata from headers")?;
    metadata.time_created = Some(SystemTime::now());

    let ObjectId { context, key } = id;
    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let response_id = service
        .insert_object(context, Some(key), &metadata, stream)
        .await?;

    let response = Json(InsertObjectResponse {
        key: response_id.key.to_string(),
    });

    Ok((StatusCode::OK, response).into_response())
}

async fn object_delete(
    service: AuthAwareService,
    Path(params): Path<ObjectParams>,
) -> ApiResult<impl IntoResponse> {
    let id = params.into_object_id();
    helpers::populate_sentry_object_id(&id);

    service.delete_object(&id).await?;

    Ok(StatusCode::NO_CONTENT)
}

/// Path parameters used for collection-level endpoints without a key.
///
/// This is meant to be used with the axum `Path` extractor.
#[derive(Clone, Debug, Deserialize)]
struct CollectionParams {
    usecase: String,
    #[serde(deserialize_with = "deserialize_scopes")]
    scopes: Scopes,
}

impl CollectionParams {
    /// Converts the params into an [`ObjectContext`].
    pub fn into_context(self) -> ObjectContext {
        ObjectContext {
            usecase: self.usecase,
            scopes: self.scopes,
        }
    }
}

/// Path parameters used for object-level endpoints.
///
/// This is meant to be used with the axum `Path` extractor.
#[derive(Clone, Debug, Deserialize)]
struct ObjectParams {
    usecase: String,
    #[serde(deserialize_with = "deserialize_scopes")]
    scopes: Scopes,
    key: String,
}

impl ObjectParams {
    /// Converts the params into an [`ObjectId`].
    pub fn into_object_id(self) -> ObjectId {
        ObjectId::from_parts(self.usecase, self.scopes, self.key)
    }
}

/// Deserializes a `Scopes` instance from a string representation.
///
/// The string representation is a semicolon-separated list of `key=value` pairs, following the
/// Matrix URIs proposal. An empty scopes string (`"_"`) represents no scopes.
fn deserialize_scopes<'de, D>(deserializer: D) -> Result<Scopes, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = Cow::<str>::deserialize(deserializer)?;
    if s == EMPTY_SCOPES {
        return Ok(Scopes::empty());
    }

    let scopes = s
        .split(';')
        .map(|s| {
            let (key, value) = s
                .split_once("=")
                .ok_or_else(|| de::Error::custom("scope must be 'key=value'"))?;

            Scope::create(key, value).map_err(de::Error::custom)
        })
        .collect::<Result<_, _>>()?;

    Ok(scopes)
}
