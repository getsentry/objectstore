use std::time::SystemTime;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use objectstore_service::ServiceError;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::Metadata;
use serde::Serialize;

use crate::auth::AuthAwareService;
use crate::endpoints::common::ApiResult;
use crate::extractors::{Xt, body::MeteredBody};
use crate::state::ServiceState;

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
    Xt(context): Xt<ObjectContext>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let mut metadata = Metadata::from_headers(&headers, "").map_err(ServiceError::from)?;
    metadata.time_created = Some(SystemTime::now());

    let response_id = service
        .insert_object(context, None, &metadata, body)
        .await?;
    let response = Json(InsertObjectResponse {
        key: response_id.key().to_string(),
    });

    Ok((StatusCode::CREATED, response).into_response())
}

async fn object_get(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
) -> ApiResult<Response> {
    let Some((metadata, stream)) = service.get_object(&id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let stream = state.meter_egress_stream(stream);

    let headers = metadata.to_headers("", false).map_err(ServiceError::from)?;
    Ok((headers, Body::from_stream(stream)).into_response())
}

async fn object_head(service: AuthAwareService, Xt(id): Xt<ObjectId>) -> ApiResult<Response> {
    let Some((metadata, _stream)) = service.get_object(&id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata.to_headers("", false).map_err(ServiceError::from)?;

    Ok((StatusCode::NO_CONTENT, headers).into_response())
}

async fn object_put(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let mut metadata = Metadata::from_headers(&headers, "").map_err(ServiceError::from)?;
    metadata.time_created = Some(SystemTime::now());

    let ObjectId { context, key } = id;
    let response_id = service
        .insert_object(context, Some(key), &metadata, body)
        .await?;

    let response = Json(InsertObjectResponse {
        key: response_id.key.to_string(),
    });

    Ok((StatusCode::OK, response).into_response())
}

async fn object_delete(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
) -> ApiResult<impl IntoResponse> {
    service.delete_object(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}
