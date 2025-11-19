//! Contains all HTTP endpoint handlers.

use std::io;
use std::time::SystemTime;

use anyhow::Context;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::{ObjectPath, OptionalObjectPath};
use objectstore_types::Metadata;
use serde::Serialize;

use crate::error::ApiResult;
use crate::state::ServiceState;

pub fn routes() -> Router<ServiceState> {
    let service_routes = Router::new().route(
        "/{*path}",
        routing::post(insert_object)
            .put(insert_object)
            .get(get_object)
            .delete(delete_object),
    );

    Router::new()
        .route("/health", routing::get(health))
        .nest("/v1/", service_routes)
}

async fn health() -> impl IntoResponse {
    "OK"
}

#[derive(Debug, Serialize)]
struct InsertResponse {
    key: String,
}

async fn insert_object(
    State(state): State<ServiceState>,
    Path(path): Path<OptionalObjectPath>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Response> {
    let (expected_method, response_status) = match path.key {
        Some(_) => (Method::PUT, StatusCode::OK),
        None => (Method::POST, StatusCode::CREATED),
    };

    // TODO: For now allow PUT everywhere. Remove the second condition when all clients are updated.
    if method != expected_method && method == Method::POST {
        return Ok(StatusCode::METHOD_NOT_ALLOWED.into_response());
    }

    let path = path.create_key();
    populate_sentry_scope(&path);

    let mut metadata =
        Metadata::from_headers(&headers, "").context("extracting metadata from headers")?;
    metadata.time_created = Some(SystemTime::now());

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let response_path = state.service.put_object(path, &metadata, stream).await?;
    let response = Json(InsertResponse {
        key: response_path.key.to_string(),
    });

    Ok((response_status, response).into_response())
}

async fn get_object(
    State(state): State<ServiceState>,
    Path(path): Path<ObjectPath>,
) -> ApiResult<Response> {
    populate_sentry_scope(&path);
    let Some((metadata, stream)) = state.service.get_object(&path).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata
        .to_headers("", false)
        .context("extracting metadata from headers")?;
    Ok((headers, Body::from_stream(stream)).into_response())
}

async fn delete_object(
    State(state): State<ServiceState>,
    Path(path): Path<ObjectPath>,
) -> ApiResult<impl IntoResponse> {
    populate_sentry_scope(&path);

    state.service.delete_object(&path).await?;

    Ok(StatusCode::NO_CONTENT)
}

fn populate_sentry_scope(path: &ObjectPath) {
    sentry::configure_scope(|s| {
        s.set_tag("usecase", path.usecase.clone());
        s.set_extra("scope", path.scope.clone().into());
        s.set_extra("key", path.key.clone().into());
    });
}
