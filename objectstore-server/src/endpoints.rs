//! Contains all HTTP endpoint handlers.

use std::io;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_service::ObjectPath;
use objectstore_types::Metadata;
use serde::{Deserialize, Serialize};

use crate::error::ApiResult;
use crate::state::ServiceState;

pub fn routes() -> Router<ServiceState> {
    let service_routes = Router::new().route("/", put(put_object_nokey)).route(
        "/{*key}",
        put(put_object).get(get_object).delete(delete_object),
    );

    Router::new()
        .route("/health", get(health))
        .nest("/v1/", service_routes)
}

async fn health() -> impl IntoResponse {
    "OK"
}

#[derive(Deserialize, Debug)]
struct ContextParams {
    scope: String,
    usecase: String,
}

#[derive(Debug, Serialize)]
struct PutBlobResponse {
    key: String,
}

async fn put_object_nokey(
    State(state): State<ServiceState>,
    Query(params): Query<ContextParams>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<impl IntoResponse> {
    let path = ObjectPath {
        usecase: params.usecase,
        scope: params.scope,
        key: uuid::Uuid::new_v4().to_string(),
    };
    populate_sentry_scope(&path);
    let metadata = Metadata::from_headers(&headers, "")?;

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let key = state.service.put_object(path, &metadata, stream).await?;

    Ok(Json(PutBlobResponse {
        key: key.key.to_string(),
    }))
}

async fn put_object(
    State(state): State<ServiceState>,
    Query(params): Query<ContextParams>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<impl IntoResponse> {
    let path = ObjectPath {
        usecase: params.usecase,
        scope: params.scope,
        key,
    };
    populate_sentry_scope(&path);
    let metadata = Metadata::from_headers(&headers, "")?;

    let stream = body.into_data_stream().map_err(io::Error::other).boxed();
    let key = state.service.put_object(path, &metadata, stream).await?;

    Ok(Json(PutBlobResponse {
        key: key.key.to_string(),
    }))
}

async fn get_object(
    State(state): State<ServiceState>,
    Query(params): Query<ContextParams>,
    Path(key): Path<String>,
) -> ApiResult<Response> {
    let path = ObjectPath {
        usecase: params.usecase,
        scope: params.scope,
        key,
    };
    populate_sentry_scope(&path);

    let Some((metadata, stream)) = state.service.get_object(&path).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata.to_headers("", false)?;
    Ok((headers, Body::from_stream(stream)).into_response())
}

async fn delete_object(
    State(state): State<ServiceState>,
    Query(params): Query<ContextParams>,
    Path(key): Path<String>,
) -> ApiResult<impl IntoResponse> {
    let path = ObjectPath {
        usecase: params.usecase,
        scope: params.scope,
        key,
    };
    populate_sentry_scope(&path);

    state.service.delete_object(&path).await?;

    Ok(())
}

fn populate_sentry_scope(path: &ObjectPath) {
    sentry::configure_scope(|s| {
        s.set_tag("usecase", path.usecase.clone());
        s.set_extra("scope", path.scope.clone().into());
        s.set_extra("key", path.key.clone().into());
    });
}
