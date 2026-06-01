use std::time::SystemTime;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use objectstore_service::error::Error as ServiceError;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::metadata::Metadata;
use objectstore_types::range::{ByteRange, RangeError};
use serde::Serialize;

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiResult};
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
    State(state): State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let mut metadata = Metadata::from_headers(&headers, "").map_err(ServiceError::from)?;
    metadata.time_created = Some(SystemTime::now());

    state
        .config
        .usecases
        .validate(&context.usecase, &metadata)
        .map_err(|e| ApiError::Client(e.to_string()))?;

    let response_id = service.insert_object(context, None, metadata, body).await?;
    let response = Json(InsertObjectResponse {
        key: response_id.key().to_string(),
    });

    Ok((StatusCode::CREATED, response).into_response())
}

async fn object_get(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let byte_range = match headers.get(http::header::RANGE) {
        Some(value) => {
            let header_str = value
                .to_str()
                .map_err(|_| ApiError::Client("invalid Range header".into()))?;
            match header_str.parse::<ByteRange>() {
                Ok(range) => Some(range),
                // If we don't know the unit or the client wants multiple ranges, fall back to
                // returning the whole object.
                Err(RangeError::InvalidUnit(_) | RangeError::MultiRange) => None,
                Err(e) => return Err(ApiError::Client(format!("invalid Range header: {e}"))),
            }
        }
        None => None,
    };

    let context = id.context().clone();
    let result = service.get_object(id, byte_range).await;

    let (metadata, content_range, stream) = match result {
        Ok(Some(result)) => result,
        Ok(None) => return Ok(StatusCode::NOT_FOUND.into_response()),
        Err(ApiError::Service(ServiceError::RangeNotSatisfiable { total })) => {
            return Ok((
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(http::header::CONTENT_RANGE, format!("bytes */{total}"))],
                [(http::header::ACCEPT_RANGES, "bytes")],
            )
                .into_response());
        }
        Err(e) => return Err(e),
    };

    let stream = state.meter_stream(stream, &context);
    let metadata_headers = metadata.to_headers("").map_err(ServiceError::from)?;

    let is_partial = !content_range.is_full();
    let status = if is_partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let mut response = (status, metadata_headers, Body::from_stream(stream)).into_response();

    let headers = response.headers_mut();
    headers.insert(
        http::header::ACCEPT_RANGES,
        http::header::HeaderValue::from_static("bytes"),
    );
    if is_partial {
        // When `is_partial == false`, `CONTENT_LENGTH is already part of `metadata_headers`.
        headers.insert(
            http::header::CONTENT_LENGTH,
            content_range.len_to_header_value(),
        );
        headers.insert(http::header::CONTENT_RANGE, content_range.to_header_value());
    }

    Ok(response)
}

async fn object_head(service: AuthAwareService, Xt(id): Xt<ObjectId>) -> ApiResult<Response> {
    let Some(metadata) = service.get_metadata(id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata.to_headers("").map_err(ServiceError::from)?;

    Ok((StatusCode::NO_CONTENT, headers).into_response())
}

async fn object_put(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let mut metadata = Metadata::from_headers(&headers, "").map_err(ServiceError::from)?;
    metadata.time_created = Some(SystemTime::now());

    let ObjectId { context, key } = id;

    state
        .config
        .usecases
        .validate(&context.usecase, &metadata)
        .map_err(|e| ApiError::Client(e.to_string()))?;

    let response_id = service
        .insert_object(context, Some(key), metadata, body)
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
    service.delete_object(id).await?;
    Ok(StatusCode::NO_CONTENT)
}
