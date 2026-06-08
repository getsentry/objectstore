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
use objectstore_types::range::ContentRange;
use serde::Serialize;

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiResult, insert_accept_ranges};
use crate::extractors::byte_range::OptionalByteRange;
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
    let mut metadata = Metadata::from_headers(&headers, "")
        .map_err(|cause| ServiceError::metadata_client("invalid object metadata headers", cause))?;
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
    OptionalByteRange(byte_range): OptionalByteRange,
    _headers: HeaderMap,
) -> ApiResult<Response> {
    let context = id.context().clone();
    let result = service.get_object(id, byte_range).await;

    let (metadata, content_range, stream) = match result {
        Ok(Some(result)) => result,
        Ok(None) => return Ok(StatusCode::NOT_FOUND.into_response()),
        Err(ApiError::Service(ServiceError::RangeNotSatisfiable { total })) => {
            let mut response = (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(
                    http::header::CONTENT_RANGE,
                    ContentRange::unsatisfiable_total_to_header_value(total),
                )],
            )
                .into_response();
            insert_accept_ranges(&mut response);
            return Ok(response);
        }
        Err(e) => return Err(e),
    };

    let stream = state.meter_stream(stream, &context);
    let metadata_headers = metadata
        .to_headers("")
        .map_err(|cause| ServiceError::metadata("failed to serialize object metadata", cause))?;

    let mut response = match content_range {
        Some(ref content_range) => {
            let mut resp = (
                StatusCode::PARTIAL_CONTENT,
                metadata_headers,
                Body::from_stream(stream),
            )
                .into_response();
            let headers = resp.headers_mut();
            headers.insert(
                http::header::CONTENT_LENGTH,
                content_range.len_to_header_value(),
            );
            headers.insert(http::header::CONTENT_RANGE, content_range.to_header_value());
            resp
        }
        None => (StatusCode::OK, metadata_headers, Body::from_stream(stream)).into_response(),
    };

    insert_accept_ranges(&mut response);

    Ok(response)
}

async fn object_head(service: AuthAwareService, Xt(id): Xt<ObjectId>) -> ApiResult<Response> {
    let Some(metadata) = service.get_metadata(id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata
        .to_headers("")
        .map_err(|cause| ServiceError::metadata("failed to serialize object metadata", cause))?;

    let mut response = (StatusCode::NO_CONTENT, headers).into_response();
    insert_accept_ranges(&mut response);
    Ok(response)
}

async fn object_put(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let mut metadata = Metadata::from_headers(&headers, "")
        .map_err(|cause| ServiceError::metadata_client("invalid object metadata headers", cause))?;
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
