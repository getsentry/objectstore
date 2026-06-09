use std::time::SystemTime;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use jsonwebtoken::get_current_timestamp;
use objectstore_service::error::Error as ServiceError;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::auth::Permission;
use objectstore_types::metadata::Metadata;
use objectstore_types::range::ContentRange;
use serde::{Deserialize, Serialize};

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiResult, insert_accept_ranges};
use crate::extractors::byte_range::OptionalByteRange;
use crate::extractors::service::StrictAuthContext;
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
        .route(
            "/objects:presign/{usecase}/{scopes}/{*key}",
            routing::post(object_presign),
        )
        .route("/objects/{usecase}/{scopes}/{*key}", object_routes)
}

/// Response returned when inserting an object.
#[derive(Debug, Serialize)]
pub struct InsertObjectResponse {
    pub key: String,
}

#[derive(Debug, Deserialize)]
struct PresignQuery {
    operation: String,
    expires_at: u64,
}

#[derive(Debug, Serialize)]
struct PresignResponse {
    signature: String,
    expires_at: u64,
    operation: String,
}

async fn object_presign(
    StrictAuthContext(auth): StrictAuthContext,
    State(state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    Query(query): Query<PresignQuery>,
) -> ApiResult<Response> {
    if query.operation != "GET" {
        return Err(ApiError::Client("operation must be GET".into()));
    }

    let now = get_current_timestamp();
    if query.expires_at <= now {
        return Err(ApiError::Client("expires_at must be in the future".into()));
    }
    if query.expires_at > auth.expires_at {
        return Err(crate::auth::AuthError::NotPermitted.into());
    }

    auth.assert_authorized(Permission::ObjectRead, id.context())?;

    let signature = state
        .presigned_key_directory
        .sign_get(&id, query.expires_at)?;
    Ok(Json(PresignResponse {
        signature,
        expires_at: query.expires_at,
        operation: "GET".into(),
    })
    .into_response())
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
    Xt(id): Xt<ObjectId>,
    service: AuthAwareService,
    State(state): State<ServiceState>,
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
    let metadata_headers = metadata.to_headers("").map_err(ServiceError::from)?;

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

async fn object_head(Xt(id): Xt<ObjectId>, service: AuthAwareService) -> ApiResult<Response> {
    let Some(metadata) = service.get_metadata(id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let headers = metadata.to_headers("").map_err(ServiceError::from)?;

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
