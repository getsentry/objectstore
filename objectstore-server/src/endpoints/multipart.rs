use std::collections::BTreeMap;
use std::convert::Infallible;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use bytes::Bytes;
use objectstore_service::error::Error as ServiceError;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::multipart::CompletedPart;
use objectstore_types::metadata::Metadata;
use serde::{Deserialize, Serialize};

use objectstore_types::auth::Permission;

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiResult};
use crate::extractors::Xt;
use crate::extractors::body::MeteredBody;
use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    let initiate_no_key = routing::post(initiate_post);
    Router::new()
        .route(
            "/objects:multipart/{usecase}/{scopes}",
            initiate_no_key.clone(),
        )
        .route("/objects:multipart/{usecase}/{scopes}/", initiate_no_key)
        .route(
            "/objects:multipart/{usecase}/{scopes}/{*key}",
            routing::put(initiate_put),
        )
        .route(
            "/objects:multipart:parts/{usecase}/{scopes}/{*key}",
            routing::get(list_parts).put(upload_part),
        )
        .route(
            "/objects:multipart:complete/{usecase}/{scopes}/{*key}",
            routing::post(complete),
        )
        .route(
            "/objects:multipart/{usecase}/{scopes}/{*key}",
            routing::delete(abort),
        )
}

// --- Query parameter types ---

#[derive(Debug, Deserialize)]
struct UploadPartQuery {
    upload_id: String,
    part_number: u32,
}

#[derive(Debug, Deserialize)]
struct UploadIdQuery {
    upload_id: String,
}

#[derive(Debug, Deserialize)]
struct ListPartsQuery {
    upload_id: String,
    max_parts: Option<u32>,
    part_number_marker: Option<u32>,
}

// --- Request/Response types ---

#[derive(Debug, Serialize)]
struct InitiateResponse {
    upload_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<String>,
}

#[derive(Debug, Serialize)]
struct UploadPartResponse {
    e_tag: String,
}

#[derive(Debug, Serialize)]
struct PartInfo {
    e_tag: String,
    last_modified: u64,
    size: u64,
}

#[derive(Debug, Serialize)]
struct ListPartsResponse {
    parts: BTreeMap<u32, PartInfo>,
    is_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_part_number_marker: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct CompletePartRequest {
    part_number: u32,
    etag: String,
}

#[derive(Debug, Deserialize)]
struct CompleteRequest {
    parts: Vec<CompletePartRequest>,
}

#[derive(Debug, Serialize)]
struct CompleteSuccessResponse {
    key: String,
}

#[derive(Debug, Serialize)]
struct CompleteErrorDetail {
    code: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct CompleteErrorResponse {
    error: CompleteErrorDetail,
}

// --- Handlers ---

async fn initiate_post(
    service: AuthAwareService,
    state: State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let id = ObjectId::optional(context, None);
    let key = Some(id.key().to_string());
    initiate_inner(service, state, id, key, headers).await
}

async fn initiate_put(
    service: AuthAwareService,
    state: State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    initiate_inner(service, state, id, None, headers).await
}

async fn initiate_inner(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    id: ObjectId,
    key: Option<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let mut metadata = Metadata::from_headers(&headers, "").map_err(ServiceError::from)?;
    // TODO: maybe do this on finalize?
    metadata.time_created = Some(SystemTime::now());

    state
        .config
        .usecases
        .validate(&id.context().usecase, &metadata)
        .map_err(|e| ApiError::Client(e.to_string()))?;

    let upload_id = service.initiate_multipart(id, metadata).await?;

    Ok((StatusCode::OK, Json(InitiateResponse { upload_id, key })).into_response())
}

async fn upload_part(
    service: AuthAwareService,
    State(_state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    Query(params): Query<UploadPartQuery>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let content_length = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| ApiError::Client("Content-Length header is required".into()))?;

    let content_md5 = headers
        .get("content-md5")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let etag = service
        .upload_part(
            id,
            params.upload_id,
            params.part_number,
            content_length,
            content_md5,
            body,
        )
        .await?;

    Ok((StatusCode::OK, Json(UploadPartResponse { e_tag: etag })).into_response())
}

async fn list_parts(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Query(params): Query<ListPartsQuery>,
) -> ApiResult<Response> {
    let response = service
        .list_parts(
            id,
            params.upload_id,
            params.max_parts,
            params.part_number_marker,
        )
        .await?;

    let parts = response
        .parts
        .into_iter()
        .map(|p| {
            let info = PartInfo {
                e_tag: p.etag,
                last_modified: p
                    .last_modified
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                size: p.size,
            };
            (p.part_number, info)
        })
        .collect();

    Ok(Json(ListPartsResponse {
        parts,
        is_truncated: response.is_truncated,
        next_part_number_marker: response.next_part_number_marker,
    })
    .into_response())
}

async fn abort(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Query(params): Query<UploadIdQuery>,
) -> ApiResult<impl IntoResponse> {
    service.abort_multipart(id, params.upload_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn complete(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Query(params): Query<UploadIdQuery>,
    Json(body): Json<CompleteRequest>,
) -> ApiResult<Response> {
    service.check_permission(Permission::ObjectWrite, id.context())?;

    let key = id.key().to_string();

    let parts: Vec<CompletedPart> = body
        .parts
        .into_iter()
        .map(|p| CompletedPart {
            part_number: p.part_number,
            etag: p.etag,
        })
        .collect();

    let upload_id = params.upload_id;

    let body_stream = async_stream::stream! {
        let mut keepalive = tokio::time::interval(Duration::from_secs(10));
        // Consume the first tick immediately (it fires at t=0).
        keepalive.tick().await;

        let result_fut = service.complete_multipart(id, upload_id, parts);
        tokio::pin!(result_fut);

        loop {
            tokio::select! {
                result = &mut result_fut => {
                    let json = match result {
                        Ok(None) => serde_json::to_vec(
                            &CompleteSuccessResponse { key },
                        ).unwrap(),
                        Ok(Some(err)) => serde_json::to_vec(
                            &CompleteErrorResponse {
                                error: CompleteErrorDetail {
                                    code: err.code,
                                    message: err.message,
                                },
                            },
                        ).unwrap(),
                        Err(e) => serde_json::to_vec(
                            &CompleteErrorResponse {
                                error: CompleteErrorDetail {
                                    code: "internal".into(),
                                    message: e.to_string(),
                                },
                            },
                        ).unwrap(),
                    };
                    yield Ok::<_, Infallible>(Bytes::from(json));
                    break;
                }
                _ = keepalive.tick() => {
                    yield Ok::<_, Infallible>(Bytes::from_static(b" "));
                }
            }
        }
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from_stream(body_stream))
        .unwrap())
}
