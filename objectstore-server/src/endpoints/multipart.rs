use std::convert::Infallible;
use std::time::{Duration, SystemTime};

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use http::HeaderValue;
use http::header;
use objectstore_service::error::Error as ServiceError;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::multipart::{CompletedPart, PartNumber, UploadId};
use objectstore_types::auth::Permission;
use objectstore_types::metadata::Metadata;
use objectstore_types::multipart::{
    CompleteErrorDetail, CompleteErrorResponse, CompleteRequest, CompleteSuccessResponse,
    InitiateResponse, ListPartsResponse, PartInfo, UploadPartResponse,
};
use serde::Deserialize;
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
            routing::put(initiate_put).delete(abort),
        )
        .route(
            "/objects:multipart:parts/{usecase}/{scopes}/{*key}",
            routing::get(list_parts).put(upload_part),
        )
        .route(
            "/objects:multipart:complete/{usecase}/{scopes}/{*key}",
            routing::post(complete),
        )
}

// --- Query parameter types ---

#[derive(Debug, Deserialize)]
struct UploadPartQuery {
    upload_id: UploadId,
    part_number: PartNumber,
}

#[derive(Debug, Deserialize)]
struct UploadIdQuery {
    upload_id: UploadId,
}

#[derive(Debug, Deserialize)]
struct ListPartsQuery {
    upload_id: UploadId,
    max_parts: Option<u32>,
    part_number_marker: Option<PartNumber>,
}

fn validate_part_number(part_number: u32) -> ApiResult<()> {
    if part_number == 0 {
        return Err(ApiError::Client("part_number must be >= 1".into()));
    }
    Ok(())
}
// --- Handlers ---

async fn initiate_put(
    service: AuthAwareService,
    state: State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    initiate_inner(service, state, id, headers).await
}

async fn initiate_post(
    service: AuthAwareService,
    state: State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let id = ObjectId::optional(context, None);
    initiate_inner(service, state, id, headers).await
}

async fn initiate_inner(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    id: ObjectId,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let mut metadata = Metadata::from_headers(&headers, "").map_err(ServiceError::from)?;
    // TODO: Do this in `complete` instead, when we have a Service API to mutate metadata.
    metadata.time_created = Some(SystemTime::now());

    state
        .config
        .usecases
        .validate(&id.context().usecase, &metadata)
        .map_err(|e| ApiError::Client(e.to_string()))?;

    let upload_id = service.initiate_multipart(id.clone(), metadata).await?;

    let response = Json(InitiateResponse {
        key: id.key().to_owned(),
        upload_id,
    });
    Ok((StatusCode::OK, response).into_response())
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

    // Content-MD5 must be base64-encoded per RFC 1864; passed through to the
    // storage backend for integrity verification.
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

    let response = Json(UploadPartResponse { etag });
    Ok((StatusCode::OK, response).into_response())
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
                etag: p.etag,
                last_modified: p.last_modified,
                size: p.size,
            };
            (p.part_number, info)
        })
        .collect();

    let response = Json(ListPartsResponse {
        parts,
        is_truncated: response.is_truncated,
        next_part_number_marker: response.next_part_number_marker,
    });
    Ok((StatusCode::OK, response).into_response())
}

async fn abort(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Query(params): Query<UploadIdQuery>,
) -> ApiResult<Response> {
    service.abort_multipart(id, params.upload_id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn complete(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Query(params): Query<UploadIdQuery>,
    Json(body): Json<CompleteRequest>,
) -> ApiResult<Response> {
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

    // This operation can take a while at the service level, so we stream whitespace to the client
    // until we have a response body, to keep the connection from being terminated.
    let stream = async_stream::stream! {
        let fut = service.complete_multipart(id, upload_id, parts);
        tokio::pin!(fut);

        let mut keepalive = tokio::time::interval(Duration::from_secs(1));
        keepalive.tick().await;
        loop {
            tokio::select! {
                res = &mut fut => {
                    let serialized = match res {
                        Ok(None) => serde_json::to_vec(
                            &CompleteSuccessResponse { key },
                        ),
                        Ok(Some(err)) => serde_json::to_vec(
                            &CompleteErrorResponse {
                                error: CompleteErrorDetail {
                                    code: err.code,
                                    message: err.message,
                                },
                            },
                        ),
                        // TODO(lcian): Construct more precise error code and message, given that
                        // we have a structured `ApiError`.
                        Err(e) => serde_json::to_vec(
                            &CompleteErrorResponse {
                                error: CompleteErrorDetail {
                                    code: "internal error".into(),
                                    message: e.to_string(),
                                },
                            },
                        ),
                    };
                    // Fallback to avoid `unwrap()`. This should never happen in practice.
                    let serialized = serialized.unwrap_or_else(|_| {
                        br#"{"error":{"code":"internal error","message":"unexpected error, please report a bug"}}"#.to_vec()
                    });

                    yield Bytes::from(serialized);
                    break;
                }
                _ = keepalive.tick() => {
                    yield Bytes::from_static(b" ");
                }
            }
        }
    };
    let stream = stream.map(Ok::<_, Infallible>);

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok((StatusCode::OK, headers, Body::from_stream(stream)).into_response())
}
