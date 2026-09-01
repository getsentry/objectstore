use std::fmt::Write as _;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use objectstore_service::error::{ErrorKind, ResultExt as _};
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::headers::ExtValue;
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
    let metadata = Metadata::from_insert_headers(&headers, "")
        .map_err(|error| ApiError::Client(error.to_string()))?;

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
        Err(ApiError::Service(e)) => match e.kind() {
            ErrorKind::RangeNotSatisfiable { total } => {
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
            _ => return Err(e.into()),
        },
        Err(e) => return Err(e),
    };

    let stream = state.meter_stream(stream, &context);
    let mut metadata_headers = metadata
        .to_headers("")
        .context(ErrorKind::Internal, "encoding object response metadata")?;

    let mut response = match content_range {
        Some(ref content_range) => {
            metadata_headers.insert(
                http::header::CONTENT_LENGTH,
                content_range.len_to_header_value(),
            );
            metadata_headers.insert(http::header::CONTENT_RANGE, content_range.to_header_value());

            (
                StatusCode::PARTIAL_CONTENT,
                metadata_headers,
                Body::from_stream(stream),
            )
                .into_response()
        }
        None => {
            insert_content_length(&mut metadata_headers, &metadata);
            (StatusCode::OK, metadata_headers, Body::from_stream(stream)).into_response()
        }
    };

    insert_content_disposition(&mut response, &metadata);
    insert_accept_ranges(&mut response);

    Ok(response)
}

async fn object_head(service: AuthAwareService, Xt(id): Xt<ObjectId>) -> ApiResult<Response> {
    let Some(metadata) = service.get_metadata(id).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let mut headers = metadata
        .to_headers("")
        .context(ErrorKind::Internal, "encoding object response metadata")?;
    insert_content_length(&mut headers, &metadata);

    let mut response = (StatusCode::OK, headers).into_response();
    insert_content_disposition(&mut response, &metadata);
    insert_accept_ranges(&mut response);
    Ok(response)
}

/// Inserts a `Content-Length` header covering the complete object.
///
/// Only valid for responses whose body is the whole object, and for `HEAD` responses, which
/// describe what a `GET` would have returned. Ranged responses announce the length of the range
/// instead and must not use this.
fn insert_content_length(headers: &mut HeaderMap, metadata: &Metadata) {
    if let Some(size) = metadata.size {
        headers.insert(
            http::header::CONTENT_LENGTH,
            http::HeaderValue::from(size as u64),
        );
    }
}

fn insert_content_disposition(response: &mut Response, metadata: &Metadata) {
    if let Some(filename) = metadata.filename.as_deref() {
        response.headers_mut().insert(
            http::header::CONTENT_DISPOSITION,
            format_content_disposition(filename),
        );
    }
}

/// Formats a `Content-Disposition: attachment; filename="..."` header value.
///
/// The filename is sanitized (`/` and `\` become `-`, dots-only names become all dashes, non-ASCII
/// and control characters become `_`) and then escaped for the RFC 6266 quoted-string (`"` is
/// backslash-escaped).
///
/// A filename that is not pure ASCII cannot be represented in that quoted-string, so it
/// additionally gets an RFC 8187 `filename*` parameter carrying the full UTF-8 value. The
/// quoted-string then serves as the ASCII fallback for clients that ignore `filename*`.
fn format_content_disposition(filename: &str) -> http::HeaderValue {
    let all_dots = filename.chars().all(|c| c == '.');

    let mut result = String::from("attachment; filename=\"");
    for c in filename.chars() {
        let c = match c {
            '/' | '\\' => '-',
            '.' if all_dots => '-',
            '"' => {
                result.push('\\');
                '"'
            }
            c if !c.is_ascii() || c.is_control() => '_',
            c => c,
        };
        result.push(c);
    }
    result.push('"');

    if !filename.is_ascii() {
        write!(result, "; filename*={}", ExtValue(filename))
            .expect("writing to a string cannot fail");
    }

    // INVARIANT: every character written above is visible ASCII — the quoted-string replaces
    // non-ASCII and control characters with `_`, and the `ext-value` is percent-encoded.
    http::HeaderValue::from_str(&result).expect("content disposition is a valid header value")
}

async fn object_put(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let metadata = Metadata::from_insert_headers(&headers, "")
        .map_err(|error| ApiError::Client(error.to_string()))?;

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
