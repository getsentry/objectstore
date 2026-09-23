//! Object creation, retrieval, expiry extension, and deletion endpoints.
//!
//! `PATCH /v1/objects/{usecase}/{scopes}/{key}` accepts `application/json` with exactly one
//! expiry-extension request. Absolute deadlines and durations anchored to request or creation time
//! are supported:
//!
//! ```json
//! {"extend_expiry": {"at": "2026-10-16T12:00:00Z"}}
//! ```
//!
//! ```json
//! {"extend_expiry": {"after": "30d", "from": "creation"}}
//! ```
//!
//! ```json
//! {"extend_expiry": {"after": "30d", "from": "now"}}
//! ```
//!
//! The operation only extends live TTL and TTI objects. It preserves the expiration policy,
//! including its duration, as well as the payload and all other metadata.
//! A satisfied request returns 204, including an already-sufficient deadline. An object
//! observed absent or expired returns 404. Ineligible or conflicting updates return 409;
//! backend failures use the normal service error responses. Invalid timestamp or duration
//! strings are rejected by JSON deserialization with 422.

use std::fmt::Write as _;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::handler::Handler;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing;
use axum::{Json, Router};
use objectstore_service::backend::common::{ExpiryTarget, SetExpiryResponse};
use objectstore_service::error::ErrorKind;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_types::headers::ExtValue;
use objectstore_types::metadata::{ExpiryAnchor, ExpiryExtension, Metadata, MetadataUpdate};
use objectstore_types::range::ContentRange;
use serde::Serialize;

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiResult, insert_accept_ranges};
use crate::endpoints::resumable;
use crate::extractors::byte_range::OptionalByteRange;
use crate::extractors::request_time::RequestTime;
use crate::extractors::{Xt, body::MeteredBody};
use crate::resumable::ResumableTarget;
use crate::state::ServiceState;

pub fn router() -> Router<ServiceState> {
    let collection_routes = routing::post(dispatch_objects_post);
    let object_routes = routing::get(object_get)
        .head(object_head)
        .put(dispatch_object_put)
        .patch(object_patch)
        .delete(dispatch_object_delete);

    Router::new()
        .route("/objects/{usecase}/{scopes}", collection_routes.clone())
        .route("/objects/{usecase}/{scopes}/", collection_routes)
        .route("/objects/{usecase}/{scopes}/{*key}", object_routes)
}

async fn dispatch_objects_post(
    State(state): State<ServiceState>,
    target: Option<ResumableTarget>,
    request: Request,
) -> Response {
    if target.is_some() {
        resumable::create_session.call(request, state).await
    } else {
        create_object.call(request, state).await
    }
}

async fn dispatch_object_put(
    State(state): State<ServiceState>,
    target: Option<ResumableTarget>,
    request: Request,
) -> Response {
    match target {
        Some(ResumableTarget::NewSession) => {
            resumable::create_session_for_key.call(request, state).await
        }
        Some(ResumableTarget::ExistingSession) => {
            resumable::continue_session.call(request, state).await
        }
        None => insert_object.call(request, state).await,
    }
}

async fn dispatch_object_delete(
    State(state): State<ServiceState>,
    target: Option<ResumableTarget>,
    request: Request,
) -> Response {
    if target.is_some() {
        resumable::cancel_session.call(request, state).await
    } else {
        delete_object.call(request, state).await
    }
}

async fn object_patch(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    RequestTime(access_time): RequestTime,
    Json(update): Json<MetadataUpdate>,
) -> ApiResult<StatusCode> {
    let target = match update.extend_expiry {
        ExpiryExtension::At { at } => ExpiryTarget::At(at.into_inner()),
        ExpiryExtension::After {
            after,
            from: ExpiryAnchor::Now,
        } => {
            let at = access_time.checked_add(after).ok_or_else(|| {
                ApiError::client("expiration deadline is outside supported range")
            })?;
            ExpiryTarget::At(at)
        }
        ExpiryExtension::After {
            after,
            from: ExpiryAnchor::Creation,
        } => ExpiryTarget::FromCreation(after),
    };

    match service.set_expiry(id, target, access_time).await? {
        SetExpiryResponse::Satisfied(_) => Ok(StatusCode::NO_CONTENT),
        SetExpiryResponse::NotFound => Ok(StatusCode::NOT_FOUND),
        SetExpiryResponse::Rejected => Err(ApiError::conflict(
            "expiry extension could not be satisfied",
        )),
    }
}

/// Response returned when inserting an object.
#[derive(Debug, Serialize)]
pub struct InsertObjectResponse {
    pub key: String,
}

async fn create_object(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    headers: HeaderMap,
    RequestTime(access_time): RequestTime,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let metadata = Metadata::from_insert_headers(&headers, "", access_time)?;

    state
        .config
        .usecases
        .validate(&context.usecase, &metadata)?;

    let response_id = service
        .insert_object(context, None, metadata, body, access_time)
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
    _headers: HeaderMap,
    RequestTime(access_time): RequestTime,
    OptionalByteRange(byte_range): OptionalByteRange,
) -> ApiResult<Response> {
    let context = id.context().clone();
    let result = service.get_object(id, access_time, byte_range).await;

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
        .map_err(|error| ApiError::internal("encoding object response metadata", error))?;

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

async fn object_head(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    RequestTime(access_time): RequestTime,
) -> ApiResult<Response> {
    let Some(metadata) = service.get_metadata(id, access_time).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };

    let mut headers = metadata
        .to_headers("")
        .map_err(|error| ApiError::internal("encoding object response metadata", error))?;
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

async fn insert_object(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    headers: HeaderMap,
    RequestTime(access_time): RequestTime,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let metadata = Metadata::from_insert_headers(&headers, "", access_time)?;

    let ObjectId { context, key } = id;

    state
        .config
        .usecases
        .validate(&context.usecase, &metadata)?;

    let response_id = service
        .insert_object(context, Some(key), metadata, body, access_time)
        .await?;

    let response = Json(InsertObjectResponse {
        key: response_id.key.to_string(),
    });

    Ok((StatusCode::OK, response).into_response())
}

async fn delete_object(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    RequestTime(access_time): RequestTime,
) -> ApiResult<impl IntoResponse> {
    service.delete_object(id, access_time).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use objectstore_service::StorageService;
    use objectstore_service::backend::common::Backend;
    use objectstore_service::backend::in_memory::InMemoryBackend;
    use objectstore_service::concurrency::ConcurrencyLimiter;
    use objectstore_service::encryption::Cipher;
    use objectstore_service::id::ObjectContext;
    use objectstore_service::stream;
    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};
    use objectstore_types::time::Timestamp;

    use super::*;
    use crate::auth::AuthContext;

    fn object_id(key: &str) -> ObjectId {
        ObjectId::new(
            ObjectContext {
                usecase: "testing".into(),
                scopes: Scopes::from_iter([Scope::create("org", "1").unwrap()]),
            },
            key.into(),
        )
    }

    #[tokio::test]
    async fn expiry_extension_distinguishes_missing_and_ineligible_objects() {
        let access_time = Timestamp::now();
        for (metadata, expected) in [
            (None, StatusCode::NOT_FOUND),
            (
                Some(Metadata {
                    expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(60)),
                    time_expires: Some(access_time - Duration::from_secs(1)),
                    ..Default::default()
                }),
                StatusCode::NOT_FOUND,
            ),
            (Some(Metadata::default()), StatusCode::CONFLICT),
        ] {
            let storage = StorageService::new(
                Box::new(InMemoryBackend::new("in-memory")),
                Cipher::ephemeral().unwrap(),
            );
            let id = object_id("expiry-outcome");
            if let Some(metadata) = metadata {
                storage
                    .insert_object(
                        id.context().clone(),
                        Some(id.key().into()),
                        metadata,
                        stream::single("payload"),
                        access_time,
                    )
                    .await
                    .unwrap();
            }
            let service = AuthAwareService::new(storage, AuthContext::Disabled, true);
            let response = object_patch(
                service,
                Xt(id),
                RequestTime(access_time),
                Json(MetadataUpdate {
                    extend_expiry: ExpiryExtension::After {
                        after: Duration::from_hours(1),
                        from: ExpiryAnchor::Now,
                    },
                }),
            )
            .await
            .into_response();
            assert_eq!(response.status(), expected);
            if expected == StatusCode::NOT_FOUND {
                assert!(
                    axum::body::to_bytes(response.into_body(), 1024)
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
        }
    }

    #[tokio::test]
    async fn creation_relative_extension_without_creation_time_is_rejected() {
        let storage = StorageService::new(
            Box::new(InMemoryBackend::new("in-memory")),
            Cipher::ephemeral().unwrap(),
        );
        let service = AuthAwareService::new(storage.clone(), AuthContext::Disabled, true);
        let access_time = Timestamp::now();
        let id = object_id("missing-creation");
        storage
            .insert_object(
                id.context().clone(),
                Some(id.key().into()),
                Metadata {
                    expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(60)),
                    time_created: None,
                    time_expires: Some(access_time + Duration::from_secs(60)),
                    ..Default::default()
                },
                stream::single("payload"),
                access_time,
            )
            .await
            .unwrap();

        let error = object_patch(
            service,
            Xt(id),
            RequestTime(access_time),
            Json(MetadataUpdate {
                extend_expiry: ExpiryExtension::After {
                    after: Duration::from_secs(30 * 86400),
                    from: ExpiryAnchor::Creation,
                },
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(error.status(), StatusCode::CONFLICT);
        assert_eq!(error.to_string(), "expiry extension could not be satisfied");
    }

    #[tokio::test]
    async fn every_anchor_uses_one_set_expiry_operation() {
        let storage = StorageService::new(
            Box::new(InMemoryBackend::new("in-memory")),
            Cipher::ephemeral().unwrap(),
        );
        let access_time = Timestamp::now();

        for (key, extension) in [
            (
                "absolute",
                ExpiryExtension::At {
                    at: (access_time + Duration::from_secs(120)).as_rfc3339(),
                },
            ),
            (
                "now",
                ExpiryExtension::After {
                    after: Duration::from_mins(2),
                    from: ExpiryAnchor::Now,
                },
            ),
            (
                "creation",
                ExpiryExtension::After {
                    after: Duration::from_mins(2),
                    from: ExpiryAnchor::Creation,
                },
            ),
        ] {
            let id = object_id(key);
            storage
                .insert_object(
                    id.context().clone(),
                    Some(id.key().into()),
                    Metadata {
                        expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(60)),
                        time_created: Some(access_time),
                        time_expires: Some(access_time + Duration::from_secs(60)),
                        ..Default::default()
                    },
                    stream::single("payload"),
                    access_time,
                )
                .await
                .unwrap();

            let captured = objectstore_metrics::with_capturing_test_client_async(async {
                let service = AuthAwareService::new(storage.clone(), AuthContext::Disabled, true);
                let status = object_patch(
                    service,
                    Xt(id),
                    RequestTime(access_time),
                    Json(MetadataUpdate {
                        extend_expiry: extension,
                    }),
                )
                .await
                .unwrap();
                assert_eq!(status, StatusCode::NO_CONTENT);
            })
            .await;
            let operations = captured
                .iter()
                .filter(|metric| metric.starts_with("cogs.usage"))
                .count();
            assert_eq!(operations, 1, "{captured:?}");
        }
    }

    #[tokio::test]
    async fn expiry_extension_preserves_service_errors() {
        let backend = InMemoryBackend::new("in-memory");
        let access_time = Timestamp::now();
        let id = object_id("service-error");
        backend
            .put_object(
                &id,
                &Metadata {
                    expiration_policy: ExpirationPolicy::TimeToLive(Duration::from_secs(60)),
                    time_created: Some(access_time),
                    time_expires: Some(access_time + Duration::from_secs(60)),
                    ..Default::default()
                },
                stream::single("payload"),
                access_time,
            )
            .await
            .unwrap();
        let storage = StorageService::new(Box::new(backend), Cipher::ephemeral().unwrap())
            .with_concurrency(ConcurrencyLimiter::new(0));
        let service = AuthAwareService::new(storage, AuthContext::Disabled, true);

        let error = object_patch(
            service,
            Xt(id),
            RequestTime(access_time),
            Json(MetadataUpdate {
                extend_expiry: ExpiryExtension::At {
                    at: (access_time + Duration::from_secs(120)).as_rfc3339(),
                },
            }),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ApiError::Service(_)));
        assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
