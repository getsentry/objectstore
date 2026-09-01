//! Resumable upload endpoints.
//!
//! Resumable uploads are a variation of the regular object endpoints rather than a separate
//! resource. Every request addresses a standard object path with the session in the query string
//! (or `upload_type=resumable` for creation).
//!
//! | Operation | Request | Success |
//! |---|---|---|
//! | Create | `POST /objects/{usecase}/{scopes}/?upload_type=resumable` | `200` + `{"key","session"}` |
//! | Create | `PUT /objects/{usecase}/{scopes}/{key}?upload_type=resumable` | `200` + `{"key","session"}` |
//! | Chunk | `PUT …/{key}?session=<s>` with `Upload-Offset: <n>` | `204` + `Upload-Offset`, or `201` + `{"key"}` |
//! | Offset query | `PUT …/{key}?session=<s>` with `Upload-Offset: *` | `204` + `Upload-Offset`, or `201` + `{"key"}` |
//! | Cancel | `DELETE …/{key}?session=<s>` | `204` |

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, http};
use axum_extra::TypedHeader;
use axum_extra::headers::ContentLength;
use objectstore_service::error::Error as ServiceError;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::stream::ClientStream;
use objectstore_types::metadata::Metadata;
use objectstore_types::resumable::{
    CommitResponse, CreateSessionResponse, HEADER_UPLOAD_OFFSET, UploadOffset, UploadProgress,
};

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiResult};
use crate::extractors::{Xt, body::MeteredBody};
use crate::resumable::{Session, UploadLengthHeader, UploadOffsetHeader, require_empty_body};
use crate::state::ServiceState;

/// Creates a session with a server-generated object key.
pub(super) async fn create_session(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    TypedHeader(UploadLengthHeader(total_length)): TypedHeader<UploadLengthHeader>,
    content_length: Option<TypedHeader<ContentLength>>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    create_session_for_id(
        service,
        state,
        ObjectId::optional(context, None),
        total_length,
        content_length.map(|TypedHeader(header)| header),
        headers,
        body,
    )
    .await
}

/// Creates a session for the object key in the request path.
pub(super) async fn create_session_for_key(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(id): Xt<ObjectId>,
    TypedHeader(UploadLengthHeader(total_length)): TypedHeader<UploadLengthHeader>,
    content_length: Option<TypedHeader<ContentLength>>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    create_session_for_id(
        service,
        state,
        id,
        total_length,
        content_length.map(|TypedHeader(header)| header),
        headers,
        body,
    )
    .await
}

/// Creates a session for the object at `id`.
///
/// Answers `501 Not Implemented` when the backend refuses to create a session or doesn't implement
/// resumable uploads.
async fn create_session_for_id(
    service: AuthAwareService,
    state: ServiceState,
    id: ObjectId,
    total_length: u64,
    content_length: Option<ContentLength>,
    headers: HeaderMap,
    body: ClientStream,
) -> ApiResult<Response> {
    require_empty_body(content_length, body, "resumable session creation").await?;
    let metadata = Metadata::from_insert_headers(&headers, "").map_err(ServiceError::from)?;

    state
        .config
        .usecases
        .validate(&id.context().usecase, &metadata)
        .map_err(|e| ApiError::Client(e.to_string()))?;

    let session = service
        .create_upload_session(id.clone(), metadata, total_length)
        .await?
        .ok_or(ServiceError::NotImplemented)?;

    let body = Json(CreateSessionResponse {
        key: id.key().to_owned(),
        session,
    });
    Ok((StatusCode::OK, body).into_response())
}

/// Acts on an open session: writes a chunk, or reports the offset the server holds.
///
/// [`HEADER_UPLOAD_OFFSET`] selects between the two. A concrete offset submits the request
/// body as the chunk starting there; the `*` wildcard submits nothing and asks where the
/// server stands, which also commits an object that was assembled but not yet committed.
/// Chunks require `Content-Length`, including over HTTP/2. Offset queries may omit it, but the
/// body stream is checked and any bytes are rejected as a malformed request.
///
/// Both answer `204 No Content` with the authoritative offset while bytes remain, and
/// `201 Created` with the key once the object is committed.
/// The acknowledged offset may be lower than the submitted chunk's end, so clients should continue
/// from this response (or from a later explicit offset query), never from local byte accounting alone.
pub(super) async fn continue_session(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Session(session): Session,
    TypedHeader(UploadOffsetHeader(offset)): TypedHeader<UploadOffsetHeader>,
    content_length: Option<TypedHeader<ContentLength>>,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let key = id.key().to_owned();

    let progress = match offset {
        UploadOffset::At(offset) => {
            let content_length = content_length
                .map(|TypedHeader(ContentLength(length))| length)
                .ok_or_else(|| ApiError::Client("Content-Length header is required".into()))?;
            service
                .put_chunk(id, session, offset, content_length, body)
                .await
        }
        UploadOffset::Unknown => {
            // The wildcard carries no payload. A body would be silently discarded, so
            // reject it rather than let a client believe those bytes were written.
            require_empty_body(
                content_length.map(|TypedHeader(header)| header),
                body,
                "Upload-Offset: *",
            )
            .await?;

            service.upload_offset(id, session).await
        }
    };

    progress_response(progress, key)
}

/// Cancels a session, discarding whatever was uploaded.
pub(super) async fn cancel_session(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Session(session): Session,
) -> ApiResult<Response> {
    service.cancel_upload(id, session).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Turns an [`UploadProgress`] outcome into the response shared by chunks and offset queries.
fn progress_response(progress: ApiResult<UploadProgress>, key: String) -> ApiResult<Response> {
    let progress = match progress {
        Ok(progress) => progress,
        Err(error @ ApiError::Service(ServiceError::UploadOffsetMismatch { offset })) => {
            let mut response = error.into_response();
            response
                .headers_mut()
                .insert(HEADER_UPLOAD_OFFSET, http::HeaderValue::from(offset));
            return Ok(response);
        }
        Err(error) => return Err(error),
    };

    let response = match progress {
        UploadProgress::Incomplete { offset } => (
            StatusCode::NO_CONTENT,
            [(HEADER_UPLOAD_OFFSET, http::HeaderValue::from(offset))],
        )
            .into_response(),
        UploadProgress::Committed => {
            (StatusCode::CREATED, Json(CommitResponse { key })).into_response()
        }
    };

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads a response's status, `Upload-Offset` header, and body.
    async fn parts_of(response: Response) -> (StatusCode, Option<String>, String) {
        let status = response.status();
        let offset = response
            .headers()
            .get(HEADER_UPLOAD_OFFSET)
            .map(|v| v.to_str().unwrap().to_owned());

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        (status, offset, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn incomplete_progress_answers_no_content_with_the_offset() {
        let progress = Ok(UploadProgress::Incomplete { offset: 262_144 });
        let response = progress_response(progress, "my-key".into()).unwrap();

        let (status, offset, body) = parts_of(response).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(offset.as_deref(), Some("262144"));
        assert!(body.is_empty(), "204 must not carry a body: {body:?}");
    }

    #[tokio::test]
    async fn commit_answers_created_with_the_key() {
        let response = progress_response(Ok(UploadProgress::Committed), "my-key".into()).unwrap();

        let (status, offset, body) = parts_of(response).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(offset, None, "a commit reports no offset");
        assert_eq!(body, r#"{"key":"my-key"}"#);
    }

    #[tokio::test]
    async fn offset_mismatch_answers_conflict_with_the_authoritative_offset() {
        let mismatch = ServiceError::UploadOffsetMismatch { offset: 786_432 };
        let response =
            progress_response(Err(ApiError::Service(mismatch)), "my-key".into()).unwrap();

        let (status, offset, body) = parts_of(response).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            offset.as_deref(),
            Some("786432"),
            "the client resynchronizes from this header"
        );
        assert!(body.contains("786432"), "{body:?}");
    }

    #[tokio::test]
    async fn other_errors_propagate_unchanged() {
        let gone = ApiError::Service(ServiceError::UploadSessionGone);
        let error = progress_response(Err(gone), "my-key".into()).unwrap_err();
        assert_eq!(error.status(), StatusCode::GONE);

        let oversized = ApiError::Service(ServiceError::ChunkExceedsUploadLength {
            offset: 8,
            content_length: 4,
            upload_length: 10,
        });
        let error = progress_response(Err(oversized), "my-key".into()).unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }
}
