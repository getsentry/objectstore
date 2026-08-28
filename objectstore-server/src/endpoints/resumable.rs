//! Resumable upload endpoints.
//!
//! Resumable uploads are a variation of the regular object endpoints rather than a separate
//! resource, following GCS and S3 rather than [TUS]. Every request addresses the same object
//! path with the session in the query string, so these handlers have no router of their own:
//! [`ResumableTarget`] classifies the query before thin handlers in [`objects`](super::objects)
//! dispatch the original request here, where each operation runs with its own Axum extractors.
//! Session tokens are encoded as unpadded base64url at this API boundary.
//!
//! | Operation | Request | Success |
//! |---|---|---|
//! | Create | `POST /objects/{usecase}/{scopes}/?upload_type=resumable` | `200` + `{"key","session"}` |
//! | Create | `PUT /objects/{usecase}/{scopes}/{key}?upload_type=resumable` | `200` + `{"key","session"}` |
//! | Chunk | `PUT …/{key}?session=<s>` with `Upload-Offset: <n>` | `204` + `Upload-Offset`, or `201` + `{"key"}` |
//! | Offset query | `PUT …/{key}?session=<s>` with `Upload-Offset: *` | `204` + `Upload-Offset`, or `201` + `{"key"}` |
//! | Cancel | `DELETE …/{key}?session=<s>` | `204` |
//!
//! There is no completion request. The total size is known from session creation, so the
//! backend recognizes the chunk carrying the last byte and commits the object itself.
//!
//! Not every backend supports this. When one declines, session creation answers
//! `501 Not Implemented` and the client performs a regular upload instead.
//!
//! [TUS]: https://tus.io/protocols/resumable-upload

use axum::extract::{Extension, OptionalFromRequestParts, Query, State};
use axum::http::{HeaderMap, StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use axum::{Json, http};
use futures_util::TryStreamExt;
use objectstore_service::error::Error as ServiceError;
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::resumable::{SessionToken, UploadOffset, UploadProgress};
use objectstore_service::stream::ClientStream;
use objectstore_types::metadata::Metadata;
use objectstore_types::resumable::{
    CommitResponse, CreateSessionResponse, HEADER_UPLOAD_LENGTH, HEADER_UPLOAD_OFFSET,
};
use serde::Deserialize;

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiResult};
use crate::extractors::{Xt, body::MeteredBody};
use crate::state::ServiceState;

/// The `upload_type` query parameter.
///
/// Only one value is accepted, so an unrecognized upload type is a deserialization failure
/// and therefore a `400` rather than being silently treated as a regular upload.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum UploadType {
    /// Create a resumable upload session.
    Resumable,
}

/// The resumable protocol's query parameters, as seen on a regular object route.
///
/// Both fields are optional and unknown parameters are ignored, because pre-signed URLs put
/// their own `os_*` parameters into the same query string.
#[derive(Debug, Deserialize)]
struct ResumableQuery {
    /// Present on a session creation request.
    upload_type: Option<UploadType>,
    /// Present on a chunk write, offset query, or cancellation.
    session: Option<SessionToken>,
}

/// Which resumable session a request on an object route targets.
#[derive(Debug)]
pub(super) enum ResumableTarget {
    /// A new session to create for the object addressed by the request.
    NewSession,
    /// An existing session to continue or cancel.
    ExistingSession(SessionToken),
}

impl ResumableQuery {
    /// Classifies a request that may create a session or act on one.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError::Client`] if both parameters are present. They address different
    /// operations, so a request carrying both is ambiguous rather than defaulted.
    fn classify(self) -> ApiResult<Option<ResumableTarget>> {
        match (self.upload_type, self.session) {
            (Some(_), Some(_)) => Err(ApiError::Client(
                "`upload_type` and `session` are mutually exclusive".into(),
            )),
            (Some(UploadType::Resumable), None) => Ok(Some(ResumableTarget::NewSession)),
            (None, Some(session)) => Ok(Some(ResumableTarget::ExistingSession(session))),
            (None, None) => Ok(None),
        }
    }
}

impl<S> OptionalFromRequestParts<S> for ResumableTarget
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> ApiResult<Option<ResumableTarget>> {
        if parts.uri.query().is_none() {
            return Ok(None);
        }

        let Query(query) = Query::<ResumableQuery>::try_from_uri(&parts.uri)
            .map_err(|error| ApiError::Client(error.to_string()))?;

        query.classify()
    }
}

/// Reads the required [`HEADER_UPLOAD_LENGTH`] header.
fn upload_length(headers: &HeaderMap) -> ApiResult<u64> {
    let value = headers
        .get(HEADER_UPLOAD_LENGTH)
        .ok_or_else(|| ApiError::Client(format!("{HEADER_UPLOAD_LENGTH} header is required")))?;

    value
        .to_str()
        .ok()
        .filter(|v| v.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| ApiError::Client(format!("{HEADER_UPLOAD_LENGTH} must be a byte count")))
}

/// Reads the required [`HEADER_UPLOAD_OFFSET`] header.
fn upload_offset(headers: &HeaderMap) -> ApiResult<UploadOffset> {
    let value = headers
        .get(HEADER_UPLOAD_OFFSET)
        .ok_or_else(|| ApiError::Client(format!("{HEADER_UPLOAD_OFFSET} header is required")))?;

    value
        .to_str()
        .map_err(|_| ApiError::Client(format!("{HEADER_UPLOAD_OFFSET} must be ASCII")))?
        .parse()
        .map_err(|e: objectstore_types::resumable::InvalidUploadOffset| {
            ApiError::Client(e.to_string())
        })
}

/// Reads the required `Content-Length` header.
///
/// Chunks declare their length so the server can forward only the prefix a backend accepts
/// without buffering the body to find out how long it is.
fn content_length(headers: &HeaderMap) -> ApiResult<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| ApiError::Client("Content-Length header is required".into()))
}

/// Confirms that a request neither declares nor streams a non-empty body.
async fn require_empty_body(
    headers: &HeaderMap,
    mut body: ClientStream,
    request: &str,
) -> ApiResult<()> {
    if headers.contains_key(http::header::CONTENT_LENGTH) && content_length(headers)? > 0 {
        return Err(ApiError::Client(format!(
            "{request} must be sent with an empty body"
        )));
    }

    while let Some(chunk) = body.try_next().await.map_err(ServiceError::from)? {
        if !chunk.is_empty() {
            return Err(ApiError::Client(format!(
                "{request} must be sent with an empty body"
            )));
        }
    }

    Ok(())
}

/// Creates a session with a server-generated object key.
pub(super) async fn create_session(
    service: AuthAwareService,
    State(state): State<ServiceState>,
    Xt(context): Xt<ObjectContext>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    create_session_for_id(
        service,
        state,
        ObjectId::optional(context, None),
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
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    create_session_for_id(service, state, id, headers, body).await
}

/// Creates a session for the object at `id`.
///
/// Answers `501 Not Implemented` when the backend declines, which tells the client to fall back
/// to a regular upload. Metadata is declared here and does not change afterwards.
async fn create_session_for_id(
    service: AuthAwareService,
    state: ServiceState,
    id: ObjectId,
    headers: HeaderMap,
    body: ClientStream,
) -> ApiResult<Response> {
    let total_length = upload_length(&headers)?;
    require_empty_body(&headers, body, "resumable session creation").await?;
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
pub(super) async fn continue_session(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Extension(session): Extension<SessionToken>,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let offset = upload_offset(&headers)?;
    let key = id.key().to_owned();

    let progress = match offset {
        UploadOffset::At(offset) => {
            let content_length = content_length(&headers)?;
            service
                .put_chunk(id, session, offset, content_length, body)
                .await
        }
        UploadOffset::Unknown => {
            // The wildcard carries no payload. A body would be silently discarded, so
            // reject it rather than let a client believe those bytes were written.
            require_empty_body(&headers, body, "Upload-Offset: *").await?;

            service.upload_offset(id, session).await
        }
    };

    progress_response(progress, key)
}

/// Cancels a session, discarding whatever was uploaded.
pub(super) async fn cancel_session(
    service: AuthAwareService,
    Xt(id): Xt<ObjectId>,
    Extension(session): Extension<SessionToken>,
) -> ApiResult<Response> {
    service.cancel_upload(id, session).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Turns an [`UploadProgress`] outcome into the response shared by chunks and offset queries.
///
/// An offset mismatch is answered here rather than through [`ApiError::status`], because the
/// authoritative offset has to travel in a header that a generic error response cannot set.
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

    fn query(upload_type: Option<UploadType>, session: Option<&str>) -> ResumableQuery {
        ResumableQuery {
            upload_type,
            session: session.map(|s| SessionToken::new(s.into()).unwrap()),
        }
    }

    #[test]
    fn classify_recognizes_each_operation() {
        assert!(matches!(
            query(Some(UploadType::Resumable), None).classify(),
            Ok(Some(ResumableTarget::NewSession))
        ));
        assert!(matches!(
            query(None, Some("token")).classify(),
            Ok(Some(ResumableTarget::ExistingSession(_)))
        ));
        assert!(matches!(query(None, None).classify(), Ok(None)));
    }

    #[test]
    fn classify_rejects_both_parameters() {
        let result = query(Some(UploadType::Resumable), Some("token")).classify();
        assert!(matches!(result, Err(ApiError::Client(_))), "{result:?}");
    }

    #[test]
    fn upload_length_requires_a_byte_count() {
        let mut headers = HeaderMap::new();
        assert!(upload_length(&headers).is_err(), "missing header");

        for invalid in ["", "-1", "+1", "1.5", "abc", " 1"] {
            headers.insert(HEADER_UPLOAD_LENGTH, invalid.parse().unwrap());
            assert!(upload_length(&headers).is_err(), "accepted {invalid:?}");
        }

        headers.insert(HEADER_UPLOAD_LENGTH, "1048576".parse().unwrap());
        assert_eq!(upload_length(&headers).unwrap(), 1_048_576);
    }

    #[test]
    fn upload_offset_parses_chunk_and_wildcard() {
        let mut headers = HeaderMap::new();
        assert!(upload_offset(&headers).is_err(), "missing header");

        headers.insert(HEADER_UPLOAD_OFFSET, "*".parse().unwrap());
        assert_eq!(upload_offset(&headers).unwrap(), UploadOffset::Unknown);

        headers.insert(HEADER_UPLOAD_OFFSET, "262144".parse().unwrap());
        assert_eq!(upload_offset(&headers).unwrap(), UploadOffset::At(262_144));

        headers.insert(HEADER_UPLOAD_OFFSET, "nope".parse().unwrap());
        assert!(upload_offset(&headers).is_err());
    }

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

        let invalid = ApiError::Service(ServiceError::InvalidUploadRequest("bad".into()));
        let error = progress_response(Err(invalid), "my-key".into()).unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }
}
