//! Resumable upload endpoints.
//!
//! Resumable uploads are a variation of the regular object endpoints rather than a separate
//! resource, following GCS and S3 rather than [TUS]. Every request addresses the same object
//! path with the session in the query string, so these handlers have no router of their own:
//! [`objects`](super::objects) dispatches to them based on [`ResumableQuery`].
//!
//! | Operation | Request | Success |
//! |---|---|---|
//! | Create | `POST /objects/{usecase}/{scopes}/?upload_type=resumable` | `200` + `Location` + `{"key","session"}` |
//! | Create | `PUT /objects/{usecase}/{scopes}/{key}?upload_type=resumable` | `200` + `Location` + `{"key","session"}` |
//! | Chunk | `PUT …/{key}?session=<s>` with `Upload-Offset: <n>` | `204` + `Upload-Offset`, or `201` + `{"key"}` |
//! | Offset query | `PUT …/{key}?session=<s>` with `Upload-Offset: *` | `204` + `Upload-Offset`, or `201` + `{"key"}` |
//! | Terminate | `DELETE …/{key}?session=<s>` | `204` |
//!
//! There is no completion request. The total size is known from session creation, so the
//! backend recognizes the chunk carrying the last byte and commits the object itself.
//!
//! Not every backend supports this. When one declines, session creation answers
//! `409 Conflict` and the client performs a regular upload instead.
//!
//! [TUS]: https://tus.io/protocols/resumable-upload

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, http};
use objectstore_service::error::Error as ServiceError;
use objectstore_service::id::ObjectId;
use objectstore_service::resumable::{SessionToken, UploadOffset, UploadProgress};
use objectstore_types::metadata::Metadata;
use objectstore_types::resumable::{
    CommitResponse, CreateSessionResponse, HEADER_UPLOAD_LENGTH, HEADER_UPLOAD_OFFSET,
};
use serde::Deserialize;

use crate::auth::AuthAwareService;
use crate::endpoints::common::{ApiError, ApiErrorResponse, ApiResult};
use crate::extractors::body::MeteredBody;
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
#[derive(Debug, Default, Deserialize)]
pub(super) struct ResumableQuery {
    /// Present on a session creation request.
    upload_type: Option<UploadType>,
    /// Present on a chunk write, offset query, or termination.
    session: Option<SessionToken>,
}

/// What a request on an object route is addressing.
#[derive(Debug)]
pub(super) enum ResumableRoute {
    /// Create a session for the object named by the request path.
    Create,
    /// Act on the identified session: write a chunk, query the offset, or terminate.
    Session(SessionToken),
    /// A regular object request that does not involve the resumable protocol.
    Regular,
}

impl ResumableQuery {
    /// Classifies a request that may create a session or act on one.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError::Client`] if both parameters are present. They address different
    /// operations, so a request carrying both is ambiguous rather than defaulted.
    pub fn classify(self) -> ApiResult<ResumableRoute> {
        match (self.upload_type, self.session) {
            (Some(_), Some(_)) => Err(ApiError::Client(
                "`upload_type` and `session` are mutually exclusive".into(),
            )),
            (Some(UploadType::Resumable), None) => Ok(ResumableRoute::Create),
            (None, Some(session)) => Ok(ResumableRoute::Session(session)),
            (None, None) => Ok(ResumableRoute::Regular),
        }
    }

    /// Classifies a request that may only act on an existing session.
    ///
    /// Used by routes where session creation is not defined: `DELETE`, which terminates, and
    /// the collection `POST`, whose generated key is only known once a session exists.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError::Client`] if `upload_type` is present.
    pub fn classify_session_only(self, operation: &str) -> ApiResult<ResumableRoute> {
        if self.upload_type.is_some() {
            return Err(ApiError::Client(format!(
                "`upload_type` is not supported on {operation}"
            )));
        }

        match self.session {
            Some(session) => Ok(ResumableRoute::Session(session)),
            None => Ok(ResumableRoute::Regular),
        }
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

/// How the request path relates to the object a session is being created for.
///
/// Needed to build the `Location` header, which must always name the object even when the
/// request did not.
#[derive(Clone, Copy, Debug)]
pub(super) enum RequestPath {
    /// The request path names the object, as on a `PUT` to the object route.
    Object,
    /// The request path is the collection the object lives in, as on a `POST` whose key was
    /// generated by the server and therefore does not appear in the path.
    Collection,
}

/// Creates a session for the object at `id`.
///
/// Answers `409 Conflict` when the backend declines, which tells the client to fall back to a
/// regular upload. Metadata is declared here and does not change afterwards.
pub(super) async fn create_session(
    service: AuthAwareService,
    state: ServiceState,
    request_path: RequestPath,
    uri_path: &str,
    id: ObjectId,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let total_length = upload_length(&headers)?;
    let metadata = Metadata::from_insert_headers(&headers, "").map_err(ServiceError::from)?;

    state
        .config
        .usecases
        .validate(&id.context().usecase, &metadata)
        .map_err(|e| ApiError::Client(e.to_string()))?;

    let Some(session) = service
        .create_upload_session(id.clone(), metadata, total_length)
        .await?
    else {
        let body = ApiErrorResponse::message("resumable uploads are unavailable for this object");
        return Ok((StatusCode::CONFLICT, Json(body)).into_response());
    };

    let mut headers = HeaderMap::new();
    if let Some(location) = session_location(uri_path, request_path, id.key(), &session) {
        headers.insert(http::header::LOCATION, location);
    }

    let body = Json(CreateSessionResponse {
        key: id.key().to_owned(),
        session,
    });
    Ok((StatusCode::OK, headers, body).into_response())
}

/// Builds the `Location` header pointing at the session.
///
/// The value is the object path with the session appended, so a client that persisted the key
/// and the session can rebuild it without having stored a URL. `Router::nest` strips the `/v1`
/// prefix from the request URI, so callers pass the path from
/// [`OriginalUri`](axum::extract::OriginalUri) — and on the collection route that path does
/// not name the object, so the key is appended to it.
///
/// Returns `None` if the result is not a valid header value, in which case the header is
/// omitted: it is a convenience, and the response body carries the same information.
fn session_location(
    uri_path: &str,
    request_path: RequestPath,
    key: &str,
    session: &SessionToken,
) -> Option<http::HeaderValue> {
    let object_path = match request_path {
        RequestPath::Object => uri_path.to_owned(),
        RequestPath::Collection => format!("{}/{key}", uri_path.trim_end_matches('/')),
    };

    http::HeaderValue::from_str(&format!("{object_path}?session={session}")).ok()
}

/// Acts on an open session: writes a chunk, or reports the offset the server holds.
///
/// [`HEADER_UPLOAD_OFFSET`] selects between the two. A concrete offset submits the request
/// body as the chunk starting there; the `*` wildcard submits nothing and asks where the
/// server stands, which also commits an object that was assembled but not yet committed.
///
/// Both answer `204 No Content` with the authoritative offset while bytes remain, and
/// `201 Created` with the key once the object is committed.
pub(super) async fn session_request(
    service: AuthAwareService,
    id: ObjectId,
    session: SessionToken,
    headers: HeaderMap,
    MeteredBody(body): MeteredBody,
) -> ApiResult<Response> {
    let offset = upload_offset(&headers)?;
    let content_length = content_length(&headers)?;
    let key = id.key().to_owned();

    let progress = match offset {
        UploadOffset::At(offset) => {
            service
                .put_chunk(id, session, offset, content_length, body)
                .await
        }
        UploadOffset::Unknown => {
            // The wildcard carries no payload. A body would be silently discarded, so
            // reject it rather than let a client believe those bytes were written.
            if content_length != 0 {
                return Err(ApiError::Client(format!(
                    "{HEADER_UPLOAD_OFFSET}: * must be sent with an empty body"
                )));
            }

            service.upload_offset(id, session).await
        }
    };

    progress_response(progress, key)
}

/// Terminates a session, discarding whatever was uploaded.
pub(super) async fn terminate(
    service: AuthAwareService,
    id: ObjectId,
    session: SessionToken,
) -> ApiResult<Response> {
    service.terminate_upload(id, session).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Turns an [`UploadProgress`] outcome into the response shared by chunks and offset queries.
///
/// An offset mismatch is answered here rather than through [`ApiError::status`], because the
/// authoritative offset has to travel in a header that a generic error response cannot set.
fn progress_response(progress: ApiResult<UploadProgress>, key: String) -> ApiResult<Response> {
    let progress = match progress {
        Ok(progress) => progress,
        Err(ApiError::Service(ServiceError::UploadOffsetMismatch { offset })) => {
            let body = ApiErrorResponse::message(format!("expected offset {offset}"));
            let response = (
                StatusCode::CONFLICT,
                [(HEADER_UPLOAD_OFFSET, http::HeaderValue::from(offset))],
                Json(body),
            );
            return Ok(response.into_response());
        }
        Err(e) => return Err(e),
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
            Ok(ResumableRoute::Create)
        ));
        assert!(matches!(
            query(None, Some("token")).classify(),
            Ok(ResumableRoute::Session(_))
        ));
        assert!(matches!(
            query(None, None).classify(),
            Ok(ResumableRoute::Regular)
        ));
    }

    #[test]
    fn classify_rejects_both_parameters() {
        let result = query(Some(UploadType::Resumable), Some("token")).classify();
        assert!(matches!(result, Err(ApiError::Client(_))), "{result:?}");
    }

    #[test]
    fn classify_session_only_rejects_upload_type() {
        let result = query(Some(UploadType::Resumable), None).classify_session_only("DELETE");
        assert!(matches!(result, Err(ApiError::Client(_))), "{result:?}");

        assert!(matches!(
            query(None, Some("token")).classify_session_only("DELETE"),
            Ok(ResumableRoute::Session(_))
        ));
        assert!(matches!(
            query(None, None).classify_session_only("DELETE"),
            Ok(ResumableRoute::Regular)
        ));
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

    #[test]
    fn session_location_appends_the_session_to_the_object_path() {
        let session = SessionToken::new("tok3n".into()).unwrap();
        let location = session_location(
            "/v1/objects/testing/org=1/my-key",
            RequestPath::Object,
            "my-key",
            &session,
        );

        assert_eq!(
            location.unwrap(),
            "/v1/objects/testing/org=1/my-key?session=tok3n"
        );
    }

    #[test]
    fn session_location_appends_a_generated_key_to_the_collection_path() {
        let session = SessionToken::new("tok3n".into()).unwrap();

        // The `POST` route matches with and without a trailing slash, and the generated key
        // never appears in the request path — so it has to be appended either way.
        for collection in ["/v1/objects/testing/org=1/", "/v1/objects/testing/org=1"] {
            let location =
                session_location(collection, RequestPath::Collection, "generated", &session);

            assert_eq!(
                location.unwrap(),
                "/v1/objects/testing/org=1/generated?session=tok3n",
                "for request path {collection:?}"
            );
        }
    }
}
