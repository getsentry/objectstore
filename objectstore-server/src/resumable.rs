//! Request extractors and validation helpers for resumable upload endpoints.

use axum::extract::{FromRequestParts, OptionalFromRequestParts, Query};
use axum::http::{HeaderName, HeaderValue, request::Parts};
use axum_extra::headers::{ContentLength, Error as HeaderError, Header};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::TryStreamExt;
use objectstore_service::error::Error as ServiceError;
use objectstore_service::stream::ClientStream;
use objectstore_types::resumable::{
    HEADER_UPLOAD_LENGTH, HEADER_UPLOAD_OFFSET, SessionToken, UploadOffset,
};
use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::endpoints::common::{ApiError, ApiResult};

/// The `upload_type` query parameter.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum UploadType {
    /// Create a resumable upload session.
    Resumable,
}

/// The resumable protocol's query parameters, as seen on a regular object route.
#[derive(Debug, Deserialize)]
struct ResumableQuery {
    /// Present on a session creation request.
    upload_type: Option<UploadType>,
    /// Present on a chunk write, offset query, or cancellation; its value is decoded by the
    /// selected endpoint handler.
    session: Option<IgnoredAny>,
}

impl ResumableQuery {
    /// Classifies a request that may create a session or act on one.
    fn classify(self) -> Option<ResumableTarget> {
        match (self.upload_type, self.session) {
            // A session unambiguously selects an existing upload, so a redundant upload_type can
            // be ignored rather than making the request ambiguous.
            (_, Some(_)) => Some(ResumableTarget::ExistingSession),
            (Some(UploadType::Resumable), None) => Some(ResumableTarget::NewSession),
            (None, None) => None,
        }
    }
}

/// Which resumable session a request on an object route targets.
#[derive(Debug)]
pub(crate) enum ResumableTarget {
    /// A new session to create for the object addressed by the request.
    NewSession,
    /// An existing session to continue or cancel.
    ExistingSession,
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

        Ok(query.classify())
    }
}

/// A session token decoded by a continuation or cancellation handler.
#[derive(Debug)]
pub(crate) struct Session(pub(crate) SessionToken);

#[derive(Debug, Deserialize)]
struct SessionQuery {
    session: String,
}

impl<S> FromRequestParts<S> for Session
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> ApiResult<Session> {
        let Query(SessionQuery { session }) = Query::<SessionQuery>::try_from_uri(&parts.uri)
            .map_err(|error| ApiError::Client(error.to_string()))?;
        Ok(Session(decode_session_token(&session)?))
    }
}

/// The typed [`HEADER_UPLOAD_LENGTH`] request header.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UploadLengthHeader(pub(crate) u64);

impl Header for UploadLengthHeader {
    fn name() -> &'static HeaderName {
        static NAME: HeaderName = HeaderName::from_static(HEADER_UPLOAD_LENGTH);
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, HeaderError>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = decode_single_value(values)?;
        let value = value.to_str().map_err(|_| HeaderError::invalid())?;

        if !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(HeaderError::invalid());
        }

        value.parse().map(Self).map_err(|_| HeaderError::invalid())
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        values.extend(std::iter::once(HeaderValue::from(self.0)));
    }
}

/// The typed [`HEADER_UPLOAD_OFFSET`] request header.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UploadOffsetHeader(pub(crate) UploadOffset);

impl Header for UploadOffsetHeader {
    fn name() -> &'static HeaderName {
        static NAME: HeaderName = HeaderName::from_static(HEADER_UPLOAD_OFFSET);
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, HeaderError>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        decode_single_value(values)?
            .to_str()
            .map_err(|_| HeaderError::invalid())?
            .parse()
            .map(Self)
            .map_err(|_| HeaderError::invalid())
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        let value = match self.0 {
            UploadOffset::At(offset) => HeaderValue::from(offset),
            UploadOffset::Unknown => HeaderValue::from_static("*"),
        };
        values.extend(std::iter::once(value));
    }
}

fn decode_single_value<'i, I>(values: &mut I) -> Result<&'i HeaderValue, HeaderError>
where
    I: Iterator<Item = &'i HeaderValue>,
{
    let value = values.next().ok_or_else(HeaderError::invalid)?;
    if values.next().is_some() {
        return Err(HeaderError::invalid());
    }
    Ok(value)
}

fn decode_session_token(encoded: &str) -> ApiResult<SessionToken> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|error| ApiError::Client(error.to_string()))?;

    if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
        return Err(ApiError::Client(
            "session token must use unpadded base64url encoding".into(),
        ));
    }

    String::from_utf8(bytes)
        .map(SessionToken::from)
        .map_err(|error| ApiError::Client(error.to_string()))
}

/// Confirms that a request neither declares nor streams a non-empty body.
pub(crate) async fn require_empty_body(
    content_length: Option<ContentLength>,
    mut body: ClientStream,
    request: &str,
) -> ApiResult<()> {
    if content_length.is_some_and(|ContentLength(length)| length > 0) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    fn query(upload_type: Option<UploadType>, has_session: bool) -> ResumableQuery {
        ResumableQuery {
            upload_type,
            session: has_session.then_some(IgnoredAny),
        }
    }

    fn decode_header<H: Header>(headers: &HeaderMap) -> Result<H, HeaderError> {
        H::decode(&mut headers.get_all(H::name()).iter())
    }

    #[test]
    fn classify_recognizes_each_operation() {
        assert!(matches!(
            query(Some(UploadType::Resumable), false).classify(),
            Some(ResumableTarget::NewSession)
        ));
        assert!(matches!(
            query(None, true).classify(),
            Some(ResumableTarget::ExistingSession)
        ));
        assert!(query(None, false).classify().is_none());
    }

    #[test]
    fn classify_prefers_session_over_upload_type() {
        assert!(matches!(
            query(Some(UploadType::Resumable), true).classify(),
            Some(ResumableTarget::ExistingSession)
        ));
    }

    #[test]
    fn session_token_decodes_from_unpadded_base64url() {
        assert_eq!(
            decode_session_token("Li4vZXNjYXBl").unwrap().as_ref(),
            "../escape"
        );
    }

    #[test]
    fn session_token_rejects_invalid_query_encodings() {
        for invalid in ["%%%", "dG9rM24=", "_w"] {
            assert!(
                decode_session_token(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn upload_length_requires_a_byte_count() {
        let mut headers = HeaderMap::new();
        assert!(
            decode_header::<UploadLengthHeader>(&headers).is_err(),
            "missing header"
        );

        for invalid in ["", "-1", "+1", "1.5", "abc", " 1"] {
            headers.insert(HEADER_UPLOAD_LENGTH, invalid.parse().unwrap());
            assert!(
                decode_header::<UploadLengthHeader>(&headers).is_err(),
                "accepted {invalid:?}"
            );
        }

        headers.insert(HEADER_UPLOAD_LENGTH, "1048576".parse().unwrap());
        assert_eq!(
            decode_header::<UploadLengthHeader>(&headers).unwrap().0,
            1_048_576
        );
    }

    #[test]
    fn upload_offset_parses_chunk_and_wildcard() {
        let mut headers = HeaderMap::new();
        assert!(
            decode_header::<UploadOffsetHeader>(&headers).is_err(),
            "missing header"
        );

        headers.insert(HEADER_UPLOAD_OFFSET, "*".parse().unwrap());
        assert_eq!(
            decode_header::<UploadOffsetHeader>(&headers).unwrap().0,
            UploadOffset::Unknown
        );

        headers.insert(HEADER_UPLOAD_OFFSET, "262144".parse().unwrap());
        assert_eq!(
            decode_header::<UploadOffsetHeader>(&headers).unwrap().0,
            UploadOffset::At(262_144)
        );

        headers.insert(HEADER_UPLOAD_OFFSET, "nope".parse().unwrap());
        assert!(decode_header::<UploadOffsetHeader>(&headers).is_err());
    }
}
