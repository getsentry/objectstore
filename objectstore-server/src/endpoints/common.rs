//! Common types and utilities for API endpoints.

use std::borrow::Cow;
use std::convert::Infallible;
use std::error::Error;
use std::fmt::Write as _;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, IntoResponseParts, Response, ResponseParts};
use http::{HeaderValue, header};
use objectstore_service::error::{Error as ServiceError, ErrorKind as ServiceErrorKind};
use objectstore_types::headers::ExtValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::AuthError;
use crate::extractors::batch::BatchError;
use crate::extractors::response_options::ContentDisposition;

/// A JSON error response returned by the API.
#[derive(Serialize, Deserialize, Debug)]
pub struct ApiErrorResponse {
    /// The main error message.
    #[serde(default)]
    detail: Option<String>,
    /// Chain of error causes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    causes: Vec<String>,
}

impl ApiErrorResponse {
    /// Creates an error response from an error, extracting the full cause chain.
    pub fn from_error<E: Error + ?Sized>(error: &E) -> Self {
        let detail = Some(error.to_string());

        let mut causes = Vec::new();
        let mut source = error.source();
        while let Some(s) = source {
            causes.push(s.to_string());
            source = s.source();
        }

        Self { detail, causes }
    }
}

/// Error type for API operations.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Errors indicating malformed or illegal requests.
    #[error("client error: {context}")]
    Client {
        /// Context describing the operation that failed.
        context: Cow<'static, str>,
        /// The underlying error, if available.
        #[source]
        cause: Option<Box<dyn Error + Send + Sync>>,
    },

    /// The requested operation was valid but could not be satisfied.
    #[error("{0}")]
    Conflict(Cow<'static, str>),

    /// Authorization/authentication errors.
    #[error("auth error: {0}")]
    Auth(#[from] AuthError),

    /// Service errors, indicating that something went wrong when receiving or executing a request.
    #[error("service error: {0}")]
    Service(#[from] ServiceError),

    /// Errors encountered when parsing or executing a batch request.
    #[error("batch error: {0}")]
    Batch(#[from] BatchError),

    /// Internal server errors.
    #[error("internal error: {context}")]
    Internal {
        /// Context describing the operation that failed.
        context: Cow<'static, str>,
        /// The underlying error, if available.
        #[source]
        cause: Option<Box<dyn Error + Send + Sync>>,
    },
}

impl ApiError {
    /// Creates a client error with context and an underlying cause.
    pub fn map_client<E>(context: impl Into<Cow<'static, str>>, cause: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Client {
            context: context.into(),
            cause: Some(Box::new(cause)),
        }
    }

    /// Creates a client error with context and no underlying cause.
    pub fn client(context: impl Into<Cow<'static, str>>) -> Self {
        Self::Client {
            context: context.into(),
            cause: None,
        }
    }

    /// Creates a conflict error with a client-safe explanation.
    pub fn conflict(context: impl Into<Cow<'static, str>>) -> Self {
        Self::Conflict(context.into())
    }

    /// Creates an internal server error with context and an underlying cause.
    pub fn internal<E>(context: impl Into<Cow<'static, str>>, cause: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Internal {
            context: context.into(),
            cause: Some(Box::new(cause)),
        }
    }

    /// Returns the HTTP status code appropriate for this error variant.
    pub fn status(&self) -> StatusCode {
        match &self {
            ApiError::Client { .. } => StatusCode::BAD_REQUEST,
            ApiError::Conflict(_) => StatusCode::CONFLICT,

            ApiError::Batch(BatchError::BadRequest(_))
            | ApiError::Batch(BatchError::Metadata(_))
            | ApiError::Batch(BatchError::Multipart(_)) => StatusCode::BAD_REQUEST,
            ApiError::Batch(BatchError::LimitExceeded(_)) => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Batch(BatchError::RateLimited) => StatusCode::TOO_MANY_REQUESTS,
            ApiError::Batch(BatchError::ResponseSerialization { .. }) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }

            ApiError::Auth(AuthError::BadRequest(_)) => StatusCode::BAD_REQUEST,
            ApiError::Auth(AuthError::ValidationFailure(_))
            | ApiError::Auth(AuthError::VerificationFailure) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::UnknownKey) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::UnsupportedPresignedMethod) => StatusCode::FORBIDDEN,
            ApiError::Auth(AuthError::NotPermitted) => StatusCode::FORBIDDEN,
            ApiError::Auth(AuthError::InternalError(_)) => StatusCode::INTERNAL_SERVER_ERROR,

            ApiError::Service(error) => match error.kind() {
                ServiceErrorKind::InvalidMetadata
                | ServiceErrorKind::InvalidUploadId
                | ServiceErrorKind::InvalidUploadSessionToken
                | ServiceErrorKind::ClientStream
                | ServiceErrorKind::ChunkExceedsUploadLength { .. }
                | ServiceErrorKind::ChunkTooSmall { .. } => StatusCode::BAD_REQUEST,
                ServiceErrorKind::UnknownUploadSession => StatusCode::NOT_FOUND,
                ServiceErrorKind::RangeNotSatisfiable { .. } => StatusCode::RANGE_NOT_SATISFIABLE,
                ServiceErrorKind::UploadOffsetMismatch { .. } => StatusCode::CONFLICT,
                ServiceErrorKind::UploadSessionGone => StatusCode::GONE,
                ServiceErrorKind::AtCapacity => StatusCode::TOO_MANY_REQUESTS,
                ServiceErrorKind::Unsupported => StatusCode::NOT_IMPLEMENTED,
                ServiceErrorKind::BackendRateLimited => StatusCode::TOO_MANY_REQUESTS,
                ServiceErrorKind::BackendTimeout | ServiceErrorKind::BackendUnavailable => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                ServiceErrorKind::BackendFailure
                | ServiceErrorKind::CorruptData
                | ServiceErrorKind::Panic
                | ServiceErrorKind::UnexpectedTombstone
                | ServiceErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            },

            ApiError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Reports this error to error tracking if it indicates a server fault (5xx status).
    ///
    /// Call this exactly once wherever an `ApiError` is serialized into a client-visible
    /// response: standalone responses ([`IntoResponse`]) and batch response parts.
    pub fn capture(&self) {
        // Captured at the source in the service layer to prevent double-logging.
        if matches!(self, ApiError::Service(_)) {
            return;
        }

        if self.status().is_server_error() {
            objectstore_log::error!(!!self, "error handling request");
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        self.capture();
        let body = ApiErrorResponse::from_error(&self);
        (self.status(), Json(body)).into_response()
    }
}

impl From<crate::usecases::UseCaseError> for ApiError {
    fn from(error: crate::usecases::UseCaseError) -> Self {
        ApiError::map_client("use case policy violation", error)
    }
}

impl From<objectstore_types::metadata::Error> for ApiError {
    fn from(error: objectstore_types::metadata::Error) -> Self {
        ApiError::map_client("invalid metadata", error)
    }
}

/// Result type for API operations.
pub type ApiResult<T> = Result<T, ApiError>;

/// Inserts `Accept-Ranges: bytes` into the response headers.
pub fn insert_accept_ranges(response: &mut Response) {
    response.headers_mut().insert(
        http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
}

/// Object presentation headers, including mandatory browser safeguards.
///
/// All successful downloads get `nosniff`; inline responses also get a CSP sandbox without
/// relaxation tokens. Sandboxing is a browser safeguard, not content sanitization, and does not
/// prevent all external resource loads. Apply these parts only to successful object responses.
#[derive(Debug)]
pub(crate) struct DownloadHeaders {
    disposition: HeaderValue,
    inline: bool,
}

impl DownloadHeaders {
    /// Builds success-only headers using the selected disposition and stored filename.
    pub fn new(disposition: ContentDisposition, filename: Option<&str>) -> Self {
        let header = match filename {
            Some(filename) => format_content_disposition(disposition, filename),
            None => HeaderValue::from_static(disposition.as_str()),
        };
        Self {
            disposition: header,
            inline: disposition == ContentDisposition::Inline,
        }
    }
}

impl IntoResponseParts for DownloadHeaders {
    type Error = Infallible;

    fn into_response_parts(
        self,
        mut response: ResponseParts,
    ) -> Result<ResponseParts, Self::Error> {
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_DISPOSITION, self.disposition);
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        if self.inline {
            headers.insert(
                header::CONTENT_SECURITY_POLICY,
                HeaderValue::from_static("sandbox"),
            );
        }
        Ok(response)
    }
}

/// Formats the selected disposition with a stored filename.
///
/// The filename is sanitized (`/` and `\` become `-`, dots-only names become all dashes, non-ASCII
/// and control characters become `_`) and then escaped for the RFC 6266 quoted-string (`"` is
/// backslash-escaped).
///
/// A filename that is not pure ASCII cannot be represented in that quoted-string, so it
/// additionally gets an RFC 8187 `filename*` parameter carrying the full UTF-8 value. The
/// quoted-string then serves as the ASCII fallback for clients that ignore `filename*`.
fn format_content_disposition(disposition: ContentDisposition, filename: &str) -> HeaderValue {
    let all_dots = filename.chars().all(|c| c == '.');

    let mut result = format!("{}; filename=\"", disposition.as_str());
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
    HeaderValue::from_str(&result).expect("content disposition is a valid header value")
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;

    use crate::endpoints::common::DownloadHeaders;
    use crate::extractors::response_options::ContentDisposition;

    #[test]
    fn inline_filename_retains_escaping_and_unicode_encoding() {
        let response = (
            DownloadHeaders::new(ContentDisposition::Inline, Some("réport.pdf")),
            (),
        )
            .into_response();
        assert_eq!(
            response.headers()["content-disposition"],
            "inline; filename=\"r_port.pdf\"; filename*=UTF-8''r%C3%A9port.pdf"
        );
        assert_eq!(response.headers()["content-security-policy"], "sandbox");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    }
}
