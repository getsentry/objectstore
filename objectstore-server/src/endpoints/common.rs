//! Common types and utilities for API endpoints.

use std::error::Error;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use http::HeaderValue;
use objectstore_service::error::Error as ServiceError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::AuthError;
use crate::extractors::batch::BatchError;

/// Error type for API operations.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Errors indicating malformed or illegal requests.
    #[error("client error: {0}")]
    Client(String),

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
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for API operations.
pub type ApiResult<T> = Result<T, ApiError>;

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

impl ApiError {
    /// Returns the HTTP status code appropriate for this error variant.
    pub fn status(&self) -> StatusCode {
        match &self {
            ApiError::Client(_) => StatusCode::BAD_REQUEST,

            ApiError::Batch(BatchError::BadRequest(_))
            | ApiError::Batch(BatchError::Metadata(_))
            | ApiError::Batch(BatchError::Multipart(_)) => StatusCode::BAD_REQUEST,
            ApiError::Batch(BatchError::LimitExceeded(_)) => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::Batch(BatchError::RateLimited) => StatusCode::TOO_MANY_REQUESTS,
            ApiError::Batch(BatchError::ResponseSerialization { .. }) => {
                objectstore_log::error!(!!self, "error serializing batch response");
                StatusCode::INTERNAL_SERVER_ERROR
            }

            ApiError::Auth(AuthError::BadRequest(_)) => StatusCode::BAD_REQUEST,
            ApiError::Auth(AuthError::ValidationFailure(_))
            | ApiError::Auth(AuthError::VerificationFailure) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::UnknownKey) => StatusCode::UNAUTHORIZED,
            ApiError::Auth(AuthError::UnsupportedPresignedMethod) => StatusCode::FORBIDDEN,
            ApiError::Auth(AuthError::NotPermitted) => StatusCode::FORBIDDEN,
            ApiError::Auth(AuthError::InternalError(_)) => {
                objectstore_log::error!(!!self, "auth system error");
                StatusCode::INTERNAL_SERVER_ERROR
            }

            ApiError::Service(ServiceError::Client(_)) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::Metadata(_)) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::RangeNotSatisfiable { .. }) => {
                StatusCode::RANGE_NOT_SATISFIABLE
            }
            ApiError::Service(ServiceError::InvalidUploadId(_)) => StatusCode::BAD_REQUEST,
            ApiError::Service(ServiceError::AtCapacity) => StatusCode::TOO_MANY_REQUESTS,
            ApiError::Service(ServiceError::NotImplemented) => StatusCode::NOT_IMPLEMENTED,
            ApiError::Service(_) => {
                objectstore_log::error!(!!self, "error handling request");
                StatusCode::INTERNAL_SERVER_ERROR
            }

            ApiError::Internal(_) => {
                objectstore_log::error!(!!self, "internal error");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ApiErrorResponse::from_error(&self);
        (self.status(), Json(body)).into_response()
    }
}

/// Inserts `Accept-Ranges: bytes` into the response headers.
pub fn insert_accept_ranges(response: &mut Response) {
    response.headers_mut().insert(
        http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
}
