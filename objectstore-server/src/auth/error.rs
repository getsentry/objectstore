use objectstore_types::auth::Permission;
use thiserror::Error;

/// Error type for different authorization failure scenarios.
#[derive(Error, Debug, PartialEq)]
pub enum AuthError {
    /// Indicates that something about the request prevented authorization verification from
    /// happening properly.
    #[error("bad request: {0}")]
    BadRequest(&'static str),

    /// Indicates that something about Objectstore prevented authorization verification from
    /// happening properly.
    #[error("internal error: {0}")]
    InternalError(String),

    /// Indicates that the provided authorization token is invalid (e.g. expired or malformed).
    #[error("failed to decode token: {0}")]
    ValidationFailure(#[from] jsonwebtoken::errors::Error),

    /// Indicates that an otherwise-valid token was unable to be verified with configured keys.
    #[error("failed to verify token")]
    VerificationFailure,

    /// Indicates that the requested operation is not authorized and auth enforcement is enabled.
    #[error("operation not allowed")]
    NotPermitted,
}

impl AuthError {
    /// Return a shortname for the failure reason that can be used to tag metrics.
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::InternalError(_) => "internal_error",
            Self::ValidationFailure(_) => "validation_failure",
            Self::VerificationFailure => "verification_failure",
            Self::NotPermitted => "not_permitted",
        }
    }

    /// Increment a counter and emit a debug log for this auth failure.
    ///
    /// If `enforce` is false, authentication failures will be logged as warnings to ensure they
    /// are found and fixed to unblock enabling enforcement.
    pub fn log(&self, permission: Option<Permission>, usecase: Option<&str>, enforce: bool) {
        let code = self.code();
        objectstore_metrics::count!("server.auth.failure", code = code);
        let msg = self.to_string();
        if !enforce {
            objectstore_log::warn!(?permission, ?usecase, ?code, ?msg, "Auth failure");
        } else {
            objectstore_log::debug!(?permission, ?usecase, ?code, ?msg, "Auth failure");
        }
    }
}
