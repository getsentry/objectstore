use thiserror::Error;

use crate::backend::common::BackendError;

/// Errors that can occur in the storage service.
#[derive(Debug, Error)]
pub enum ServiceError {
    /// An error from the storage backend.
    #[error("backend error: {0}")]
    Backend(#[from] BackendError),

    /// An I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
