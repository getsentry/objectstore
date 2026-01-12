use thiserror::Error;

use crate::backend::common::BackendError;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("backend error: {0}")]
    Backend(#[from] BackendError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
