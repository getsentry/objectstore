//! Common types for endpoint handlers.

use crate::error::RequestError;

/// Type alias for endpoint results.
pub type ApiResult<T> = std::result::Result<T, RequestError>;
