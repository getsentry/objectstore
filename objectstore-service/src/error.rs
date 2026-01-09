//! Error types for the storage service and backend layers.

use thiserror::Error;

/// Errors that can occur in storage backend operations.
#[derive(Debug, Error)]
pub enum BackendError {
    /// Network or HTTP request failed.
    #[error("network error: {message}")]
    Network {
        /// Error message.
        message: String,
        /// Underlying cause.
        #[source]
        cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Failed to encode data for storage.
    #[error("encoding error: {message}")]
    Encoding {
        /// Error message.
        message: String,
        /// Underlying cause.
        #[source]
        cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Failed to decode data from storage.
    #[error("decoding error: {message}")]
    Decoding {
        /// Error message.
        message: String,
        /// Underlying cause.
        #[source]
        cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Backend returned unexpected response.
    #[error("invalid response: {message}")]
    InvalidResponse {
        /// Error message.
        message: String,
        /// HTTP status code if available.
        status_code: Option<u16>,
    },

    /// I/O error (filesystem operations).
    #[error("I/O error")]
    Io(#[from] std::io::Error),
}

impl BackendError {
    /// Creates a network error with a message and optional cause.
    pub fn network(message: impl Into<String>) -> Self {
        Self::Network {
            message: message.into(),
            cause: None,
        }
    }

    /// Creates a network error with a message and cause.
    pub fn network_with_cause(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Network {
            message: message.into(),
            cause: Some(Box::new(cause)),
        }
    }

    /// Creates an encoding error with a message and optional cause.
    pub fn encoding(message: impl Into<String>) -> Self {
        Self::Encoding {
            message: message.into(),
            cause: None,
        }
    }

    /// Creates an encoding error with a message and cause.
    pub fn encoding_with_cause(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Encoding {
            message: message.into(),
            cause: Some(Box::new(cause)),
        }
    }

    /// Creates a decoding error with a message and optional cause.
    pub fn decoding(message: impl Into<String>) -> Self {
        Self::Decoding {
            message: message.into(),
            cause: None,
        }
    }

    /// Creates a decoding error with a message and cause.
    pub fn decoding_with_cause(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Decoding {
            message: message.into(),
            cause: Some(Box::new(cause)),
        }
    }

    /// Creates an invalid response error with a message and optional status code.
    pub fn invalid_response(message: impl Into<String>, status_code: Option<u16>) -> Self {
        Self::InvalidResponse {
            message: message.into(),
            status_code,
        }
    }
}

/// Errors that can occur in storage service operations.
#[derive(Debug, Error)]
pub enum ServiceError {
    /// Backend operation failed.
    #[error("backend error")]
    Backend(#[from] BackendError),

    /// Failed to read request body stream.
    #[error("stream read error")]
    StreamRead(#[source] std::io::Error),

    /// Internal service error.
    #[error("internal error: {message}")]
    Internal {
        /// Error message.
        message: String,
    },
}

impl ServiceError {
    /// Creates an internal error with a message.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}
