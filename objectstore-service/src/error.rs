//! Error types for service and backend operations.
//!
//! [`Error`] covers I/O, serialization, HTTP, metadata, authentication,
//! and backend-specific failures. [`Result`] is the corresponding alias.

use thiserror::Error as ThisError;

/// Error type for service operations.
#[derive(Debug, ThisError)]
pub enum Error {
    /// IO errors related to payload streaming or file operations.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Errors related to de/serialization.
    #[error("serde error: {context}")]
    Serde {
        /// Context describing what was being serialized/deserialized.
        context: String,
        /// The underlying serde error.
        #[source]
        cause: serde_json::Error,
    },

    /// All errors stemming from the reqwest client, used in multiple backends to send requests to
    /// e.g. GCP APIs.
    /// These can be network errors encountered when sending the requests, but can also indicate
    /// errors returned by the API itself.
    #[error("reqwest error: {context}")]
    Reqwest {
        /// Context describing the request that failed.
        context: String,
        /// The underlying reqwest error.
        #[source]
        cause: reqwest::Error,
    },

    /// Errors related to de/serialization and parsing of object metadata.
    #[error("metadata error: {0}")]
    Metadata(#[from] objectstore_types::metadata::Error),

    /// Errors encountered when attempting to authenticate with GCP.
    #[error("GCP authentication error: {0}")]
    GcpAuth(#[from] gcp_auth::Error),

    /// A spawned service task panicked or was cancelled.
    #[error("service task failed: {0}")]
    TaskFailed(String),

    /// Any other error stemming from one of the storage backends, which might be specific to that
    /// backend or to a certain operation.
    #[error("storage backend error: {context}")]
    Generic {
        /// Context describing the operation that failed.
        context: String,
        /// The underlying error, if available.
        #[source]
        cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

impl Error {
    /// Creates an [`Error::Reqwest`] from a reqwest error with context.
    pub fn reqwest(context: impl Into<String>, cause: reqwest::Error) -> Self {
        Self::Reqwest {
            context: context.into(),
            cause,
        }
    }
}

/// Result type for service operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
