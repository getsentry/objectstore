use thiserror::Error;

/// Error type for service operations.
#[derive(Debug, Error)]
pub enum ServiceError {
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

impl ServiceError {
    /// Creates a [`ServiceError::Reqwest`] from a reqwest error with context.
    pub fn reqwest(context: impl Into<String>, cause: reqwest::Error) -> Self {
        Self::Reqwest {
            context: context.into(),
            cause,
        }
    }
}

/// Result type for service operations.
pub type ServiceResult<T> = Result<T, ServiceError>;
