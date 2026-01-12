use std::fmt::Debug;

use objectstore_types::Metadata;

use crate::PayloadStream;
use crate::id::ObjectId;
use thiserror::Error;

/// User agent string used for outgoing requests.
///
/// This intentionally has a "sentry" prefix so that it can easily be traced back to us.
pub const USER_AGENT: &str = concat!("sentry-objectstore/", env!("CARGO_PKG_VERSION"));

/// A type-erased [`Backend`] instance.
pub type BoxedBackend = Box<dyn Backend>;

#[async_trait::async_trait]
pub trait Backend: Debug + Send + Sync + 'static {
    /// The backend name, used for diagnostics.
    fn name(&self) -> &'static str;

    /// Stores an object at the given path with the given metadata.
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: PayloadStream,
    ) -> BackendResult<()>;

    /// Retrieves an object at the given path, returning its metadata and a stream of bytes.
    async fn get_object(&self, id: &ObjectId) -> BackendResult<Option<(Metadata, PayloadStream)>>;

    /// Deletes the object at the given path.
    async fn delete_object(&self, id: &ObjectId) -> BackendResult<()>;
}

#[derive(Debug, Error)]
pub enum BackendError {
    /// IO errors related to payload streaming or file operations.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Errors related to de/serialization.
    #[error("serde error: {context}")]
    Serde {
        context: String,
        #[source]
        cause: serde_json::Error,
    },

    /// All errors stemming from the reqwest client, used in multiple backends to send requests to
    /// e.g. GCP APIs.
    /// These can be network errors encountered when sending the requests, but can also indicate
    /// errors returned by the API itself.
    #[error("reqwest error: {context}")]
    Reqwest {
        context: String,
        #[source]
        cause: reqwest::Error,
    },

    /// Errors related to de/serialization and parsing of object metadata.
    #[error("metadata error: {0}")]
    Metadata(#[from] objectstore_types::Error),

    /// Errors encountered when attempting to authenticate with GCP.
    #[error("GCP authentication error: {0}")]
    GcpAuth(#[from] gcp_auth::Error),

    /// Any other error stemming from one of the storage backends, which might be specific to that
    /// backend or to a certain operation.
    #[error("storage backend error: {context}")]
    Generic {
        context: String,
        #[source]
        cause: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Result type for backend operations.
pub type BackendResult<T> = Result<T, BackendError>;

/// Creates a reqwest client with required defaults.
pub fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .hickory_dns(true)
        .build()
        .expect("Client::new()")
}
