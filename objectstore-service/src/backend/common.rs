use std::fmt::Debug;
use std::io;

use anyhow::Result;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use objectstore_types::Metadata;

use crate::ObjectPath;

/// User agent string used for outgoing requests.
///
/// This intentionally has a "sentry" prefix so that it can easily be traced back to us.
pub const USER_AGENT: &str = concat!("sentry-objectstore/", env!("CARGO_PKG_VERSION"));

pub type BoxedBackend = Box<dyn Backend>;
pub type BackendStream = BoxStream<'static, io::Result<Bytes>>;

#[async_trait::async_trait]
pub trait Backend: Debug + Send + Sync + 'static {
    /// The backend name, used for diagnostics.
    fn name(&self) -> &'static str;

    /// Stores an object at the given path with the given metadata.
    async fn put_object(
        &self,
        path: &ObjectPath,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> Result<()>;

    /// Retrieves an object at the given path, returning its metadata and a stream of bytes.
    async fn get_object(&self, path: &ObjectPath) -> Result<Option<(Metadata, BackendStream)>>;

    /// Deletes the object at the given path.
    async fn delete_object(&self, path: &ObjectPath) -> Result<()>;
}

/// Creates a reqwest client with required defaults.
pub fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .hickory_dns(true)
        .build()
        .expect("Client::new()")
}
