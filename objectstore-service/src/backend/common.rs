use std::fmt::Debug;

use objectstore_types::Metadata;

use crate::id::ObjectId;
use crate::{PayloadStream, ServiceResult};

/// User agent string used for outgoing requests.
///
/// This intentionally has a "sentry" prefix so that it can easily be traced back to us.
pub const USER_AGENT: &str = concat!("sentry-objectstore/", env!("CARGO_PKG_VERSION"));

/// Backend response for put operations.
pub(super) type PutResponse = ();
/// Backend response for get operations.
pub(super) type GetResponse = Option<(Metadata, PayloadStream)>;
/// Backend response for metadata-only get operations.
pub(super) type MetadataResponse = Option<Metadata>;
/// Backend response for delete operations.
pub(super) type DeleteResponse = ();

/// Response from [`Backend::delete_and_check_tombstone`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TombstoneCheckResponse {
    /// The deleted entity was a redirect tombstone.
    Tombstone,
    /// The deleted entity was a regular object or non-existent.
    Object,
}

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
    ) -> ServiceResult<PutResponse>;

    /// Retrieves an object at the given path, returning its metadata and a stream of bytes.
    async fn get_object(&self, id: &ObjectId) -> ServiceResult<GetResponse>;

    /// Retrieves only the metadata for an object, without the payload.
    async fn get_metadata(&self, id: &ObjectId) -> ServiceResult<MetadataResponse> {
        Ok(self
            .get_object(id)
            .await?
            .map(|(metadata, _stream)| metadata))
    }

    /// Deletes the object at the given path.
    async fn delete_object(&self, id: &ObjectId) -> ServiceResult<DeleteResponse>;

    /// Deletes the object and reports whether it was a redirect tombstone.
    async fn delete_and_check_tombstone(
        &self,
        id: &ObjectId,
    ) -> ServiceResult<TombstoneCheckResponse> {
        let metadata = self.get_metadata(id).await?;
        let result = match metadata {
            Some(m) if m.is_redirect_tombstone == Some(true) => TombstoneCheckResponse::Tombstone,
            _ => TombstoneCheckResponse::Object,
        };
        self.delete_object(id).await?;
        Ok(result)
    }
}

/// Creates a reqwest client with required defaults.
pub fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .hickory_dns(true)
        .build()
        .expect("Client::new()")
}
