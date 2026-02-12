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

/// Result of a delete operation that also detects whether the row had a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeleteDetectResult {
    /// Row had a payload column — real object in this backend.
    HadPayload,
    /// Row had no payload column — tombstone or non-existent.
    NoPayload,
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
        Ok(self.get_object(id).await?.map(|(metadata, _stream)| metadata))
    }

    /// Deletes the object at the given path.
    async fn delete_object(&self, id: &ObjectId) -> ServiceResult<DeleteResponse>;

    /// Deletes the object and reports whether it had a payload column.
    async fn delete_and_detect(&self, id: &ObjectId) -> ServiceResult<DeleteDetectResult> {
        let metadata = self.get_metadata(id).await?;
        match metadata {
            Some(m) if m.is_redirect_tombstone == Some(true) => {
                self.delete_object(id).await?;
                Ok(DeleteDetectResult::NoPayload)
            }
            Some(_) => {
                self.delete_object(id).await?;
                Ok(DeleteDetectResult::HadPayload)
            }
            None => Ok(DeleteDetectResult::NoPayload),
        }
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
