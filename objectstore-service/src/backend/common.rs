//! Shared trait definition and types for all backends.

use std::fmt;

use objectstore_types::metadata::{ExpirationPolicy, Metadata};

use bytes::Bytes;

use crate::error::Result;
use crate::id::ObjectId;
use crate::stream::{ClientStream, PayloadStream};

/// Information about a redirect tombstone in the high-volume backend.
#[derive(Clone, Debug, PartialEq)]
pub struct Tombstone {
    /// The [`ObjectId`] of the object in the long-term backend.
    ///
    /// For legacy tombstones with an empty `r` column, the HV backend resolves
    /// this to the HV `ObjectId` itself before surfacing the tombstone to callers.
    pub target: ObjectId,

    /// The expiration policy copied from the original object.
    pub expiration_policy: ExpirationPolicy,
}

/// Typed response from [`HighVolumeBackend::get_tiered_object`].
pub enum TieredGet {
    /// A real object was found.
    Object(Metadata, PayloadStream),
    /// A redirect tombstone was found; the real object lives in the long-term backend.
    Tombstone(Tombstone),
    /// No entry exists at this key.
    NotFound,
}

impl fmt::Debug for TieredGet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TieredGet::Object(metadata, _stream) => f
                .debug_tuple("Object")
                .field(metadata)
                .finish_non_exhaustive(),
            TieredGet::Tombstone(info) => f.debug_tuple("Tombstone").field(info).finish(),
            TieredGet::NotFound => write!(f, "NotFound"),
        }
    }
}

/// Typed metadata-only response from [`HighVolumeBackend::get_tiered_metadata`].
#[derive(Debug)]
pub enum TieredMetadata {
    /// Metadata for a real object was found.
    Object(Metadata),
    /// A redirect tombstone was found; the real object lives in the long-term backend.
    Tombstone(Tombstone),
    /// No entry exists at this key.
    NotFound,
}

/// User agent string used for outgoing requests.
///
/// This intentionally has a "sentry" prefix so that it can easily be traced back to us.
pub const USER_AGENT: &str = concat!("sentry-objectstore/", env!("CARGO_PKG_VERSION"));

/// Backend response for put operations.
pub type PutResponse = ();
/// Backend response for get operations.
pub type GetResponse = Option<(Metadata, PayloadStream)>;
/// Backend response for metadata-only get operations.
pub type MetadataResponse = Option<Metadata>;
/// Backend response for delete operations.
pub type DeleteResponse = ();

/// Trait implemented by all storage backends.
#[async_trait::async_trait]
pub trait Backend: fmt::Debug + Send + Sync + 'static {
    /// The backend name, used for diagnostics.
    fn name(&self) -> &'static str;

    /// Stores an object at the given path with the given metadata.
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: ClientStream,
    ) -> Result<PutResponse>;

    /// Retrieves an object at the given path, returning its metadata and a stream of bytes.
    async fn get_object(&self, id: &ObjectId) -> Result<GetResponse>;

    /// Retrieves only the metadata for an object, without the payload.
    async fn get_metadata(&self, id: &ObjectId) -> Result<MetadataResponse> {
        Ok(self
            .get_object(id)
            .await?
            .map(|(metadata, _stream)| metadata))
    }

    /// Deletes the object at the given path.
    async fn delete_object(&self, id: &ObjectId) -> Result<DeleteResponse>;
}

/// Trait for backends that support tombstone-conditional operations.
///
/// Only backends suitable for the high-volume tier of
/// [`TieredStorage`](super::tiered::TieredStorage) implement this trait.
/// The conditional methods provide atomic operations to avoid overwriting
/// redirect tombstones.
#[async_trait::async_trait]
pub trait HighVolumeBackend: Backend {
    /// Writes the object only if NO redirect tombstone exists at this key.
    ///
    /// Returns `None` after storing the object, or `Some(tombstone)` (skipping
    /// the write) when a redirect tombstone is present. The returned tombstone
    /// carries the target LT `ObjectId` so the caller can route without a
    /// second round trip.
    ///
    /// Takes [`Bytes`] instead of a [`ClientStream`] because callers on this
    /// path have already fully buffered the payload.
    async fn put_non_tombstone(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        payload: Bytes,
    ) -> Result<Option<Tombstone>>;

    /// Retrieves an object with explicit tombstone awareness.
    ///
    /// Returns [`TieredGet::Tombstone`] instead of synthesizing a tombstone
    /// object, making the caller's routing logic a compile-time distinction.
    async fn get_tiered_object(&self, id: &ObjectId) -> Result<TieredGet>;

    /// Retrieves only metadata with explicit tombstone awareness.
    ///
    /// Implementations should skip the payload column where possible to avoid
    /// fetching up to 1 MiB of data just to discover a tombstone.
    async fn get_tiered_metadata(&self, id: &ObjectId) -> Result<TieredMetadata>;

    /// Deletes the object only if it is NOT a redirect tombstone.
    ///
    /// Returns `None` after deleting the row (or if the row was already absent),
    /// or `Some(tombstone)` (leaving the row intact) when the object is a
    /// redirect tombstone. The returned tombstone carries the target LT
    /// `ObjectId` so the caller can delete from long-term storage directly,
    /// without a second round trip.
    async fn delete_non_tombstone(&self, id: &ObjectId) -> Result<Option<Tombstone>>;

    /// Writes a redirect tombstone for the given object.
    ///
    /// A tombstone signals that the real object lives in the long-term backend
    /// identified by the tombstone's [`target`](Tombstone::target) ID.
    async fn create_tombstone(&self, id: &ObjectId, tombstone: Tombstone) -> Result<()>;
}

/// Creates a reqwest client with required defaults.
///
/// Automatic decompression is disabled because backends store pre-compressed
/// payloads and manage `Content-Encoding` themselves.
pub(super) fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .hickory_dns(true)
        .no_zstd()
        .no_brotli()
        .no_gzip()
        .no_deflate()
        .build()
        .expect("Client::new()")
}
