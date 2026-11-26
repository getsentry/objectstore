//! Types for Objectstore's Compare-and-Swap (CAS) protocol.
//!
//! [`ObjectRegistry`]: a trait defining the CAS protocol's operations.
//!
//! [`HighVolumeAdapter`]: an implementation of `ObjectRegistry` that wraps an underlying
//! [`HighVolumeBackend`] for compatibility. The underlying backend does not necessarily
//! adhere to the CAS protocol.
//!
//! [`TimeBasedId`]: a Snowflake-like 64-bit unique ID that is ordered by creation time.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use bytes::Bytes;

use objectstore_types::metadata::Metadata;
use objectstore_types::range::{ByteRange, ContentRange};
use objectstore_types::time::Timestamp;

use crate::backend::common::{
    ExpiryUpdate, HighVolumeBackend, SetExpiryResponse, TieredGet, TieredMetadata, TieredUpdate,
    TieredWrite, Tombstone,
};
use crate::error::{Error, ErrorKind, Result};
use crate::id::ObjectId;
use crate::stream::PayloadStream;

/// Unix timestamp of the [`TimeBasedId`] epoch, 2026-01-01T00:00:00Z, in milliseconds.
const EPOCH_UNIX_MILLIS: u64 = 1_767_225_600_000;

/// Number of low bits holding random data.
const RANDOM_BITS: u32 = 20;
/// Number of bits holding the millisecond timestamp.
const TIMESTAMP_BITS: u32 = 43;

const RANDOM_MASK: u64 = (1 << RANDOM_BITS) - 1;
const TIMESTAMP_MAX: u64 = (1 << TIMESTAMP_BITS) - 1;

/// Holds the core pair of values used for conditional writes under the CAS protocol.
#[derive(Clone, Debug)]
pub struct Version {
    /// Indicates the state of object data for a registry entry. Not affected by metadata
    /// changes or expiry updates.
    pub generation: TimeBasedId,
    /// Indicates the state of a registry entry overall. Affected by changes to object
    /// data, metadata, and expiry updates.
    pub metageneration: TimeBasedId,
}

impl Version {
    /// Creates a fresh `Version` from the current time.
    pub fn now() -> Self {
        Self {
            generation: TimeBasedId::now(),
            metageneration: TimeBasedId::now(),
        }
    }

    /// Creates the [`Version`] to stamp on an entry written over `read` for use in
    /// [`ObjectRegistry::compare_and_write`] or [`ObjectRegistry::blind_compare_and_write`].
    ///
    /// Missing and [`ObservedVersion::Legacy`] entries get a fresh version.
    pub fn for_write<P>(read: &RegistryRead<P>) -> Self {
        let RegistryRead::Exists(entry) = read else {
            return Self::now();
        };

        match &entry.version {
            ObservedVersion::Legacy => Self::now(),
            ObservedVersion::V(version) => Self {
                generation: TimeBasedId::after(&version.generation),
                metageneration: TimeBasedId::now(),
            },
        }
    }

    /// Creates the [`Version`] to stamp on an entry when patching it for use in
    /// [`ObjectRegistry::compare_and_update`].
    ///
    /// Returns `None` if the entry is missing since updates never create entries.
    pub fn for_update<P>(read: &RegistryRead<P>) -> Option<Self> {
        let RegistryRead::Exists(entry) = read else {
            return None;
        };

        Some(match &entry.version {
            ObservedVersion::Legacy => Self {
                generation: TimeBasedId::ZERO,
                metageneration: TimeBasedId::now(),
            },
            ObservedVersion::V(version) => Self {
                generation: version.generation,
                metageneration: TimeBasedId::after(&version.metageneration),
            },
        })
    }
}

/// The observed [`Version`] of a live entry.
#[derive(Clone, Debug)]
pub enum ObservedVersion {
    /// The entry exists but was created under an older version of the CAS protocol.
    Legacy,
    /// The entry exists and has complete CAS information.
    V(Version),
}

/// The operation that consumed a resumable upload session token.
#[derive(Debug)]
pub enum ConsumedBy {
    /// The resumable upload was finalized.
    Finalize,
    /// The resumable upload was cancelled.
    Cancel,
}

/// Contains a stream of an entry's inlined object data (or a segment of it).
pub struct InlineObject {
    /// The range within a blob of object data that can be streamed from `payload`.
    ///
    /// Used in responses to HTTP range reads.
    pub range: Option<ContentRange>,
    /// A stream of object data from an object inlined in a registry entry.
    pub payload: PayloadStream,
}

impl fmt::Debug for InlineObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InlineObject")
            .field("range", &self.range)
            .finish_non_exhaustive()
    }
}

/// The state and content of a registry entry.
///
/// Generic around `P` to allow different operations to deal with inlined object payloads
/// in an appropriate way.
#[derive(Debug)]
pub enum EntryPayload<P> {
    /// The entry contains an inlined object and its metadata.
    Inline(Metadata, P),
    /// The entry contains a redirect pointer to an object's data in another backend.
    Redirect(ObjectId),
    /// The entry contains a tombstone indicating explicit deletion.
    DeletionMarker,
    /// The entry is a token representing an ongoing resumable upload session.
    UploadSessionMarker,
    /// The entry indicates a resumable upload session token has been consumed and future
    /// operations on that session should not proceed.
    UploadSessionConsumptionMarker(ConsumedBy),
}

/// The time at which a registry entry should expire.
pub type ExpirationTime = Timestamp;

/// A registry entry.
///
/// Generic around `V` so reads can report an [`ObservedVersion`] while writes carry a
/// concrete [`Version`], and around `P` like [`EntryPayload`]. See [`MetaEntry`],
/// [`ObjectEntry`], and [`WriteEntry`] for the combinations in use.
#[derive(Debug)]
pub struct Entry<V, P> {
    /// The time at which the entry is set to expire.
    pub expiry: Option<ExpirationTime>,
    /// The entry's version for use in the CAS protocol.
    pub version: V,
    /// The content of the entry.
    pub payload: EntryPayload<P>,
}

/// An entry read by [`ObjectRegistry::get_entry_meta`], without inline payload data.
pub type MetaEntry = Entry<ObservedVersion, ()>;
/// An entry read by [`ObjectRegistry::get_entry`], with inline payload data.
pub type ObjectEntry = Entry<ObservedVersion, InlineObject>;
/// An entry to store with [`ObjectRegistry::compare_and_write`] or
/// [`ObjectRegistry::blind_compare_and_write`].
pub type WriteEntry = Entry<Version, Bytes>;

/// The result of a registry lookup.
///
/// Generic around `P` so [`ObjectRegistry::get_entry_meta`] can omit inline payload data
/// and [`ObjectRegistry::get_entry`] can include it.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum RegistryRead<P> {
    /// No live entry exists.
    NotFound,
    /// A live entry exists.
    Exists(Entry<ObservedVersion, P>),
}

impl<P> RegistryRead<P> {
    /// Returns the redirect target if the entry is a redirect.
    pub fn redirect_target(&self) -> Option<&ObjectId> {
        match self {
            Self::Exists(Entry {
                payload: EntryPayload::Redirect(target),
                ..
            }) => Some(target),
            _ => None,
        }
    }
}

/// Type alias for the return value of [`ObjectRegistry::get_entry_meta`]. Omits inline
/// payload data.
pub type RegistryMeta = RegistryRead<()>;
/// Type alias for the return value of [`ObjectRegistry::get_entry`]. Includes inline
/// payload data.
pub type RegistryObject = RegistryRead<InlineObject>;

/// A patch operation to be applied on a registry entry in a call to
/// [`ObjectRegistry::compare_and_update`].
#[derive(Debug)]
pub enum Patch {
    /// The object's expiry should be extended.
    SetExpiry(ExpiryUpdate),
}

/// The result of a CAS operation.
#[derive(Debug)]
pub enum OperationResult {
    /// The operation was committed.
    Applied,
    /// The operation was not committed due to a predicate failure.
    Rejected,
    /// It's unclear whether the operation was committed or not due to an error.
    Unknown,
}

impl OperationResult {
    /// Returns [`Applied`](Self::Applied) if `applied`, otherwise
    /// [`Rejected`](Self::Rejected).
    pub fn applied_if(applied: bool) -> Self {
        if applied {
            Self::Applied
        } else {
            Self::Rejected
        }
    }
}

/// The ID of a registry entry.
///
/// Entries are namespaced so that the key for an upload session doesn't collide with the
/// key for the object that session is building.
#[derive(Debug)]
pub enum RegistryKey {
    /// Identifies a registry entry for an object stored in Objectstore.
    Object(ObjectId),
    /// Identifies a registry entry for resumable upload session state.
    UploadSession(ObjectId),
}

impl RegistryKey {
    /// Extracts the `ObjectId` of a namespaced key.
    pub fn id(&self) -> &ObjectId {
        match self {
            Self::Object(id) => id,
            Self::UploadSession(id) => id,
        }
    }
}

/// The interface and specification for Objectstore's CAS protocol.
#[async_trait::async_trait]
pub trait ObjectRegistry: Send + Sync {
    /// Given a namespaced `key`, return the corresponding registry entry minus any inline
    /// object data.
    async fn get_entry_meta(
        &self,
        key: &RegistryKey,
        access_time: Timestamp,
    ) -> Result<RegistryMeta>;

    /// Given a namespaced `key`, return the corresponding registry entry including any
    /// inline object data.
    async fn get_entry(
        &self,
        key: &RegistryKey,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<RegistryObject>;

    /// Stores `write` in the registry entry corresponding to `key` as long as its state
    /// matches what was previously read in `observed`.
    ///
    /// `write.version` becomes the entry's new version; see [`Version::for_write`].
    async fn compare_and_write(
        &self,
        key: &RegistryKey,
        observed: &RegistryMeta,
        write: &WriteEntry,
        access_time: Timestamp,
    ) -> Result<OperationResult>;

    /// Apply `patch` to the registry entry corresponding to `key` as long as its state
    /// matches what was previously read in `observed`.
    ///
    /// `version` becomes the entry's new version; see [`Version::for_update`].
    async fn compare_and_update(
        &self,
        key: &RegistryKey,
        observed: &RegistryMeta,
        version: &Version,
        patch: &Patch,
        access_time: Timestamp,
    ) -> Result<OperationResult>;

    /// Optimistically attempt to store `write` in the registry entry corresponding to
    /// `key`. Must fail if the currently-stored entry contains
    /// [`EntryPayload::Redirect`].
    async fn blind_compare_and_write(
        &self,
        key: &RegistryKey,
        write: &WriteEntry,
        access_time: Timestamp,
    ) -> Result<OperationResult>;
}

/// Adapter that shims the [`ObjectRegistry`] API into pre-existing [`HighVolumeBackend`]
/// implementations.
///
/// Note: [`HighVolumeBackend`] implementations do not use `ObjectRegistry`'s CAS protocol
/// and may provide different consistency guarantees.
#[derive(Debug)]
pub struct HighVolumeAdapter {
    inner: Box<dyn HighVolumeBackend>,
}

impl HighVolumeAdapter {
    /// Creates a new `HighVolumeAdapter` wrapping `inner`.
    pub fn new(inner: Box<dyn HighVolumeBackend>) -> Self {
        Self { inner }
    }
}

impl From<TieredMetadata> for RegistryMeta {
    fn from(item: TieredMetadata) -> Self {
        match item {
            TieredMetadata::NotFound => RegistryRead::NotFound,
            TieredMetadata::Object(metadata) => RegistryRead::Exists(Entry {
                expiry: metadata.time_expires,
                version: ObservedVersion::Legacy,
                payload: EntryPayload::Inline(metadata, ()),
            }),
            TieredMetadata::Tombstone(tombstone) => RegistryRead::Exists(Entry {
                expiry: tombstone.time_expires,
                version: ObservedVersion::Legacy,
                payload: EntryPayload::Redirect(tombstone.target),
            }),
        }
    }
}

impl From<TieredGet> for RegistryObject {
    fn from(item: TieredGet) -> Self {
        match item {
            TieredGet::NotFound => RegistryRead::NotFound,
            TieredGet::Object(metadata, range, payload) => RegistryRead::Exists(Entry {
                expiry: metadata.time_expires,
                version: ObservedVersion::Legacy,
                payload: EntryPayload::Inline(metadata, InlineObject { range, payload }),
            }),
            TieredGet::Tombstone(tombstone) => RegistryRead::Exists(Entry {
                expiry: tombstone.time_expires,
                version: ObservedVersion::Legacy,
                payload: EntryPayload::Redirect(tombstone.target),
            }),
        }
    }
}

#[async_trait::async_trait]
impl ObjectRegistry for HighVolumeAdapter {
    async fn get_entry_meta(
        &self,
        key: &RegistryKey,
        access_time: Timestamp,
    ) -> Result<RegistryMeta> {
        Ok(match key {
            RegistryKey::Object(id) => self
                .inner
                .get_tiered_metadata(id, access_time)
                .await?
                .into(),
            RegistryKey::UploadSession(id) => {
                match self.inner.get_upload_marker(id, access_time).await? {
                    Some(expiry) => RegistryRead::Exists(Entry {
                        expiry: Some(expiry),
                        version: ObservedVersion::Legacy,
                        payload: EntryPayload::UploadSessionMarker,
                    }),
                    None => RegistryRead::NotFound,
                }
            }
        })
    }

    async fn get_entry(
        &self,
        key: &RegistryKey,
        access_time: Timestamp,
        range: Option<ByteRange>,
    ) -> Result<RegistryObject> {
        Ok(match key {
            RegistryKey::Object(id) => self
                .inner
                .get_tiered_object(id, access_time, range)
                .await?
                .into(),
            RegistryKey::UploadSession(id) => {
                match self.inner.get_upload_marker(id, access_time).await? {
                    Some(expiry) => RegistryRead::Exists(Entry {
                        expiry: Some(expiry),
                        version: ObservedVersion::Legacy,
                        payload: EntryPayload::UploadSessionMarker,
                    }),
                    None => RegistryRead::NotFound,
                }
            }
        })
    }

    async fn compare_and_write(
        &self,
        key: &RegistryKey,
        observed: &RegistryMeta,
        write: &WriteEntry,
        access_time: Timestamp,
    ) -> Result<OperationResult> {
        let tiered_write = match &write.payload {
            EntryPayload::UploadSessionMarker => {
                let Some(expiry) = write.expiry else {
                    return Err(Error::new(
                        ErrorKind::Internal,
                        "upload session marker requires an expiry",
                    ));
                };
                self.inner.create_upload_marker(key.id(), expiry).await?;
                return Ok(OperationResult::Applied);
            }
            EntryPayload::UploadSessionConsumptionMarker(_) => {
                let deleted = self
                    .inner
                    .delete_upload_marker(key.id(), access_time)
                    .await?;
                return Ok(OperationResult::applied_if(deleted));
            }
            EntryPayload::DeletionMarker => TieredWrite::Delete,
            EntryPayload::Inline(m, p) => {
                if m.time_expires != write.expiry {
                    return Err(Error::new(
                        ErrorKind::Internal,
                        "entry expiry does not match metadata expiry",
                    ));
                }
                TieredWrite::Object(m.clone(), p.clone())
            }
            EntryPayload::Redirect(t) => TieredWrite::Tombstone(Tombstone {
                target: t.clone(),
                time_expires: write.expiry,
            }),
        };
        let written = self
            .inner
            .compare_and_write(
                key.id(),
                observed.redirect_target(),
                tiered_write,
                access_time,
            )
            .await?;
        Ok(OperationResult::applied_if(written))
    }

    async fn compare_and_update(
        &self,
        key: &RegistryKey,
        observed: &RegistryMeta,
        _version: &Version,
        patch: &Patch,
        access_time: Timestamp,
    ) -> Result<OperationResult> {
        if matches!(observed, RegistryRead::NotFound) {
            return Ok(OperationResult::Rejected);
        }
        let current_target = observed.redirect_target();
        let tiered_update = match patch {
            Patch::SetExpiry(expiry_update) => TieredUpdate::SetExpiry(*expiry_update),
        };
        let result = self
            .inner
            .compare_and_update(key.id(), current_target, tiered_update, access_time)
            .await?;
        Ok(match result {
            // The resolved deadline is ignored on purpose: callers resolve the target and
            // check eligibility from their own read before calling so they already know it.
            SetExpiryResponse::Satisfied(_) => OperationResult::Applied,
            SetExpiryResponse::NotFound | SetExpiryResponse::Rejected => OperationResult::Rejected,
        })
    }

    async fn blind_compare_and_write(
        &self,
        key: &RegistryKey,
        write: &WriteEntry,
        access_time: Timestamp,
    ) -> Result<OperationResult> {
        Ok(match &write.payload {
            EntryPayload::Inline(m, p) => {
                if m.time_expires != write.expiry {
                    return Err(Error::new(
                        ErrorKind::Internal,
                        "entry expiry does not match metadata expiry",
                    ));
                }
                let tombstone = self
                    .inner
                    .put_non_tombstone(key.id(), m, p.clone(), access_time)
                    .await?;
                OperationResult::applied_if(tombstone.is_none())
            }
            EntryPayload::DeletionMarker => {
                let tombstone = self
                    .inner
                    .delete_non_tombstone(key.id(), access_time)
                    .await?;
                OperationResult::applied_if(tombstone.is_none())
            }
            _ => {
                return Err(Error::new(ErrorKind::Unsupported, "Not implemented"));
            }
        })
    }
}

/// A 64-bit, Snowflake-like identifier ordered by creation time.
///
/// | Bit (MSB first) | 0      | 1..=43                        | 44..=63 |
/// |-----------------|--------|-------------------------------|---------|
/// | Value           | 0      | Milliseconds since 2026-01-01 | Random  |
///
/// The top bit is always zero so the value is positive as both `u64` and `i64`. Unlike
/// Snowflake IDs, the low bits are random rather than host-based.
///
/// IDs are serialized as 8-byte big-endian integers to ensure byte-wise indexes will
/// order them numerically.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct TimeBasedId(u64);

impl TimeBasedId {
    /// Sentinel value for data written before `TimeBasedId` was introduced.
    pub const ZERO: Self = Self(0);

    /// Creates a `TimeBasedId` with a sentinel value of 0.
    pub const fn zero() -> Self {
        Self::ZERO
    }

    /// Creates a `TimeBasedId` from its raw representation.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw representation.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Encodes the ID as 8 big-endian bytes.
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Decodes an ID from 8 big-endian bytes.
    ///
    /// Returns `None` if `bytes` is not exactly 8 bytes long.
    pub fn from_be_bytes(bytes: &[u8]) -> Option<Self> {
        Some(Self(u64::from_be_bytes(bytes.try_into().ok()?)))
    }

    /// Creates a `TimeBasedId` based on the current system time and random bits.
    ///
    /// Guaranteed to be recent, but not necessarily monotonic amidst concurrent requests.
    pub fn now() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        Self::from_parts(millis.saturating_sub(EPOCH_UNIX_MILLIS), rand::random())
    }

    /// Creates a `TimeBasedId` that is strictly larger than `self` by incrementing
    /// `self`'s time component and regenerating the random component.
    ///
    /// Guaranteed to be strictly larger than `self`, but not necessarily recent.
    pub fn bump(&self) -> Self {
        Self::from_parts(self.timestamp_millis().saturating_add(1), rand::random())
    }

    /// Creates a `TimeBasedId` that is guaranteed to be both recent and strictly larger
    /// than `other`.
    ///
    /// Use this method to create `TimeBasedId`s when it's important that the value
    /// ratchet upwards.
    pub fn after(other: &Self) -> Self {
        std::cmp::max(Self::now(), other.bump())
    }

    /// Returns the timestamp component: milliseconds since the ID epoch.
    pub fn timestamp_millis(&self) -> u64 {
        self.0 >> RANDOM_BITS
    }

    fn from_parts(millis: u64, random: u64) -> Self {
        let millis = millis.min(TIMESTAMP_MAX);
        Self((millis << RANDOM_BITS) | (random & RANDOM_MASK))
    }
}

impl std::fmt::Display for TimeBasedId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_based_id_sign_bit() {
        let id = TimeBasedId::from_parts(u64::MAX, u64::MAX);
        assert_eq!(id.as_u64() >> 63, 0);

        let u = id.as_u64();
        let i: i64 = u.try_into().unwrap();
        assert_eq!(u, i as u64);

        assert_eq!(u, 0x7FFFFFFFFFFFFFFF);
        assert_eq!(i, (0x7FFFFFFFFFFFFFFFu64 as i64));
    }

    #[test]
    fn time_based_id_bump() {
        let id = TimeBasedId::from_parts(1000, RANDOM_MASK);
        let bumped = id.bump();
        assert!(bumped > id);
        assert_eq!(bumped.timestamp_millis(), 1001);
    }

    #[test]
    fn time_based_id_after() {
        // Do we choose `other.bump()` for IDs past `now()`?
        let future = TimeBasedId::from_parts(TimeBasedId::now().timestamp_millis() + 60_000, 0);
        let next = TimeBasedId::after(&future);
        assert!(next > future);
        assert_eq!(next.timestamp_millis(), future.timestamp_millis() + 1);

        // Do we choose `now()` when `other.bump()` is old?
        let old = TimeBasedId::from_parts(5, 0);
        let next = TimeBasedId::after(&old);
        // `other.bump()` has a millisecond component of 6. The epoch is 2026-01-01 so
        // `now()` will certainly be higher than 6.
        assert!(next.timestamp_millis() > 6);
    }
}
