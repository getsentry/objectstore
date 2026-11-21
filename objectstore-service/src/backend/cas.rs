//! Types for [`crate::backend::TieredStorage`]'s CAS protocol.
//!
//! [`ObjectRegistry`]: a trait defining the CAS protocol.
//!
//! [`HighVolumeAdapter`]: an implementation of `ObjectRegistry` that wraps an underlying
//! [`crate::backend::common::HighVolumeBackend`] for compatibility.
//!
//! [`TimeBasedId`]: a Snowflake-like 64-bit unique ID that is ordered by creation time.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use bytes::Bytes;

use objectstore_types::metadata::Metadata;
use objectstore_types::time::Timestamp;

use crate::backend::common::{
    ExpiryUpdate, HighVolumeBackend, TieredGet, TieredMetadata, TieredWrite, TieredUpdate, Tombstone, SetExpiryResponse,
};
use crate::error::{Error, ErrorKind, Result};
use crate::id::ObjectId;

/// Unix timestamp of the [`TimeBasedId`] epoch, 2026-01-01T00:00:00Z, in milliseconds.
const EPOCH_UNIX_MILLIS: u64 = 1_767_225_600_000;

/// Number of low bits holding random data.
const RANDOM_BITS: u32 = 20;
/// Number of bits holding the millisecond timestamp.
const TIMESTAMP_BITS: u32 = 43;

const RANDOM_MASK: u64 = (1 << RANDOM_BITS) - 1;
const TIMESTAMP_MAX: u64 = (1 << TIMESTAMP_BITS) - 1;

pub struct Version {
    pub generation: TimeBasedId,
    pub metageneration: TimeBasedId,
}

pub enum ObservedVersion {
    Legacy,
    V(Version),
}

impl ObservedVersion {
    pub fn increment_for_write(&self) -> Version {
        match self {
            Self::Legacy => Version {
                metageneration: TimeBasedId::now(),
                generation: TimeBasedId::now(),
            },
            Self::V(version) => Version {
                metageneration: TimeBasedId::now(),
                generation: TimeBasedId::after(&version.generation),
            },
        }
    }

    pub fn increment_for_update(&self) -> Version {
        match self {
            Self::Legacy => Version {
                metageneration: TimeBasedId::now(),
                generation: TimeBasedId::ZERO,
            },
            Self::V(version) => Version {
                metageneration: TimeBasedId::after(&version.metageneration),
                generation: version.generation,
            },
        }
    }
}

pub enum ConsumedBy {
    Finalize,
    Cancel,
}

pub enum EntryPayload {
    Inline(Metadata, Bytes),
    Redirect(ObjectId),
    DeletionMarker,
    UploadSessionMarker,
    UploadSessionConsumptionMarker(ConsumedBy),
}

pub type ExpirationTime = Timestamp;

pub enum RegistryRead {
    NotFound,
    Entry(Option<ExpirationTime>, ObservedVersion, EntryPayload),
}

pub struct RegistryWrite {
    expiry: Option<ExpirationTime>,
    version: Version,
    payload: EntryPayload,
}

pub enum Patch {
    SetExpiry(ExpiryUpdate),
}

pub enum OperationResult {
    Applied,
    Rejected,
    Unknown,
}

pub enum RegistryKey {
    Object(ObjectId),
    UploadSession(ObjectId),
}

impl RegistryKey {
    pub fn id(&self) -> &ObjectId {
        match self {
            Self::Object(id) => id,
            Self::UploadSession(id) => id,
        }
    }
}

pub trait ObjectRegistry {
    async fn get_entry_meta(&self, key: &RegistryKey) -> Result<RegistryRead>;

    async fn get_entry(&self, key: &RegistryKey) -> Result<RegistryRead>;

    async fn compare_and_write(
        &self,
        key: &RegistryKey,
        observed: &RegistryRead,
        write: &RegistryWrite,
    ) -> Result<OperationResult>;

    async fn compare_and_update(
        &self,
        key: &RegistryKey,
        observed: &RegistryRead,
        patch: &Patch,
    ) -> Result<OperationResult>;

    async fn blind_compare_and_write(
        &self,
        key: &RegistryKey,
        write: &RegistryWrite,
    ) -> Result<OperationResult>;
}

pub struct HighVolumeAdapter {
    inner: Box<dyn HighVolumeBackend>,
}

impl HighVolumeAdapter {
    pub fn new(inner: Box<dyn HighVolumeBackend>) -> Self {
        Self { inner }
    }
}

impl From<TieredMetadata> for RegistryRead {
    fn from(item: TieredMetadata) -> RegistryRead {
        match item {
            TieredMetadata::NotFound => RegistryRead::NotFound,
            TieredMetadata::Object(metadata) => RegistryRead::Entry(
                metadata.time_expires,
                ObservedVersion::Legacy,
                EntryPayload::Inline(metadata, Bytes::new()),
            ),
            TieredMetadata::Tombstone(tombstone) => RegistryRead::Entry(
                tombstone.time_expires,
                ObservedVersion::Legacy,
                EntryPayload::Redirect(tombstone.target),
            ),
        }
    }
}

impl From<TieredGet> for RegistryRead {
    fn from(item: TieredGet) -> RegistryRead {
        match item {
            TieredGet::NotFound => RegistryRead::NotFound,
            TieredGet::Object(metadata, _, payload) => RegistryRead::Entry(
                metadata.time_expires,
                ObservedVersion::Legacy,
                EntryPayload::Inline(metadata, payload),
            ),
            TieredGet::Tombstone(tombstone) => RegistryRead::Entry(
                tombstone.time_expires,
                ObservedVersion::Legacy,
                EntryPayload::Redirect(tombstone.target),
            ),
        }
    }
}

fn fake_upload_session_marker_expiry() -> Timestamp {
    Timestamp::now() + Duration::from_hours(5 * 24)
}

impl ObjectRegistry for HighVolumeAdapter {
    async fn get_entry_meta(&self, key: &RegistryKey) -> Result<RegistryRead> {
        // TODO: thread this through the trait until I can remove it :(
        let access_time = Timestamp::now();
        Ok(match key {
            RegistryKey::Object(id) => self
                .inner
                .get_tiered_metadata(id, access_time)
                .await?
                .into(),
            RegistryKey::UploadSession(id) => {
                if self.inner.has_upload_marker(id, access_time).await? {
                    // TODO: Expose upload session marker expiry time from HV backend
                    RegistryRead::Entry(
                        Some(fake_upload_session_marker_expiry()),
                        ObservedVersion::Legacy,
                        EntryPayload::UploadSessionMarker,
                    )
                } else {
                    RegistryRead::NotFound
                }
            }
        })
    }

    async fn get_entry(&self, key: &RegistryKey) -> Result<RegistryRead> {
        // TODO: thread this through the trait until I can remove it :(
        let access_time = Timestamp::now();
        Ok(match key {
            // TODO content range
            RegistryKey::Object(id) => self
                .inner
                .get_tiered_object(id, access_time, None)
                .await?
                .into(),
            RegistryKey::UploadSession(_) => RegistryRead::NotFound,
        })
    }

    async fn compare_and_write(
        &self,
        key: &RegistryKey,
        observed: &RegistryRead,
        write: &RegistryWrite,
    ) -> Result<OperationResult> {
        // TODO: thread this through the trait until I can remove it :(
        let access_time = Timestamp::now();
        let current_target = match observed {
            RegistryRead::Entry(_, _, EntryPayload::Redirect(t)) => Some(t),
            _ => None,
        };
        Ok(match write.payload {
            EntryPayload::DeletionMarker | EntryPayload::UploadSessionConsumptionMarker(_) => {
                return Err(Error::new(ErrorKind::Internal, "Not implemented"));
            }
            EntryPayload::UploadSessionMarker => {
                // TODO eliminate unwrap
                self.inner
                    .create_upload_marker(key.id(), write.expiry.unwrap())
                    .await
                    .map(|_| OperationResult::Applied)?
            }
            EntryPayload::Inline(m, p) => {
                let written = self
                    .inner
                    .compare_and_write(
                        key.id(),
                        current_target,
                        TieredWrite::Object(m, p),
                        access_time,
                    )
                    .await?;
                if written {
                    OperationResult::Applied
                } else {
                    OperationResult::Rejected
                }
            }
            EntryPayload::Redirect(t) => {
                let written = self
                    .inner
                    .compare_and_write(
                        key.id(),
                        current_target,
                        TieredWrite::Tombstone(Tombstone {
                            target: t,
                            time_expires: write.expiry,
                        }),
                        access_time,
                    )
                    .await?;
                if written {
                    OperationResult::Applied
                } else {
                    OperationResult::Rejected
                }
            }
        })
    }

    async fn compare_and_update(
        &self,
        key: &RegistryKey,
        observed: &RegistryRead,
        patch: &Patch,
    ) -> Result<OperationResult> {
        // TODO: thread this through the trait until I can remove it :(
        let access_time = Timestamp::now();
        let current_target = match observed {
            RegistryRead::Entry(_, _, EntryPayload::Redirect(t)) => Some(t),
            _ => None,
        };
        let tiered_update = match patch {
            Patch::SetExpiry(expiry_update) => TieredUpdate::SetExpiry(*expiry_update),
        };
        let result = self.inner.compare_and_update(key.id(), current_target, tiered_update, access_time).await?;
        Ok(match result {
            // TODO handle the returned timestamp properly
            SetExpiryResponse::Satisfied(_) => OperationResult::Applied,
            _ => OperationResult::Rejected,
        })
    }

    async fn blind_compare_and_write(
        &self,
        key: &RegistryKey,
        write: &RegistryWrite,
    ) -> Result<OperationResult> {
        // TODO: thread this through the trait until I can remove it :(
        let access_time = Timestamp::now();
        Ok(match write.payload {
            EntryPayload::Inline(m, p) => {
                match self.inner.put_non_tombstone(key.id(), &m, p, access_time).await? {
                    Some(_) => OperationResult::Rejected,
                    None => OperationResult::Applied,
                }
            },
            EntryPayload::DeletionMarker => {
                match self.inner.delete_non_tombstone(key.id(), access_time).await? {
                    Some(_) => OperationResult::Rejected,
                    None => OperationResult::Applied,
                }
            },
            _ => {
                return Err(Error::new(ErrorKind::Internal, "Not implemented"));
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

        assert_eq!(u, 0xEFFFFFFFFFFFFFFF);
        assert_eq!(i, (0xEFFFFFFFFFFFFFFFu64 as i64));
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
