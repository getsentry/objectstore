//! Types for [`crate::backend::TieredStorage`]'s CAS protocol.
//!
//! [`TimeBasedId`]: a Snowflake-like 64-bit unique ID that is ordered by creation time.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Unix timestamp of the [`TimeBasedId`] epoch, 2026-01-01T00:00:00Z, in milliseconds.
const EPOCH_UNIX_MILLIS: u64 = 1_767_225_600_000;

/// Number of low bits holding random data.
const RANDOM_BITS: u32 = 20;
/// Number of bits holding the millisecond timestamp.
const TIMESTAMP_BITS: u32 = 43;

const RANDOM_MASK: u64 = (1 << RANDOM_BITS) - 1;
const TIMESTAMP_MAX: u64 = (1 << TIMESTAMP_BITS) - 1;

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
