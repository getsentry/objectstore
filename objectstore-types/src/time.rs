//! Second-precision timestamps for object creation, access, and expiration.
//!
//! [`Timestamp`] represents object creation times, expiration deadlines, and the access times
//! used to check or renew them. Fractional timestamps round up to the next second. Event
//! timestamps, metrics, and multipart modification times retain their own precision.
//!
//! The default serde representation preserves the `SystemTime` metadata format. Use
//! [`Timestamp::as_rfc3339`] for HTTP headers and JSON fields containing RFC3339 strings.

use std::borrow::Cow;
use std::fmt;
use std::ops::{Add, Sub};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use humantime::TimestampError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// A whole-second Unix timestamp used for access time and expiration.
///
/// Also used for object creation times. Values range from the Unix epoch through
/// `9999-12-31T23:59:59Z`. When constructed with fractional seconds, the timestamp rounds up to
/// the next whole second. Values outside this range are rejected.
///
/// Serializes in the same format as `SystemTime`, with `secs_since_epoch` and `nanos_since_epoch`
/// fields. The nanoseconds field is always zero when serialized. Deserialization accepts legacy
/// fractional timestamps and rounds them upward.
///
/// ```
/// use std::time::Duration;
/// use objectstore_types::time::Timestamp;
///
/// let deadline = Timestamp::from_unix_micros(1_700_000_000_123_456)?;
/// assert_eq!(deadline.as_rfc3339().to_string(), "2023-11-14T22:13:21Z");
/// let extended = deadline + Duration::from_secs(60);
/// assert_eq!(extended.as_secs(), deadline.as_secs() + 60);
/// # Ok::<(), objectstore_types::time::InvalidTimestamp>(())
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The Unix epoch.
    pub const UNIX_EPOCH: Self = Self(0);

    /// The maximum supported timestamp, `9999-12-31T23:59:59Z`.
    const MAX: u64 = 253_402_300_799;

    /// Captures the current wall-clock time, rounded up to a whole second.
    ///
    /// # Panics
    /// Panics if the system clock is outside the supported timestamp range.
    pub fn now() -> Self {
        Self::try_from(SystemTime::now()).expect("system clock outside timestamp range")
    }

    /// Constructs a timestamp from whole Unix seconds.
    pub fn from_unix_secs(seconds: u64) -> Result<Self, InvalidTimestamp> {
        if seconds <= Self::MAX {
            Ok(Self(seconds))
        } else {
            Err(InvalidTimestamp)
        }
    }

    /// Constructs a timestamp from Unix microseconds, rounding fractional seconds upward.
    pub fn from_unix_micros(micros: i64) -> Result<Self, InvalidTimestamp> {
        let micros = u64::try_from(micros).map_err(|_| InvalidTimestamp)?;
        Self::from_unix_secs(micros.div_ceil(1_000_000))
    }

    /// Parses an RFC3339 timestamp, rounding fractional seconds upward.
    ///
    /// Returns an error if the input is invalid or outside the supported timestamp range.
    pub fn from_rfc3339(value: &str) -> Result<Self, TimestampError> {
        let time = humantime::parse_rfc3339(value)?;
        Self::try_from(time).map_err(|_| TimestampError::OutOfRange)
    }

    /// Returns the timestamp in whole Unix seconds.
    pub fn as_secs(self) -> u64 {
        self.0
    }

    /// Returns the second-aligned timestamp in Unix microseconds.
    pub fn as_micros(self) -> u64 {
        self.0 * 1_000_000
    }

    /// Returns a copy that displays and serializes as an RFC3339 string.
    pub fn as_rfc3339(self) -> Rfc3339Timestamp {
        Rfc3339Timestamp(self)
    }

    /// Adds a duration, rounding upward, or returns `None` if the result is out of range.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let seconds = self.0.checked_add(duration.as_secs())?;
        let seconds = seconds.checked_add(u64::from(duration.subsec_nanos() != 0))?;
        Self::from_unix_secs(seconds).ok()
    }

    /// Adds a duration, rounding upward and clamping to the maximum supported timestamp.
    pub fn saturating_add(self, duration: Duration) -> Self {
        self.checked_add(duration).unwrap_or(Self(Self::MAX))
    }

    /// Subtracts a duration, rounding upward, or returns `None` if the result precedes the epoch.
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        // Ceiling a whole timestamp minus a duration subtracts only the whole seconds.
        // Reject an unrounded result before the epoch, just as construction does.
        if duration > Duration::from_secs(self.0) {
            return None;
        }
        Some(Self(self.0 - duration.as_secs()))
    }

    /// Returns the elapsed duration, or `None` if `earlier` is later than this timestamp.
    pub fn checked_duration_since(self, earlier: Self) -> Option<Duration> {
        self.0.checked_sub(earlier.0).map(Duration::from_secs)
    }
}

impl TryFrom<SystemTime> for Timestamp {
    type Error = InvalidTimestamp;

    fn try_from(time: SystemTime) -> Result<Self, Self::Error> {
        let duration = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| InvalidTimestamp)?;
        let seconds = duration
            .as_secs()
            .checked_add(u64::from(duration.subsec_nanos() != 0))
            .ok_or(InvalidTimestamp)?;
        Self::from_unix_secs(seconds)
    }
}

impl From<Timestamp> for SystemTime {
    fn from(time: Timestamp) -> Self {
        Self::UNIX_EPOCH + Duration::from_secs(time.0)
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    /// Adds a duration, rounding up. Panics if the result is outside the supported range.
    fn add(self, duration: Duration) -> Self {
        self.checked_add(duration)
            .expect("timestamp addition out of range")
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Self;

    /// Subtracts a duration, rounding up. Panics if the result precedes the epoch.
    fn sub(self, duration: Duration) -> Self {
        self.checked_sub(duration)
            .expect("timestamp subtraction out of range")
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        SystemTime::from(*self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(SystemTime::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// A timestamp is outside the supported range.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("timestamp is outside the Unix epoch through year 9999")]
pub struct InvalidTimestamp;

/// An owned timestamp view that displays and serializes as a whole-second RFC3339 string.
///
/// Deserialization accepts fractional seconds and rounds upward, like [`Timestamp`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Rfc3339Timestamp(Timestamp);

impl Rfc3339Timestamp {
    /// Returns the underlying timestamp.
    pub fn into_inner(self) -> Timestamp {
        self.0
    }
}

impl fmt::Display for Rfc3339Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        humantime::format_rfc3339_seconds(self.0.into()).fmt(f)
    }
}

impl FromStr for Rfc3339Timestamp {
    type Err = TimestampError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Timestamp::from_rfc3339(value).map(Self)
    }
}

impl Serialize for Rfc3339Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Rfc3339Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Plain `Cow::deserialize` always owns; `borrow` enables borrowing from the input.
        #[derive(Deserialize)]
        #[serde(transparent)]
        struct BorrowedStr<'a>(#[serde(borrow)] Cow<'a, str>);

        let BorrowedStr(value) = BorrowedStr::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounding_and_arithmetic() {
        let epoch = SystemTime::UNIX_EPOCH;
        for (nanos, seconds) in [(0, 0), (1, 1), (999_999_999, 1), (1_000_000_000, 1)] {
            let time = Timestamp::try_from(epoch + Duration::from_nanos(nanos)).unwrap();
            assert_eq!(time.as_secs(), seconds);
            assert_eq!(time.as_micros(), seconds * 1_000_000);
            assert_eq!(Timestamp::try_from(SystemTime::from(time)).unwrap(), time);
        }
        let time = Timestamp::from_unix_secs(10).unwrap();
        assert_eq!((time + Duration::from_millis(1500)).as_secs(), 12);
        assert_eq!((time - Duration::from_millis(1500)).as_secs(), 9);
        assert_eq!(time.checked_duration_since(time), Some(Duration::ZERO));
        assert!(Timestamp::try_from(epoch - Duration::from_nanos(1)).is_err());
        assert!(Timestamp::from_unix_micros(-1).is_err());
        assert_eq!(Timestamp::from_unix_micros(1).unwrap().as_secs(), 1);
        assert!(Timestamp::from_unix_micros(i64::MAX).is_err());
        assert!(
            Timestamp::UNIX_EPOCH
                .checked_sub(Duration::from_nanos(1))
                .is_none()
        );
        let max = Timestamp::from_unix_secs(Timestamp::MAX).unwrap();
        assert!(max.checked_add(Duration::from_nanos(1)).is_none());
        assert!(max.checked_add(Duration::MAX).is_none());
        assert_eq!(max.saturating_add(Duration::from_nanos(1)), max);
        assert_eq!(
            time.saturating_add(Duration::from_millis(1500)).as_secs(),
            12
        );
        assert_eq!(max.as_rfc3339().to_string(), "9999-12-31T23:59:59Z");
    }

    #[test]
    fn serialization_formats() {
        let legacy = r#"{"secs_since_epoch":1700000000,"nanos_since_epoch":1}"#;
        let time: Timestamp = serde_json::from_str(legacy).unwrap();
        assert_eq!(time.as_secs(), 1_700_000_001);
        let json = serde_json::to_string(&time).unwrap();
        assert_eq!(
            json,
            r#"{"secs_since_epoch":1700000001,"nanos_since_epoch":0}"#
        );
        assert_eq!(serde_json::from_str::<Timestamp>(&json).unwrap(), time);
        assert_eq!(
            serde_json::from_str::<SystemTime>(&json).unwrap(),
            time.into()
        );
        let rfc: Rfc3339Timestamp =
            serde_json::from_str(r#""2023-11-14T22:13:20.000001Z""#).unwrap();
        assert_eq!(rfc.into_inner(), time);
        assert_eq!(
            serde_json::to_string(&rfc).unwrap(),
            r#""2023-11-14T22:13:21Z""#
        );
        assert_eq!(rfc.to_string(), "2023-11-14T22:13:21Z");
        assert!(
            "9999-12-31T23:59:59.1Z"
                .parse::<Rfc3339Timestamp>()
                .is_err()
        );
    }
}
