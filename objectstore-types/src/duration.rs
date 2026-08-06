//! The wire format for durations.
//!
//! Durations are exchanged as part of the
//! [`x-sn-expiration`](crate::metadata::HEADER_EXPIRATION) header, for instance as `ttl:7d 12h`.
//!
//! # Emitted format
//!
//! A duration is written as a space-separated list of `<integer><unit>` components, ordered from
//! the largest unit to the smallest. Components that are zero are omitted, and a zero duration is
//! written as `0s`. Only four units are ever emitted:
//!
//! | Unit | Meaning       |
//! |------|---------------|
//! | `d`  | day, 24 hours |
//! | `h`  | hour          |
//! | `m`  | minute        |
//! | `s`  | second        |
//!
//! Day is deliberately the largest unit. Weeks, months, and years are never emitted, because they
//! either have no fixed length (a calendar month) or invite a definition that differs between
//! implementations (is a year 365 or 365.25 days?). Durations longer than a day therefore stay in
//! days: 400 days is written as `400d`, never as `1y 1m 5d`.
//!
//! Second is the smallest unit; any sub-second remainder is truncated.
//!
//! # Parsing
//!
//! [`parse_duration`] accepts a superset of the emitted format, including units this crate never
//! writes. That leniency exists to keep reading values that older versions persisted, and is not
//! part of the wire format: do not rely on it, and do not reproduce it in clients.

use std::error::Error;
use std::fmt;
use std::time::Duration;

const SECS_PER_MINUTE: u64 = 60;
const SECS_PER_HOUR: u64 = 60 * SECS_PER_MINUTE;
const SECS_PER_DAY: u64 = 24 * SECS_PER_HOUR;

/// Formats a duration in the wire format.
///
/// Returns a displayable value that writes the duration using the `d`, `h`, `m`, and `s` units,
/// as described in the [module documentation](self). Sub-second remainders are truncated.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use objectstore_types::duration::format_duration;
///
/// let formatted = format_duration(Duration::from_secs(400 * 86400 + 90));
/// assert_eq!(formatted.to_string(), "400d 1m 30s");
/// ```
pub fn format_duration(duration: Duration) -> FormattedDuration {
    FormattedDuration(duration)
}

/// Parses a duration from the wire format.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use objectstore_types::duration::parse_duration;
///
/// let duration = parse_duration("400d 1m 30s")?;
/// assert_eq!(duration, Duration::from_secs(400 * 86400 + 90));
/// # Ok::<(), objectstore_types::duration::ParseDurationError>(())
/// ```
///
/// # Errors
///
/// Returns a [`ParseDurationError`] if `input` is not a valid duration.
pub fn parse_duration(input: &str) -> Result<Duration, ParseDurationError> {
    humantime::parse_duration(input).map_err(ParseDurationError)
}

/// The error returned when a string is not a valid duration in the wire format.
///
/// Returned by [`parse_duration`].
#[derive(Debug)]
pub struct ParseDurationError(humantime::DurationError);

impl fmt::Display for ParseDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for ParseDurationError {}

/// A [`Duration`] that displays in the wire format.
///
/// Created by [`format_duration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormattedDuration(Duration);

impl fmt::Display for FormattedDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.0.as_secs();
        if secs == 0 {
            return f.write_str("0s");
        }

        let components = [
            (secs / SECS_PER_DAY, "d"),
            (secs % SECS_PER_DAY / SECS_PER_HOUR, "h"),
            (secs % SECS_PER_HOUR / SECS_PER_MINUTE, "m"),
            (secs % SECS_PER_MINUTE, "s"),
        ];

        let mut separator = "";
        for (value, unit) in components {
            if value > 0 {
                write!(f, "{separator}{value}{unit}")?;
                separator = " ";
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn format(duration: Duration) -> String {
        format_duration(duration).to_string()
    }

    #[test]
    fn formats_units() {
        assert_eq!(format(Duration::ZERO), "0s");
        assert_eq!(format(Duration::from_secs(30)), "30s");
        assert_eq!(format(Duration::from_secs(60)), "1m");
        assert_eq!(format(Duration::from_secs(3600)), "1h");
        assert_eq!(format(Duration::from_secs(86400)), "1d");
    }

    #[test]
    fn formats_combined_units_and_skips_zeroes() {
        let duration = Duration::from_secs(2 * 86400 + 3 * 3600 + 4);
        assert_eq!(format(duration), "2d 3h 4s");
    }

    #[test]
    fn keeps_long_durations_in_days() {
        // Neither of these may roll over into weeks, months, or years.
        assert_eq!(format(Duration::from_secs(7 * 86400)), "7d");
        assert_eq!(format(Duration::from_secs(400 * 86400)), "400d");
    }

    #[test]
    fn truncates_sub_second_remainder() {
        assert_eq!(format(Duration::from_millis(1500)), "1s");
        assert_eq!(format(Duration::from_millis(500)), "0s");
    }

    #[test]
    fn round_trips_through_parse() {
        for secs in [0, 1, 59, 60, 3661, 86400, 396 * 86400 + 62208] {
            let duration = Duration::from_secs(secs);
            let formatted = format(duration);
            assert_eq!(parse_duration(&formatted).unwrap(), duration, "{formatted}");
        }
    }

    #[test]
    fn parses_units_that_are_never_emitted() {
        assert_eq!(
            parse_duration("2weeks").unwrap(),
            Duration::from_secs(1_209_600)
        );
        assert_eq!(
            parse_duration("1year").unwrap(),
            Duration::from_secs(31_557_600)
        );
    }
}
