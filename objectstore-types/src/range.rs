//! Types for HTTP range requests.

use std::fmt;

use http::header::HeaderValue;
use thiserror::Error;

/// Specifier for a single byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// Bounded range with start and end, inclusive
    Bounded(u64, u64),
    /// From offset X onwards
    From(u64),
    /// Last X bytes
    Last(u64),
}

/// Parses a `Range` request header value into a [`ByteRange`].
///
/// Only `bytes=` ranges with a single specifier are accepted.
/// Multiple ranges and non-`bytes` units are rejected.
impl TryFrom<&str> for ByteRange {
    type Error = RangeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let lower = value.to_ascii_lowercase();
        let Some(spec) = lower.strip_prefix("bytes=") else {
            let unit = lower.split_once('=').map_or(&*lower, |(u, _)| u);
            return Err(RangeError::InvalidUnit(unit.to_owned()));
        };
        if spec.contains(',') {
            return Err(RangeError::MultiRange);
        }

        let (start, end) = spec.split_once('-').ok_or(RangeError::Invalid)?;
        if end.is_empty() {
            let start: u64 = start.parse().map_err(|_| RangeError::Invalid)?;
            Ok(ByteRange::From(start))
        } else if start.is_empty() {
            let last: u64 = end.parse().map_err(|_| RangeError::Invalid)?;
            Ok(ByteRange::Last(last))
        } else {
            let start: u64 = start.parse().map_err(|_| RangeError::Invalid)?;
            let end: u64 = end.parse().map_err(|_| RangeError::Invalid)?;
            if start > end {
                return Err(RangeError::Invalid);
            }
            Ok(ByteRange::Bounded(start, end))
        }
    }
}

impl ByteRange {
    /// Formats this range as a `Range` request header value (e.g. `bytes=0-499`).
    pub fn to_header_value(&self) -> HeaderValue {
        let s = match self {
            ByteRange::Bounded(a, b) => format!("bytes={a}-{b}"),
            ByteRange::From(n) => format!("bytes={n}-"),
            ByteRange::Last(n) => format!("bytes=-{n}"),
        };
        HeaderValue::from_str(&s).expect("always a valid header value")
    }

    /// Resolves this range against a known total size.
    pub fn resolve(self, total: u64) -> Option<ContentRange> {
        if total == 0 {
            return None;
        }

        let (start, end) = match self {
            ByteRange::Bounded(s, e) => {
                if s >= total {
                    return None;
                }
                (s, e.min(total - 1))
            }
            ByteRange::From(s) => {
                if s >= total {
                    return None;
                }
                (s, total - 1)
            }
            ByteRange::Last(n) => {
                let start = total.saturating_sub(n);
                (start, total - 1)
            }
        };

        Some(ContentRange { start, end, total })
    }
}

/// Describes which bytes of the full object are present in the response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentRange {
    /// Byte offset of the first byte in the body (inclusive).
    pub start: u64,
    /// Byte offset of the last byte in the body (inclusive).
    pub end: u64,
    /// Total size of the complete object in bytes.
    pub total: u64,
}

impl ContentRange {
    /// Creates a [`ContentRange`] representing the entire object.
    pub fn full(total: u64) -> Self {
        Self {
            start: 0,
            end: total.saturating_sub(1),
            total,
        }
    }

    /// Parses a `Content-Range` response header value (e.g. `bytes 0-499/1234`).
    pub fn parse(header: &str) -> Option<Self> {
        let rest = header.strip_prefix("bytes ")?;
        let (range_part, total_str) = rest.split_once('/')?;
        let total: u64 = total_str.parse().ok()?;
        let (start_str, end_str) = range_part.split_once('-')?;
        let start: u64 = start_str.parse().ok()?;
        let end: u64 = end_str.parse().ok()?;
        Some(Self { start, end, total })
    }

    /// Parses the total from an unsatisfiable `Content-Range` header (`bytes */1234`).
    pub fn parse_unsatisfiable_total(header: &str) -> Option<u64> {
        let rest = header.strip_prefix("bytes */")?;
        rest.parse().ok()
    }

    /// Returns the number of bytes in this range.
    pub fn len(&self) -> u64 {
        if self.total == 0 {
            return 0;
        }
        self.end - self.start + 1
    }

    /// Returns `true` if this range is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if this range covers the entire object.
    pub fn is_full(&self) -> bool {
        self.total == 0 || (self.start == 0 && self.len() == self.total)
    }

    /// Formats the value for a `Content-Range` response header.
    ///
    /// The returned value is always valid ASCII and can be inserted directly
    /// into an HTTP header map.
    pub fn to_header_value(&self) -> HeaderValue {
        HeaderValue::from_str(&self.to_string())
            .expect("ContentRange always produces a valid header value")
    }
}

impl fmt::Display for ContentRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bytes {}-{}/{}", self.start, self.end, self.total)
    }
}

/// Errors that can occur when parsing a `Range` header.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RangeError {
    /// The value could not be parsed as a valid byte range.
    #[error("invalid byte range")]
    Invalid,
    /// The value contained multiple range specifiers separated by commas.
    #[error("expected single byte range, found multipart range")]
    MultiRange,
    /// The range unit is invalid
    #[error("invalid range unit: {0}, expected: bytes")]
    InvalidUnit(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_ranges() {
        assert_eq!(
            ByteRange::try_from("bytes=0-499"),
            Ok(ByteRange::Bounded(0, 499))
        );
        assert_eq!(ByteRange::try_from("bytes=500-"), Ok(ByteRange::From(500)));
        assert_eq!(ByteRange::try_from("bytes=-100"), Ok(ByteRange::Last(100)));
        // Case insensitive
        assert_eq!(
            ByteRange::try_from("Bytes=0-499"),
            Ok(ByteRange::Bounded(0, 499))
        );
        assert_eq!(ByteRange::try_from("BYTES=100-"), Ok(ByteRange::From(100)));
    }

    #[test]
    fn parse_invalid_ranges() {
        assert_eq!(
            ByteRange::try_from("bytes=0-10, 20-30"),
            Err(RangeError::MultiRange)
        );
        assert_eq!(
            ByteRange::try_from("items=0-10"),
            Err(RangeError::InvalidUnit("items".into()))
        );
        assert_eq!(
            ByteRange::try_from("bytes=500-100"),
            Err(RangeError::Invalid)
        );
        assert_eq!(ByteRange::try_from("bytes=-0"), Err(RangeError::Invalid));
    }

    #[test]
    fn resolve_satisfiable() {
        let cr = |start, end, total| Some(ContentRange { start, end, total });
        assert_eq!(ByteRange::Bounded(0, 499).resolve(1000), cr(0, 499, 1000));
        assert_eq!(ByteRange::Bounded(0, 9999).resolve(500), cr(0, 499, 500));
        assert_eq!(ByteRange::From(500).resolve(1000), cr(500, 999, 1000));
        assert_eq!(ByteRange::Last(100).resolve(1000), cr(900, 999, 1000));
        assert_eq!(ByteRange::Last(2000).resolve(1000), cr(0, 999, 1000));
    }

    #[test]
    fn resolve_unsatisfiable() {
        assert_eq!(ByteRange::Bounded(1000, 2000).resolve(500), None);
        assert_eq!(ByteRange::From(500).resolve(500), None);
        assert_eq!(ByteRange::Bounded(0, 0).resolve(0), None);
    }

    #[test]
    fn content_range_properties() {
        let full = ContentRange::full(1000);
        assert_eq!(
            full,
            ContentRange {
                start: 0,
                end: 999,
                total: 1000
            }
        );
        assert_eq!(full.len(), 1000);
        assert!(full.is_full());

        let partial = ContentRange {
            start: 0,
            end: 499,
            total: 1000,
        };
        assert_eq!(partial.len(), 500);
        assert!(!partial.is_full());

        let zero = ContentRange::full(0);
        assert_eq!(zero.len(), 0);
        assert!(zero.is_full());
    }

    #[test]
    fn header_value_roundtrips() {
        assert_eq!(ByteRange::Bounded(0, 499).to_header_value(), "bytes=0-499");
        assert_eq!(ByteRange::From(500).to_header_value(), "bytes=500-");
        assert_eq!(ByteRange::Last(100).to_header_value(), "bytes=-100");

        let cr = ContentRange {
            start: 0,
            end: 499,
            total: 1234,
        };
        assert_eq!(cr.to_header_value(), "bytes 0-499/1234");
        assert_eq!(ContentRange::parse("bytes 0-499/1234"), Some(cr));
        assert_eq!(ContentRange::parse("bytes */1234"), None);
        assert_eq!(ContentRange::parse("invalid"), None);
    }

    #[test]
    fn parse_unsatisfiable_total() {
        assert_eq!(
            ContentRange::parse_unsatisfiable_total("bytes */1234"),
            Some(1234)
        );
        assert_eq!(
            ContentRange::parse_unsatisfiable_total("bytes 0-499/1234"),
            None
        );
        assert_eq!(ContentRange::parse_unsatisfiable_total("invalid"), None);
    }
}
