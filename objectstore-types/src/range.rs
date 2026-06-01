//! Types for HTTP range requests.

use std::fmt;
use std::str::FromStr;

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

impl ByteRange {
    /// Formats this range for a `Range` request header.
    pub fn to_header_value(&self) -> HeaderValue {
        let s = match self {
            ByteRange::Bounded(a, b) => format!("bytes={a}-{b}"),
            ByteRange::From(n) => format!("bytes={n}-"),
            ByteRange::Last(n) => format!("bytes=-{n}"),
        };
        HeaderValue::from_str(&s).expect("always a valid header value")
    }

    /// Resolves this range against a known total size, returning `None` if
    /// unsatisfiable (the object is empty, or the start offset is past the end).
    pub fn resolve(self, total: u64) -> Option<ContentRange> {
        if total == 0 {
            return None;
        }

        let (start, end) = match self {
            ByteRange::Bounded(start, end) => {
                if start >= total {
                    return None;
                }
                (start, end.min(total - 1)) // clamp
            }
            ByteRange::From(start) => {
                if start >= total {
                    return None;
                }
                (start, total - 1)
            }
            ByteRange::Last(negative_start) => {
                let start = total.saturating_sub(negative_start);
                (start, total - 1)
            }
        };

        Some(ContentRange { start, end, total })
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

/// Parses a `Range` request header value into a [`ByteRange`].
///
/// Only `bytes=` ranges with a single specifier are accepted.
/// Multiple ranges and non-`bytes` units are rejected.
impl FromStr for ByteRange {
    type Err = RangeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
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
            if last == 0 {
                return Err(RangeError::Invalid);
            }
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

/// Parses a `Content-Range` response header value into a [`ContentRange`].
impl FromStr for ContentRange {
    type Err = RangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parse = || {
            let rest = s.strip_prefix("bytes ")?;
            let (range_part, total_str) = rest.split_once('/')?;
            let total: u64 = total_str.parse().ok()?;
            let (start_str, end_str) = range_part.split_once('-')?;
            let start: u64 = start_str.parse().ok()?;
            let end: u64 = end_str.parse().ok()?;
            Some(Self { start, end, total })
        };
        parse().ok_or(RangeError::Invalid)
    }
}

impl ContentRange {
    /// Creates a [`ContentRange`] representing an entire object.
    pub fn full(total: u64) -> Self {
        Self {
            start: 0,
            end: total.saturating_sub(1),
            total,
        }
    }

    /// Parses the total from an unsatisfiable `Content-Range` response header value.
    ///
    /// An unsatisfiable `Content-Range` header value is of the form `bytes */1234`, where `1234`
    /// represents the total size of the object.
    /// This is communicated back to the client, so that it can make requests that make sense for
    /// that total.
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
        self.start == 0 && self.len() == self.total
    }

    /// Formats this range for a `Content-Range` response header.
    ///
    /// The returned value is always valid ASCII and can be inserted directly
    /// into an HTTP header map.
    pub fn to_header_value(&self) -> HeaderValue {
        HeaderValue::from_str(&self.to_string()).expect("always a valid header value")
    }

    /// Formats the length of this range for a `Content-Length` response header.
    pub fn len_to_header_value(&self) -> HeaderValue {
        HeaderValue::from_str(&self.len().to_string()).expect("always a valid header value")
    }
}

impl fmt::Display for ContentRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bytes {}-{}/{}", self.start, self.end, self.total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_ranges() {
        assert_eq!(
            "bytes=0-499".parse::<ByteRange>(),
            Ok(ByteRange::Bounded(0, 499))
        );
        assert_eq!("bytes=500-".parse::<ByteRange>(), Ok(ByteRange::From(500)));
        assert_eq!("bytes=-100".parse::<ByteRange>(), Ok(ByteRange::Last(100)));
        // Case insensitive
        assert_eq!(
            "Bytes=0-499".parse::<ByteRange>(),
            Ok(ByteRange::Bounded(0, 499))
        );
        assert_eq!("BYTES=100-".parse::<ByteRange>(), Ok(ByteRange::From(100)));
    }

    #[test]
    fn parse_invalid_ranges() {
        assert_eq!(
            "bytes=0-10, 20-30".parse::<ByteRange>(),
            Err(RangeError::MultiRange)
        );
        assert_eq!(
            "items=0-10".parse::<ByteRange>(),
            Err(RangeError::InvalidUnit("items".into()))
        );
        assert_eq!(
            "bytes=500-100".parse::<ByteRange>(),
            Err(RangeError::Invalid)
        );
        assert_eq!("bytes=-0".parse::<ByteRange>(), Err(RangeError::Invalid));
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
    fn content_range_methods() {
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
        assert_eq!("bytes 0-499/1234".parse::<ContentRange>(), Ok(cr));
        assert!("bytes */1234".parse::<ContentRange>().is_err());
        assert!("invalid".parse::<ContentRange>().is_err());
    }
}
