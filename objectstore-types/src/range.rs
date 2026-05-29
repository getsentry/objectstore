//! Types for HTTP range requests.

use std::fmt;

use http::header::HeaderValue;

/// A byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// Bounded range with start and end, inclusive
    Inclusive(u64, u64),
    /// From offset X onwards
    From(u64),
    /// Last X bytes
    Last(u64),
}

impl ByteRange {
    /// Formats this range as a `Range` request header value (e.g. `bytes=0-499`).
    ///
    /// The returned value is always valid ASCII and can be inserted directly
    /// into an HTTP header map.
    pub fn to_header_value(&self) -> HeaderValue {
        let s = match self {
            ByteRange::Inclusive(s, e) => format!("bytes={s}-{e}"),
            ByteRange::From(s) => format!("bytes={s}-"),
            ByteRange::Last(n) => format!("bytes=-{n}"),
        };
        // SAFETY: the format only contains ASCII digits, hyphens, and the
        // literal prefix "bytes=", which are all valid header value bytes.
        HeaderValue::from_str(&s).expect("ByteRange always produces a valid header value")
    }

    /// Resolves this range against a known total size, returning the concrete
    /// byte offsets and total, or [`None`] if the range is unsatisfiable.
    pub fn resolve(self, total: u64) -> Option<ContentRange> {
        if total == 0 {
            return None;
        }

        let (start, end) = match self {
            ByteRange::Inclusive(s, e) => {
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

/// Parses a `Range` request header string into a [`ByteRange`].
///
/// Only `bytes=` ranges with a single specifier are accepted. Multi-range
/// requests (containing commas) and non-`bytes` units are rejected.
///
/// Converting a [`http::header::HeaderValue`] to `&str` via
/// [`HeaderValue::to_str`] is the caller's responsibility, since that
/// conversion can fail when the value contains non-visible-ASCII bytes.
impl TryFrom<&str> for ByteRange {
    type Error = RangeError;

    fn try_from(header: &str) -> Result<Self, Self::Error> {
        let lower = header.to_ascii_lowercase();
        let spec = lower
            .strip_prefix("bytes=")
            .ok_or(RangeError::UnknownUnit)?;

        if spec.contains(',') {
            return Err(RangeError::MultiRangeNotSupported);
        }

        let (start_str, end_str) = spec.split_once('-').ok_or(RangeError::InvalidRange)?;

        if start_str.is_empty() {
            // bytes=-N (suffix / last N bytes)
            let n: u64 = end_str.parse().map_err(|_| RangeError::InvalidRange)?;
            if n == 0 {
                return Err(RangeError::InvalidRange);
            }
            Ok(ByteRange::Last(n))
        } else if end_str.is_empty() {
            // bytes=N- (from offset to end)
            let start: u64 = start_str.parse().map_err(|_| RangeError::InvalidRange)?;
            Ok(ByteRange::From(start))
        } else {
            // bytes=N-M (inclusive range)
            let start: u64 = start_str.parse().map_err(|_| RangeError::InvalidRange)?;
            let end: u64 = end_str.parse().map_err(|_| RangeError::InvalidRange)?;
            if start > end {
                return Err(RangeError::InvalidRange);
            }
            Ok(ByteRange::Inclusive(start, end))
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
        let s = format!("bytes {}-{}/{}", self.start, self.end, self.total);
        // SAFETY: the format only contains ASCII digits, spaces, hyphens,
        // and slashes, which are all valid header value bytes.
        HeaderValue::from_str(&s).expect("ContentRange always produces a valid header value")
    }
}

impl fmt::Display for ContentRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bytes {}-{}/{}", self.start, self.end, self.total)
    }
}

/// Errors that can occur when parsing a `Range` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeError {
    /// The header contained multiple range specifiers separated by commas.
    MultiRangeNotSupported,
    /// The header value could not be parsed as a valid byte range.
    InvalidRange,
    /// The range unit is not `bytes` (e.g. `items=0-10`). Per RFC 9110, unknown
    /// units should be ignored and the request served as a normal full-body response.
    UnknownUnit,
}

impl fmt::Display for RangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeError::MultiRangeNotSupported => {
                write!(f, "multi-range requests are not supported")
            }
            RangeError::InvalidRange => write!(f, "invalid Range header"),
            RangeError::UnknownUnit => write!(f, "unknown range unit"),
        }
    }
}

impl std::error::Error for RangeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_from_to() {
        assert_eq!(
            ByteRange::try_from("bytes=0-499"),
            Ok(ByteRange::Inclusive(0, 499))
        );
    }

    #[test]
    fn parse_from() {
        assert_eq!(ByteRange::try_from("bytes=500-"), Ok(ByteRange::From(500)));
    }

    #[test]
    fn parse_suffix() {
        assert_eq!(ByteRange::try_from("bytes=-100"), Ok(ByteRange::Last(100)));
    }

    #[test]
    fn parse_rejects_multi_range() {
        assert_eq!(
            ByteRange::try_from("bytes=0-10, 20-30"),
            Err(RangeError::MultiRangeNotSupported)
        );
    }

    #[test]
    fn parse_case_insensitive() {
        assert_eq!(
            ByteRange::try_from("Bytes=0-499"),
            Ok(ByteRange::Inclusive(0, 499))
        );
        assert_eq!(ByteRange::try_from("BYTES=100-"), Ok(ByteRange::From(100)));
    }

    #[test]
    fn parse_returns_unknown_unit_for_non_bytes() {
        assert_eq!(ByteRange::try_from("items=0-10"), Err(RangeError::UnknownUnit));
    }

    #[test]
    fn parse_rejects_inverted_range() {
        assert_eq!(
            ByteRange::try_from("bytes=500-100"),
            Err(RangeError::InvalidRange)
        );
    }

    #[test]
    fn parse_rejects_zero_suffix() {
        assert_eq!(ByteRange::try_from("bytes=-0"), Err(RangeError::InvalidRange));
    }

    #[test]
    fn resolve_from_to() {
        let range = ByteRange::Inclusive(0, 499).resolve(1000);
        assert_eq!(
            range,
            Some(ContentRange {
                start: 0,
                end: 499,
                total: 1000
            })
        );
    }

    #[test]
    fn resolve_from_to_clamped() {
        let range = ByteRange::Inclusive(0, 9999).resolve(500);
        assert_eq!(
            range,
            Some(ContentRange {
                start: 0,
                end: 499,
                total: 500
            })
        );
    }

    #[test]
    fn resolve_from() {
        let range = ByteRange::From(500).resolve(1000);
        assert_eq!(
            range,
            Some(ContentRange {
                start: 500,
                end: 999,
                total: 1000
            })
        );
    }

    #[test]
    fn resolve_suffix() {
        let range = ByteRange::Last(100).resolve(1000);
        assert_eq!(
            range,
            Some(ContentRange {
                start: 900,
                end: 999,
                total: 1000
            })
        );
    }

    #[test]
    fn resolve_suffix_larger_than_total() {
        let range = ByteRange::Last(2000).resolve(1000);
        assert_eq!(
            range,
            Some(ContentRange {
                start: 0,
                end: 999,
                total: 1000
            })
        );
    }

    #[test]
    fn resolve_unsatisfiable() {
        assert_eq!(ByteRange::Inclusive(1000, 2000).resolve(500), None);
        assert_eq!(ByteRange::From(500).resolve(500), None);
        assert_eq!(ByteRange::Inclusive(0, 0).resolve(0), None);
    }

    #[test]
    fn content_range_full() {
        let cr = ContentRange::full(1000);
        assert_eq!(cr.start, 0);
        assert_eq!(cr.end, 999);
        assert_eq!(cr.total, 1000);
        assert_eq!(cr.len(), 1000);
        assert!(cr.is_full());
        assert_eq!(cr.to_header_value(), "bytes 0-999/1000");
    }

    #[test]
    fn content_range_partial_is_not_full() {
        let cr = ContentRange {
            start: 0,
            end: 499,
            total: 1000,
        };
        assert!(!cr.is_full());
        assert_eq!(cr.len(), 500);
    }

    #[test]
    fn content_range_full_zero_bytes() {
        let cr = ContentRange::full(0);
        assert_eq!(cr.len(), 0);
        assert!(cr.is_full());
    }

    #[test]
    fn byte_range_to_header_value() {
        assert_eq!(
            ByteRange::Inclusive(0, 499).to_header_value(),
            "bytes=0-499"
        );
        assert_eq!(ByteRange::From(500).to_header_value(), "bytes=500-");
        assert_eq!(ByteRange::Last(100).to_header_value(), "bytes=-100");
    }

    #[test]
    fn content_range_parse() {
        assert_eq!(
            ContentRange::parse("bytes 0-499/1234"),
            Some(ContentRange {
                start: 0,
                end: 499,
                total: 1234
            })
        );
        assert_eq!(ContentRange::parse("bytes */1234"), None);
        assert_eq!(ContentRange::parse("invalid"), None);
    }

    #[test]
    fn content_range_parse_unsatisfiable_total() {
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
