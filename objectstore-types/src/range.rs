//! HTTP Range request types for partial content retrieval.
//!
//! [`ByteRange`] represents a parsed single byte-range from a `Range` HTTP
//! header. [`ContentRange`] describes which portion of an object is present in
//! a response body, used to build `Content-Range` response headers and choose
//! between 200 and 206 status codes.

use std::fmt;

/// A single byte-range parsed from the `Range` HTTP request header.
///
/// Only the `bytes` range unit is supported, and only a single range specifier
/// (multi-range requests are rejected at parse time).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=N-M` — from byte offset N to byte offset M (both inclusive).
    FromTo(u64, u64),
    /// `bytes=N-` — from byte offset N to the end of the representation.
    From(u64),
    /// `bytes=-N` — the last N bytes of the representation.
    Suffix(u64),
}

impl ByteRange {
    /// Parses a `Range` header value into a [`ByteRange`].
    ///
    /// Only `bytes=` ranges with a single specifier are accepted. Multi-range
    /// requests (containing commas) and non-`bytes` units are rejected.
    pub fn parse(header: &str) -> Result<Self, RangeError> {
        let spec = header
            .strip_prefix("bytes=")
            .ok_or(RangeError::UnknownUnit)?;

        if spec.contains(',') {
            return Err(RangeError::MultiRangeNotSupported);
        }

        let (start_str, end_str) = spec.split_once('-').ok_or(RangeError::InvalidRange)?;

        if start_str.is_empty() {
            // bytes=-N (suffix)
            let n: u64 = end_str.parse().map_err(|_| RangeError::InvalidRange)?;
            if n == 0 {
                return Err(RangeError::InvalidRange);
            }
            Ok(ByteRange::Suffix(n))
        } else if end_str.is_empty() {
            // bytes=N- (from offset to end)
            let start: u64 = start_str.parse().map_err(|_| RangeError::InvalidRange)?;
            Ok(ByteRange::From(start))
        } else {
            // bytes=N-M
            let start: u64 = start_str.parse().map_err(|_| RangeError::InvalidRange)?;
            let end: u64 = end_str.parse().map_err(|_| RangeError::InvalidRange)?;
            if start > end {
                return Err(RangeError::InvalidRange);
            }
            Ok(ByteRange::FromTo(start, end))
        }
    }

    /// Formats this range as a `Range` header value (e.g. `bytes=0-499`).
    pub fn to_header_value(&self) -> String {
        match self {
            ByteRange::FromTo(s, e) => format!("bytes={s}-{e}"),
            ByteRange::From(s) => format!("bytes={s}-"),
            ByteRange::Suffix(n) => format!("bytes=-{n}"),
        }
    }

    /// Resolves this range against a known total size, returning the concrete
    /// byte offsets and total, or `None` if the range is unsatisfiable.
    pub fn resolve(self, total: u64) -> Option<ContentRange> {
        if total == 0 {
            return None;
        }

        let (start, end) = match self {
            ByteRange::FromTo(s, e) => {
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
            ByteRange::Suffix(n) => {
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
    pub fn to_header_value(&self) -> String {
        format!("bytes {}-{}/{}", self.start, self.end, self.total)
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
            ByteRange::parse("bytes=0-499"),
            Ok(ByteRange::FromTo(0, 499))
        );
    }

    #[test]
    fn parse_from() {
        assert_eq!(ByteRange::parse("bytes=500-"), Ok(ByteRange::From(500)));
    }

    #[test]
    fn parse_suffix() {
        assert_eq!(ByteRange::parse("bytes=-100"), Ok(ByteRange::Suffix(100)));
    }

    #[test]
    fn parse_rejects_multi_range() {
        assert_eq!(
            ByteRange::parse("bytes=0-10, 20-30"),
            Err(RangeError::MultiRangeNotSupported)
        );
    }

    #[test]
    fn parse_returns_unknown_unit_for_non_bytes() {
        assert_eq!(ByteRange::parse("items=0-10"), Err(RangeError::UnknownUnit));
    }

    #[test]
    fn parse_rejects_inverted_range() {
        assert_eq!(
            ByteRange::parse("bytes=500-100"),
            Err(RangeError::InvalidRange)
        );
    }

    #[test]
    fn parse_rejects_zero_suffix() {
        assert_eq!(ByteRange::parse("bytes=-0"), Err(RangeError::InvalidRange));
    }

    #[test]
    fn resolve_from_to() {
        let range = ByteRange::FromTo(0, 499).resolve(1000);
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
        let range = ByteRange::FromTo(0, 9999).resolve(500);
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
        let range = ByteRange::Suffix(100).resolve(1000);
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
        let range = ByteRange::Suffix(2000).resolve(1000);
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
        assert_eq!(ByteRange::FromTo(1000, 2000).resolve(500), None);
        assert_eq!(ByteRange::From(500).resolve(500), None);
        assert_eq!(ByteRange::FromTo(0, 0).resolve(0), None);
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
        assert_eq!(ByteRange::FromTo(0, 499).to_header_value(), "bytes=0-499");
        assert_eq!(ByteRange::From(500).to_header_value(), "bytes=500-");
        assert_eq!(ByteRange::Suffix(100).to_header_value(), "bytes=-100");
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
