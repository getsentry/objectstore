//! Object key type with RFC 3986 percent-encoding.
//!
//! Object keys identify individual objects within a usecase and scope. This module
//! provides the [`ObjectKey`] newtype that enforces:
//!
//! - **Percent-encoding**: Only RFC 3986 unreserved characters (`A-Z a-z 0-9 - . _ ~`)
//!   appear literally; everything else is percent-encoded with uppercase hex digits.
//! - **Length limit**: Encoded keys must not exceed [`MAX_KEY_LENGTH`] (128) characters.
//! - **Non-empty**: Keys must contain at least one character.
//! - **No literal `/`**: Because `/` is always percent-encoded, storage paths are
//!   guaranteed to be flat (no accidental directory separators).
//!
//! # Wire form
//!
//! The encoded form is the canonical representation used on the wire (HTTP paths,
//! headers, JSON responses) and in storage backends. Clients receive the encoded form
//! and decode it for display.
//!
//! # Constructors
//!
//! - [`ObjectKey::new`] — Takes a raw UTF-8 string and percent-encodes it.
//! - [`ObjectKey::from_encoded`] — Takes a wire-form string, validates and normalizes it.
//! - [`ObjectKey::from_encoded_unchecked`] — For trusted input that is known to be valid
//!   (e.g. UUID v4 strings).

use std::fmt;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde::Serialize;

/// Maximum length of an encoded object key, in bytes.
pub const MAX_KEY_LENGTH: usize = 128;

/// The percent-encoding set: encode everything that is not an RFC 3986 unreserved character.
///
/// Unreserved characters are: `A-Z a-z 0-9 - . _ ~`
const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Errors that can occur when constructing an [`ObjectKey`].
#[derive(Debug, thiserror::Error)]
pub enum ObjectKeyError {
    /// The key is empty.
    #[error("object key must not be empty")]
    Empty,

    /// The encoded key exceeds [`MAX_KEY_LENGTH`].
    #[error("encoded object key is {0} bytes, exceeds maximum of {MAX_KEY_LENGTH}")]
    TooLong(usize),

    /// A character that is not unreserved and not `%` was found in an encoded key.
    #[error("invalid character {0:?} at byte position {1} in encoded key")]
    InvalidChar(char, usize),

    /// A `%` was found without two following hex digits.
    #[error("incomplete percent-encoding at byte position {0}")]
    IncompleteEncoding(usize),

    /// The two characters after `%` are not valid hex digits.
    #[error("invalid hex digits in percent-encoding at byte position {0}")]
    InvalidHex(usize),
}

/// A validated, percent-encoded object key.
///
/// Stores the normalized encoded wire form. See the [module docs](self) for encoding
/// rules and constructors.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Creates a new `ObjectKey` by percent-encoding a raw UTF-8 string.
    ///
    /// The input is encoded so that only unreserved characters appear literally.
    /// The result is validated for non-emptiness and length.
    pub fn new(raw: &str) -> Result<Self, ObjectKeyError> {
        if raw.is_empty() {
            return Err(ObjectKeyError::Empty);
        }

        let encoded = utf8_percent_encode(raw, ENCODE_SET).to_string();

        if encoded.len() > MAX_KEY_LENGTH {
            return Err(ObjectKeyError::TooLong(encoded.len()));
        }

        Ok(Self(encoded))
    }

    /// Creates an `ObjectKey` from an already-encoded wire-form string.
    ///
    /// Validates that the string contains only unreserved characters and valid
    /// `%XX` sequences, then normalizes by uppercasing hex digits and decoding
    /// encoded unreserved characters.
    pub fn from_encoded(wire: &str) -> Result<Self, ObjectKeyError> {
        if wire.is_empty() {
            return Err(ObjectKeyError::Empty);
        }

        // Validate the wire form
        validate_encoded(wire)?;

        // Normalize: decode the wire form back to raw bytes, then re-encode.
        // This uppercases hex digits and decodes needlessly-encoded unreserved chars.
        let decoded = percent_decode_str(wire)
            .decode_utf8()
            .map_err(|_| ObjectKeyError::InvalidHex(0))?;
        let normalized = utf8_percent_encode(&decoded, ENCODE_SET).to_string();

        if normalized.len() > MAX_KEY_LENGTH {
            return Err(ObjectKeyError::TooLong(normalized.len()));
        }

        Ok(Self(normalized))
    }

    /// Creates an `ObjectKey` from trusted input without validation.
    ///
    /// Use this only for values known to be valid encoded keys (e.g. UUID v4 strings).
    pub fn from_encoded_unchecked(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Returns the percent-encoded wire form.
    pub fn encoded(&self) -> &str {
        &self.0
    }

    /// Decodes the key back to its original UTF-8 representation.
    pub fn decoded(&self) -> String {
        percent_decode_str(&self.0)
            .decode_utf8()
            .expect("ObjectKey always contains valid percent-encoded UTF-8")
            .into_owned()
    }

    /// Converts the encoded key to an HTTP header value.
    ///
    /// This is infallible because the encoded form contains only ASCII visible characters.
    pub fn as_header_value(&self) -> http::HeaderValue {
        http::HeaderValue::from_str(&self.0)
            .expect("percent-encoded key is always valid ASCII VCHAR")
    }
}

/// Validates that a string is a valid percent-encoded key (only unreserved chars + valid `%XX`).
fn validate_encoded(s: &str) -> Result<(), ObjectKeyError> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            if i + 2 >= bytes.len() {
                return Err(ObjectKeyError::IncompleteEncoding(i));
            }
            if !bytes[i + 1].is_ascii_hexdigit() || !bytes[i + 2].is_ascii_hexdigit() {
                return Err(ObjectKeyError::InvalidHex(i));
            }
            i += 3;
        } else if is_unreserved(b) {
            i += 1;
        } else {
            return Err(ObjectKeyError::InvalidChar(b as char, i));
        }
    }
    Ok(())
}

/// Returns `true` if the byte is an RFC 3986 unreserved character.
fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_' || b == b'~'
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectKey({:?})", &self.0)
    }
}

impl Serialize for ObjectKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- ObjectKey::new tests ---

    #[test]
    fn new_plain_ascii() {
        let key = ObjectKey::new("hello-world_123.txt").unwrap();
        assert_eq!(key.encoded(), "hello-world_123.txt");
    }

    #[test]
    fn new_utf8() {
        let key = ObjectKey::new("caf\u{00e9}").unwrap();
        assert_eq!(key.encoded(), "caf%C3%A9");
    }

    #[test]
    fn new_reserved_chars() {
        let key = ObjectKey::new("foo/bar").unwrap();
        assert_eq!(key.encoded(), "foo%2Fbar");
    }

    #[test]
    fn new_spaces_and_special() {
        let key = ObjectKey::new("hello world!").unwrap();
        assert_eq!(key.encoded(), "hello%20world%21");
    }

    #[test]
    fn new_empty() {
        assert!(matches!(ObjectKey::new(""), Err(ObjectKeyError::Empty)));
    }

    #[test]
    fn new_too_long() {
        // A string that when encoded exceeds 128 chars
        let raw = "a".repeat(129);
        assert!(matches!(
            ObjectKey::new(&raw),
            Err(ObjectKeyError::TooLong(_))
        ));
    }

    #[test]
    fn new_unreserved_chars_literal() {
        let key = ObjectKey::new("AZaz09-._~").unwrap();
        assert_eq!(key.encoded(), "AZaz09-._~");
    }

    // --- ObjectKey::from_encoded tests ---

    #[test]
    fn from_encoded_valid() {
        let key = ObjectKey::from_encoded("hello%2Fworld").unwrap();
        assert_eq!(key.encoded(), "hello%2Fworld");
    }

    #[test]
    fn from_encoded_normalizes_lowercase_hex() {
        let key = ObjectKey::from_encoded("caf%c3%a9").unwrap();
        assert_eq!(key.encoded(), "caf%C3%A9");
    }

    #[test]
    fn from_encoded_normalizes_encoded_unreserved() {
        // 'a' is unreserved so %61 should become 'a'
        let key = ObjectKey::from_encoded("%61bc").unwrap();
        assert_eq!(key.encoded(), "abc");
    }

    #[test]
    fn from_encoded_empty() {
        assert!(matches!(
            ObjectKey::from_encoded(""),
            Err(ObjectKeyError::Empty)
        ));
    }

    #[test]
    fn from_encoded_invalid_char() {
        assert!(matches!(
            ObjectKey::from_encoded("foo/bar"),
            Err(ObjectKeyError::InvalidChar('/', _))
        ));
    }

    #[test]
    fn from_encoded_incomplete_percent() {
        assert!(matches!(
            ObjectKey::from_encoded("foo%2"),
            Err(ObjectKeyError::IncompleteEncoding(_))
        ));
    }

    #[test]
    fn from_encoded_invalid_hex() {
        assert!(matches!(
            ObjectKey::from_encoded("foo%GG"),
            Err(ObjectKeyError::InvalidHex(_))
        ));
    }

    #[test]
    fn from_encoded_too_long() {
        let long = "a".repeat(129);
        assert!(matches!(
            ObjectKey::from_encoded(&long),
            Err(ObjectKeyError::TooLong(_))
        ));
    }

    // --- Round-trip tests ---

    #[test]
    fn decoded_round_trips_with_new() {
        let raw = "caf\u{00e9}/latte";
        let key = ObjectKey::new(raw).unwrap();
        assert_eq!(key.decoded(), raw);
    }

    #[test]
    fn decoded_plain_ascii() {
        let key = ObjectKey::new("simple").unwrap();
        assert_eq!(key.decoded(), "simple");
    }

    // --- Header value test ---

    #[test]
    fn as_header_value_valid() {
        let key = ObjectKey::new("hello world").unwrap();
        let hv = key.as_header_value();
        assert_eq!(hv.to_str().unwrap(), "hello%20world");
    }

    // --- Serialize test ---

    #[test]
    fn serde_serializes_encoded() {
        let key = ObjectKey::new("café").unwrap();
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, r#""caf%C3%A9""#);
    }

    // --- Display and Debug ---

    #[test]
    fn display_outputs_encoded() {
        let key = ObjectKey::new("a b").unwrap();
        assert_eq!(format!("{key}"), "a%20b");
    }

    #[test]
    fn debug_format() {
        let key = ObjectKey::new("test").unwrap();
        assert_eq!(format!("{key:?}"), r#"ObjectKey("test")"#);
    }
}
