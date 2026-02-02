//! Object key definitions and validation.
//!
//! This module provides the [`ObjectKey`] type for representing validated object keys
//! according to RFC 3986 and the objectstore specification.

use std::fmt::{self, Write};
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum length of an object key in ASCII characters (including percent encoding).
pub const MAX_KEY_LENGTH: usize = 128;

/// RFC 3986 unreserved characters: ALPHA / DIGIT / "-" / "." / "_" / "~"
const fn is_unreserved(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~')
}

/// RFC 3986 reserved characters (gen-delims / sub-delims).
/// gen-delims: ":" / "/" / "?" / "#" / "[" / "]" / "@"
/// sub-delims: "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
const fn is_reserved(c: char) -> bool {
    matches!(
        c,
        ':' | '/'
            | '?'
            | '#'
            | '['
            | ']'
            | '@'
            | '!'
            | '$'
            | '&'
            | '\''
            | '('
            | ')'
            | '*'
            | '+'
            | ','
            | ';'
            | '='
    )
}

/// A validated and normalized object key.
///
/// Object keys follow RFC 3986 encoding rules:
/// - Keys are limited to 128 ASCII characters (including percent encoding)
/// - Reserved characters must be percent-encoded
/// - Unreserved characters may be percent-encoded and will be normalized (decoded)
///
/// The key is stored in its normalized, encoded form where:
/// - Reserved characters are percent-encoded
/// - Unreserved characters are decoded (not percent-encoded)
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    /// The normalized, encoded key.
    inner: String,
}

impl ObjectKey {
    /// Creates an `ObjectKey` from a raw (unencoded) string.
    ///
    /// This is intended for client-side use where users provide human-readable keys.
    /// Reserved characters will be automatically percent-encoded.
    ///
    /// # Examples
    ///
    /// ```
    /// use objectstore_types::key::ObjectKey;
    ///
    /// // Simple key
    /// let key = ObjectKey::from_raw("my-object").unwrap();
    /// assert_eq!(key.as_str(), "my-object");
    ///
    /// // Key with slash (gets encoded)
    /// let key = ObjectKey::from_raw("path/to/object").unwrap();
    /// assert_eq!(key.as_str(), "path%2Fto%2Fobject");
    ///
    /// // Empty keys are invalid
    /// assert!(ObjectKey::from_raw("").is_err());
    /// ```
    pub fn from_raw(raw: &str) -> Result<Self, InvalidKeyError> {
        if raw.is_empty() {
            return Err(InvalidKeyError::Empty);
        }

        // Build the encoded key
        let mut encoded = String::with_capacity(raw.len());
        for c in raw.chars() {
            if !c.is_ascii() {
                return Err(InvalidKeyError::NonAscii(c));
            }
            if is_unreserved(c) {
                encoded.push(c);
            } else if c == '%' {
                // Percent sign itself needs to be encoded
                encoded.push_str("%25");
            } else if is_reserved(c) || !c.is_ascii_graphic() {
                // Encode reserved chars and non-graphic ASCII (spaces, control chars)
                percent_encode_char(c, &mut encoded);
            } else {
                // Other printable ASCII that's not reserved or unreserved
                // These should also be encoded for safety
                percent_encode_char(c, &mut encoded);
            }
        }

        if encoded.len() > MAX_KEY_LENGTH {
            return Err(InvalidKeyError::TooLong {
                length: encoded.len(),
            });
        }

        Ok(Self { inner: encoded })
    }

    /// Creates an `ObjectKey` from an already percent-encoded string.
    ///
    /// This is intended for server-side use where keys come from HTTP requests
    /// and are already URL-encoded. The key will be validated and normalized.
    ///
    /// # Normalization
    ///
    /// - Percent-encoded unreserved characters are decoded (`%41` → `A`)
    /// - Percent-encoded reserved characters remain encoded (`%2F` stays `%2F`)
    /// - Percent hex digits are uppercased (`%2f` → `%2F`)
    ///
    /// # Examples
    ///
    /// ```
    /// use objectstore_types::key::ObjectKey;
    ///
    /// // Already properly encoded
    /// let key = ObjectKey::from_encoded("path%2Fto%2Fobject").unwrap();
    /// assert_eq!(key.as_str(), "path%2Fto%2Fobject");
    ///
    /// // Unreserved chars get decoded
    /// let key = ObjectKey::from_encoded("hello%41world").unwrap();
    /// assert_eq!(key.as_str(), "helloAworld");
    ///
    /// // Literal slash is invalid
    /// assert!(ObjectKey::from_encoded("path/to/object").is_err());
    /// ```
    pub fn from_encoded(encoded: &str) -> Result<Self, InvalidKeyError> {
        if encoded.is_empty() {
            return Err(InvalidKeyError::Empty);
        }

        if encoded.len() > MAX_KEY_LENGTH {
            return Err(InvalidKeyError::TooLong {
                length: encoded.len(),
            });
        }

        // Validate and normalize the encoded string
        let mut normalized = String::with_capacity(encoded.len());
        let mut chars = encoded.chars().peekable();

        while let Some(c) = chars.next() {
            if !c.is_ascii() {
                return Err(InvalidKeyError::NonAscii(c));
            }

            if c == '%' {
                // Parse percent-encoded sequence
                let hex1 = chars
                    .next()
                    .ok_or(InvalidKeyError::InvalidPercentEncoding)?;
                let hex2 = chars
                    .next()
                    .ok_or(InvalidKeyError::InvalidPercentEncoding)?;

                if !hex1.is_ascii_hexdigit() || !hex2.is_ascii_hexdigit() {
                    return Err(InvalidKeyError::InvalidPercentEncoding);
                }

                let byte = u8::from_str_radix(&format!("{hex1}{hex2}"), 16)
                    .map_err(|_| InvalidKeyError::InvalidPercentEncoding)?;
                let decoded_char = byte as char;

                if is_unreserved(decoded_char) {
                    // Decode unreserved characters
                    normalized.push(decoded_char);
                } else {
                    // Keep reserved/other characters encoded, with uppercase hex
                    normalized.push('%');
                    normalized.push(hex1.to_ascii_uppercase());
                    normalized.push(hex2.to_ascii_uppercase());
                }
            } else if is_reserved(c) {
                // Literal reserved character is not allowed
                return Err(InvalidKeyError::InvalidChar(c));
            } else if is_unreserved(c) {
                normalized.push(c);
            } else if !c.is_ascii_graphic() {
                // Non-graphic characters (spaces, control chars) must be encoded
                return Err(InvalidKeyError::InvalidChar(c));
            } else {
                // Other ASCII characters - allow them through
                normalized.push(c);
            }
        }

        Ok(Self { inner: normalized })
    }

    /// Returns the normalized, encoded key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.inner
    }

    /// Consumes the `ObjectKey` and returns the inner string.
    pub fn into_inner(self) -> String {
        self.inner
    }

}

/// Percent-encodes a single character and appends it to the output.
fn percent_encode_char(c: char, output: &mut String) {
    output.push('%');
    output.push_str(&format!("{:02X}", c as u8));
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display the human-readable (decoded) form
        let mut chars = self.inner.chars();
        while let Some(c) = chars.next() {
            if c == '%' {
                // We know the encoding is valid since we validated on construction
                let hex1 = chars.next().expect("valid percent encoding");
                let hex2 = chars.next().expect("valid percent encoding");
                let byte = u8::from_str_radix(&format!("{hex1}{hex2}"), 16)
                    .expect("valid hex digits");
                f.write_char(byte as char)?;
            } else {
                f.write_char(c)?;
            }
        }
        Ok(())
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        &self.inner
    }
}

impl Deref for ObjectKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl FromStr for ObjectKey {
    type Err = InvalidKeyError;

    /// Parses an encoded key string.
    ///
    /// This is equivalent to [`ObjectKey::from_encoded`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_encoded(s)
    }
}

impl Serialize for ObjectKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.inner)
    }
}

impl<'de> Deserialize<'de> for ObjectKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ObjectKey::from_encoded(&s).map_err(serde::de::Error::custom)
    }
}

/// An error indicating that an object key is invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidKeyError {
    /// The key is empty.
    #[error("key must not be empty")]
    Empty,

    /// The key exceeds the maximum length.
    #[error("key exceeds maximum length of {MAX_KEY_LENGTH} characters (got {length})")]
    TooLong {
        /// The actual length of the key.
        length: usize,
    },

    /// The key contains a non-ASCII character.
    #[error("key contains non-ASCII character '{0}'")]
    NonAscii(char),

    /// The key contains an invalid literal character that should be percent-encoded.
    #[error("key contains invalid character '{0}' (must be percent-encoded)")]
    InvalidChar(char),

    /// The key contains invalid percent encoding.
    #[error("key contains invalid percent encoding")]
    InvalidPercentEncoding,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== from_raw tests ====================

    #[test]
    fn test_from_raw_simple() {
        let key = ObjectKey::from_raw("my-object").unwrap();
        assert_eq!(key.as_str(), "my-object");
    }

    #[test]
    fn test_from_raw_with_unreserved() {
        let key = ObjectKey::from_raw("file_name-v1.2.3~beta").unwrap();
        assert_eq!(key.as_str(), "file_name-v1.2.3~beta");
    }

    #[test]
    fn test_from_raw_encodes_slash() {
        let key = ObjectKey::from_raw("path/to/object").unwrap();
        assert_eq!(key.as_str(), "path%2Fto%2Fobject");
    }

    #[test]
    fn test_from_raw_encodes_reserved() {
        let key = ObjectKey::from_raw("key?query=value").unwrap();
        assert_eq!(key.as_str(), "key%3Fquery%3Dvalue");
    }

    #[test]
    fn test_from_raw_encodes_space() {
        let key = ObjectKey::from_raw("hello world").unwrap();
        assert_eq!(key.as_str(), "hello%20world");
    }

    #[test]
    fn test_from_raw_encodes_percent() {
        let key = ObjectKey::from_raw("100%").unwrap();
        assert_eq!(key.as_str(), "100%25");
    }

    #[test]
    fn test_from_raw_empty() {
        let err = ObjectKey::from_raw("").unwrap_err();
        assert!(matches!(err, InvalidKeyError::Empty));
    }

    #[test]
    fn test_from_raw_too_long() {
        let long_key = "a".repeat(MAX_KEY_LENGTH + 1);
        let err = ObjectKey::from_raw(&long_key).unwrap_err();
        assert!(matches!(err, InvalidKeyError::TooLong { .. }));
    }

    #[test]
    fn test_from_raw_non_ascii() {
        let err = ObjectKey::from_raw("hello\u{00e9}world").unwrap_err();
        assert!(matches!(err, InvalidKeyError::NonAscii(_)));
    }

    #[test]
    fn test_from_raw_max_length() {
        // Key that's exactly at the limit
        let key = "a".repeat(MAX_KEY_LENGTH);
        assert!(ObjectKey::from_raw(&key).is_ok());
    }

    #[test]
    fn test_from_raw_encoding_increases_length() {
        // 43 slashes become 43 * 3 = 129 chars, exceeding limit
        let key = "/".repeat(43);
        let err = ObjectKey::from_raw(&key).unwrap_err();
        assert!(matches!(err, InvalidKeyError::TooLong { .. }));
    }

    // ==================== from_encoded tests ====================

    #[test]
    fn test_from_encoded_simple() {
        let key = ObjectKey::from_encoded("my-object").unwrap();
        assert_eq!(key.as_str(), "my-object");
    }

    #[test]
    fn test_from_encoded_with_encoded_slash() {
        let key = ObjectKey::from_encoded("path%2Fto%2Fobject").unwrap();
        assert_eq!(key.as_str(), "path%2Fto%2Fobject");
    }

    #[test]
    fn test_from_encoded_normalizes_unreserved() {
        // %41 is 'A', which is unreserved, so it should be decoded
        let key = ObjectKey::from_encoded("hello%41world").unwrap();
        assert_eq!(key.as_str(), "helloAworld");
    }

    #[test]
    fn test_from_encoded_keeps_reserved_encoded() {
        // %2F is '/', which is reserved, so it should stay encoded
        let key = ObjectKey::from_encoded("path%2Fobject").unwrap();
        assert_eq!(key.as_str(), "path%2Fobject");
    }

    #[test]
    fn test_from_encoded_uppercases_hex() {
        let key = ObjectKey::from_encoded("path%2fobject").unwrap();
        assert_eq!(key.as_str(), "path%2Fobject");
    }

    #[test]
    fn test_from_encoded_literal_slash_invalid() {
        let err = ObjectKey::from_encoded("path/to/object").unwrap_err();
        assert!(matches!(err, InvalidKeyError::InvalidChar('/')));
    }

    #[test]
    fn test_from_encoded_literal_question_invalid() {
        let err = ObjectKey::from_encoded("key?query").unwrap_err();
        assert!(matches!(err, InvalidKeyError::InvalidChar('?')));
    }

    #[test]
    fn test_from_encoded_empty() {
        let err = ObjectKey::from_encoded("").unwrap_err();
        assert!(matches!(err, InvalidKeyError::Empty));
    }

    #[test]
    fn test_from_encoded_too_long() {
        let long_key = "a".repeat(MAX_KEY_LENGTH + 1);
        let err = ObjectKey::from_encoded(&long_key).unwrap_err();
        assert!(matches!(err, InvalidKeyError::TooLong { .. }));
    }

    #[test]
    fn test_from_encoded_invalid_percent_truncated() {
        let err = ObjectKey::from_encoded("hello%2").unwrap_err();
        assert!(matches!(err, InvalidKeyError::InvalidPercentEncoding));
    }

    #[test]
    fn test_from_encoded_invalid_percent_chars() {
        let err = ObjectKey::from_encoded("hello%GG").unwrap_err();
        assert!(matches!(err, InvalidKeyError::InvalidPercentEncoding));
    }

    #[test]
    fn test_from_encoded_non_ascii() {
        let err = ObjectKey::from_encoded("hello\u{00e9}").unwrap_err();
        assert!(matches!(err, InvalidKeyError::NonAscii(_)));
    }

    #[test]
    fn test_from_encoded_space_invalid() {
        let err = ObjectKey::from_encoded("hello world").unwrap_err();
        assert!(matches!(err, InvalidKeyError::InvalidChar(' ')));
    }

    // ==================== roundtrip tests ====================

    #[test]
    fn test_roundtrip_simple() {
        let key = ObjectKey::from_raw("my-object").unwrap();
        let parsed = ObjectKey::from_encoded(key.as_str()).unwrap();
        assert_eq!(key, parsed);
    }

    #[test]
    fn test_roundtrip_with_slash() {
        let key = ObjectKey::from_raw("path/to/object").unwrap();
        let parsed = ObjectKey::from_encoded(key.as_str()).unwrap();
        assert_eq!(key, parsed);
    }

    #[test]
    fn test_roundtrip_complex() {
        let key = ObjectKey::from_raw("my file (1).txt").unwrap();
        let parsed = ObjectKey::from_encoded(key.as_str()).unwrap();
        assert_eq!(key, parsed);
    }

    // ==================== serialization tests ====================

    #[test]
    fn test_serialize() {
        let key = ObjectKey::from_raw("path/to/object").unwrap();
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, r#""path%2Fto%2Fobject""#);
    }

    #[test]
    fn test_deserialize() {
        let key: ObjectKey = serde_json::from_str(r#""path%2Fto%2Fobject""#).unwrap();
        assert_eq!(key.as_str(), "path%2Fto%2Fobject");
    }

    #[test]
    fn test_deserialize_normalizes() {
        let key: ObjectKey = serde_json::from_str(r#""hello%41world""#).unwrap();
        assert_eq!(key.as_str(), "helloAworld");
    }

    #[test]
    fn test_deserialize_invalid() {
        let result: Result<ObjectKey, _> = serde_json::from_str(r#""path/object""#);
        assert!(result.is_err());
    }

    // ==================== trait tests ====================

    #[test]
    fn test_from_str() {
        let key: ObjectKey = "my-object".parse().unwrap();
        assert_eq!(key.as_str(), "my-object");
    }

    #[test]
    fn test_display() {
        let key = ObjectKey::from_raw("path/to/object").unwrap();
        // Display returns the human-readable (decoded) form
        assert_eq!(format!("{key}"), "path/to/object");
    }

    #[test]
    fn test_deref() {
        let key = ObjectKey::from_raw("my-object").unwrap();
        assert!(key.starts_with("my-"));
    }

    // ==================== edge cases ====================

    #[test]
    fn test_all_unreserved_chars() {
        let unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
        let key = ObjectKey::from_raw(unreserved).unwrap();
        assert_eq!(key.as_str(), unreserved);

        let key = ObjectKey::from_encoded(unreserved).unwrap();
        assert_eq!(key.as_str(), unreserved);
    }

    #[test]
    fn test_all_reserved_gen_delims_encoded() {
        // gen-delims: : / ? # [ ] @
        for c in [':', '/', '?', '#', '[', ']', '@'] {
            let raw = format!("a{c}b");
            let key = ObjectKey::from_raw(&raw).unwrap();
            assert!(!key.as_str().contains(c), "char {c} should be encoded");
        }
    }

    #[test]
    fn test_all_reserved_sub_delims_encoded() {
        // sub-delims: ! $ & ' ( ) * + , ; =
        for c in ['!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '='] {
            let raw = format!("a{c}b");
            let key = ObjectKey::from_raw(&raw).unwrap();
            assert!(!key.as_str().contains(c), "char {c} should be encoded");
        }
    }

    #[test]
    fn test_encoded_percent_stays_encoded() {
        // %25 is '%', which should stay encoded
        let key = ObjectKey::from_encoded("100%25").unwrap();
        assert_eq!(key.as_str(), "100%25");
    }

    #[test]
    fn test_mixed_encoding() {
        // Mix of encoded reserved, encoded unreserved (to be decoded), and plain unreserved
        let key = ObjectKey::from_encoded("a%2Fb%41c").unwrap();
        // %2F stays (reserved), %41 becomes A (unreserved)
        assert_eq!(key.as_str(), "a%2FbAc");
    }
}
