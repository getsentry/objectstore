//! Escaping for free-form values carried in HTTP headers.
//!
//! HTTP header values carry only visible ASCII, so any value that may contain arbitrary Unicode —
//! an object key, a filename, a custom metadata value — has to be escaped to survive the
//! transport. This module owns that escaping for the whole workspace: it is the only place that
//! depends on [`percent_encoding`], so callers escape by name rather than by assembling a
//! character set of their own.
//!
//! Encoding is a property of the *transport*, never of the value: everything in memory holds the
//! logical string, and [`encode_header_value`] is applied only when writing a header.

use std::borrow::Cow;
use std::fmt;
use std::str::Utf8Error;

use http::HeaderValue;
use http::header::ToStrError;
use percent_encoding::{
    AsciiSet, CONTROLS, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode,
};

/// The characters escaped when a free-form value is written into a header value.
///
/// Non-ASCII bytes are escaped by the encoder itself; this set adds the C0 controls and `DEL`,
/// which a header value cannot carry, plus `%` so that a literal percent sign can never be
/// confused with an escape sequence when decoding.
///
/// Every other visible ASCII character is left alone, which keeps values that are already plain
/// ASCII byte-identical to their logical form on the wire.
const HEADER_ESCAPE: &AsciiSet = &CONTROLS.add(b'%');

/// The characters escaped in an [RFC 8187] `ext-value`.
///
/// This is the complement of the spec's `attr-char` production: alphanumerics plus a handful of
/// symbols survive, everything else — including the non-ASCII bytes this exists for — is escaped.
///
/// [RFC 8187]: https://www.rfc-editor.org/rfc/rfc8187
const EXT_VALUE_ESCAPE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'!')
    .remove(b'#')
    .remove(b'$')
    .remove(b'&')
    .remove(b'+')
    .remove(b'-')
    .remove(b'.')
    .remove(b'^')
    .remove(b'_')
    .remove(b'`')
    .remove(b'|')
    .remove(b'~');

/// Escapes a logical string into a header value.
///
/// This is the inverse of [`decode_header_value`]. Escaping cannot fail — the result is always
/// visible ASCII — so this returns the [`HeaderValue`] directly rather than a string a caller has
/// to parse and handle the impossible error of.
///
/// # Examples
///
/// ```
/// use objectstore_types::headers::encode_header_value;
///
/// assert_eq!(encode_header_value("report.pdf"), "report.pdf");
/// assert_eq!(encode_header_value("réport.pdf"), "r%C3%A9port.pdf");
/// assert_eq!(encode_header_value("100% done"), "100%25 done");
/// ```
pub fn encode_header_value(value: &str) -> HeaderValue {
    // INVARIANT: `HEADER_ESCAPE` escapes every byte a header value cannot carry — the controls,
    // `DEL`, and everything non-ASCII — so what is left is always visible ASCII.
    HeaderValue::from_str(&encode_header_str(value))
        .expect("escaped value is always a valid header value")
}

/// Escapes a logical string for a transport that is not an HTTP header.
///
/// Use this where the escaped form is needed as a string rather than a header — notably GCS object
/// metadata, which is written as JSON but has to match what the `x-goog-meta-*` headers carry.
/// Where the target *is* a header, prefer [`encode_header_value`].
///
/// Values that are already plain ASCII are returned borrowed and unchanged.
///
/// # Examples
///
/// ```
/// use objectstore_types::headers::encode_header_str;
///
/// assert_eq!(encode_header_str("report.pdf"), "report.pdf");
/// assert_eq!(encode_header_str("réport.pdf"), "r%C3%A9port.pdf");
/// ```
pub fn encode_header_str(value: &str) -> Cow<'_, str> {
    utf8_percent_encode(value, HEADER_ESCAPE).into()
}

/// The reasons a header value can fail to decode into a logical string.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The raw header value contained bytes outside visible ASCII.
    ///
    /// A conforming writer escapes those, so this means the value was not written by one.
    #[error("header value is not visible ASCII")]
    NotAscii(#[from] ToStrError),

    /// The escape sequences did not decode to valid UTF-8.
    #[error("header value is not valid percent-encoded UTF-8")]
    InvalidUtf8(#[from] Utf8Error),
}

/// Decodes a header value into the logical string it carries.
///
/// This is the inverse of [`encode_header_value`], and the counterpart most callers want: it
/// covers both ways a raw header can fail to be a logical string, so there is no separate
/// [`HeaderValue::to_str`] step to handle. Decoding does not depend on how aggressively the writer
/// escaped, so values written by older peers — or with a different escape set — read back
/// unchanged.
///
/// Callers are expected to wrap the error in one of their own that names the header at fault.
///
/// # Examples
///
/// ```
/// use http::HeaderValue;
/// use objectstore_types::headers::decode_header_value;
///
/// let value = HeaderValue::from_static("r%C3%A9port.pdf");
/// assert_eq!(decode_header_value(&value)?, "réport.pdf");
/// # Ok::<(), objectstore_types::headers::DecodeError>(())
/// ```
pub fn decode_header_value(value: &HeaderValue) -> Result<String, DecodeError> {
    Ok(decode_header_str(value.to_str()?)?)
}

/// Decodes an escaped string back into its logical form.
///
/// Use this for escaped values that do not arrive in an actual header — notably GCS object
/// metadata, which is read back as JSON. Where the value *is* a header, prefer
/// [`decode_header_value`], which also rejects raw bytes outside visible ASCII.
///
/// # Examples
///
/// ```
/// use objectstore_types::headers::decode_header_str;
///
/// assert_eq!(decode_header_str("r%C3%A9port.pdf")?, "réport.pdf");
/// assert_eq!(decode_header_str("100%25 done")?, "100% done");
/// assert!(decode_header_str("%FF.pdf").is_err());
/// # Ok::<(), std::str::Utf8Error>(())
/// ```
pub fn decode_header_str(value: &str) -> Result<String, Utf8Error> {
    Ok(percent_decode_str(value).decode_utf8()?.into_owned())
}

/// A logical string wrapped for use as an [RFC 8187] `ext-value`, in header parameters like
/// `Content-Disposition`'s `filename*`.
///
/// The string is escaped lazily as this is displayed, so a caller can write it straight into a
/// header it is already building instead of allocating an intermediate string. The output includes
/// the charset prefix, so it goes directly after the `=` of a header parameter.
///
/// Unlike [`encode_header_value`], this escapes everything outside a narrow `attr-char` set,
/// because an `ext-value` sits inside a header parameter rather than spanning a whole value.
///
/// [RFC 8187]: https://www.rfc-editor.org/rfc/rfc8187
///
/// # Examples
///
/// ```
/// use objectstore_types::headers::ExtValue;
///
/// assert_eq!(ExtValue("réport.pdf").to_string(), "UTF-8''r%C3%A9port.pdf");
/// ```
#[derive(Debug)]
pub struct ExtValue<'a>(pub &'a str);

impl fmt::Display for ExtValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UTF-8''")?;
        fmt::Display::fmt(&utf8_percent_encode(self.0, EXT_VALUE_ESCAPE), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_non_ascii() {
        assert_eq!(
            encode_header_value("réport-📄.pdf"),
            "r%C3%A9port-%F0%9F%93%84.pdf",
        );
    }

    #[test]
    fn encodes_percent() {
        assert_eq!(encode_header_value("100% done"), "100%25 done");
    }

    #[test]
    fn encodes_control_characters() {
        assert_eq!(encode_header_value("a\r\nb\tc\x7f"), "a%0D%0Ab%09c%7F");
    }

    #[test]
    fn encodes_to_a_borrowed_str_when_unchanged() {
        std::assert_matches!(encode_header_str("report.pdf"), Cow::Borrowed(_));
        std::assert_matches!(encode_header_str("réport.pdf"), Cow::Owned(_));
    }

    #[test]
    fn leaves_visible_ascii_alone() {
        // Must stay byte-identical so values written before this encoding existed are unaffected.
        let value = r#"has"quote path/to.txt!$&'()*+,;=:@?<>[]{}|^`~#"#;
        assert_eq!(encode_header_value(value), value);
    }

    #[test]
    fn roundtrips() {
        for value in [
            "réport-📄.pdf",
            "100% done",
            "50%.pdf",
            "plain.txt",
            "a\r\nb",
            "",
        ] {
            let encoded = encode_header_str(value);
            assert!(encoded.is_ascii(), "{encoded} is not ascii");
            assert_eq!(decode_header_str(&encoded).unwrap(), value);

            assert_eq!(
                decode_header_value(&encode_header_value(value)).unwrap(),
                value,
            );
        }
    }

    #[test]
    fn decodes_aggressively_escaped_values() {
        // Decoding is independent of the writer's escape set, which is what makes it safe to
        // change how much we escape without breaking peers.
        assert_eq!(decode_header_str("%6B%65%79%2D%31").unwrap(), "key-1");
    }

    #[test]
    fn decode_rejects_invalid_utf8() {
        let header = HeaderValue::from_static("%FF.pdf");
        std::assert_matches!(
            decode_header_value(&header),
            Err(DecodeError::InvalidUtf8(_)),
        );
        assert!(decode_header_str("%FF.pdf").is_err());
    }

    #[test]
    fn decode_rejects_raw_non_ascii() {
        // A conforming writer escapes these, so an unescaped byte means the value is malformed
        // rather than merely unencoded.
        let header = HeaderValue::from_bytes("réport.pdf".as_bytes()).unwrap();
        std::assert_matches!(decode_header_value(&header), Err(DecodeError::NotAscii(_)),);
    }

    #[test]
    fn ext_value_escapes_reserved_characters() {
        assert_eq!(
            ExtValue("réport 📄.pdf").to_string(),
            "UTF-8''r%C3%A9port%20%F0%9F%93%84.pdf",
        );
        assert_eq!(ExtValue("a\"b;c").to_string(), "UTF-8''a%22b%3Bc");
    }
}
