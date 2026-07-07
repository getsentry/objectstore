//! Utilities for signing and verifying pre-signed URLs.
//!
//! A pre-signed URL lets a client that owns an Ed25519 keypair hand out a
//! time-limited URL that authorizes a specific request.
//!
//! Example:
//!
//! ```text
//! GET /v1/objects/<usecase>/<scopes>/<key>
//!     ?X-Os-Timestamp=2026-04-20T13:37:00.00Z
//!     &X-Os-Expires=3600
//!     &X-Os-Signed-Headers=host
//!     &X-Os-Key-Id=relay
//!     &X-Os-Alg=Ed25519
//!     &X-Os-Sig=<signature>
//! ```
//!
//! The signature covers a **canonical form** (see below) of the request.
//! `X-Os-Alg` is currently ignored, and intended for potential future use.
//!
//! # Canonical form
//!
//! The canonical form is comprised of four newline-separated components:
//!
//! ```text
//! <canonical method>\n
//! <canonical path>\n
//! <canonical query string>\n
//! <canonical headers>
//! ```
//!
//! - canonical method: the uppercase HTTP method, with `HEAD` mapped to `GET`;
//! - canonical path: the percent-encoded request path;
//! - canonical query string: every query parameter except `X-Os-Sig`,
//!   with keys and values percent-encoded, sorted by encoded key then
//!   encoded value, joined as `key=value` pairs with `&` separator.
//! - canonical headers: the request headers named in `X-Os-Signed-Headers`, each
//!   rendered as `name:value` with a lowercase name and its value's surrounding
//!   whitespace stripped, sorted by name, and joined with a `\n` separator.
//!   When no headers are signed this component is empty, so the canonical form ends
//!   with a trailing newline.
//!
//! Percent encoding of the path and query is always performed using the
//! [`NON_ALPHANUMERIC`] character set into uppercase hex digits.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Signer};
use http::{HeaderMap, HeaderName, Method};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, percent_encode};

pub use ed25519_dalek::{SigningKey, VerifyingKey};

/// Query parameter carrying the time at which the request was signed.
pub const X_OS_TIMESTAMP: &str = "X-Os-Timestamp";

/// Query parameter carrying the validity duration, in seconds.
pub const X_OS_EXPIRES: &str = "X-Os-Expires";

/// Query parameter carrying the list of signed headers.
pub const X_OS_SIGNED_HEADERS: &str = "X-Os-Signed-Headers";

/// Query parameter naming the key ID used to sign the request.
pub const X_OS_KEY_ID: &str = "X-Os-Key-Id";

/// Query parameter carrying the signing algorithm specifier.
pub const X_OS_ALG: &str = "X-Os-Alg";

/// Query parameter carrying the base64url-encoded signature.
pub const X_OS_SIG: &str = "X-Os-Sig";

/// Errors returned when verifying a pre-signed request signature.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The signature was not valid base64url or did not decode to a 64-byte
    /// Ed25519 signature.
    #[error("invalid signature encoding")]
    InvalidSignatureEncoding,
    /// A signed header's value was not valid text (i.e. not printable ASCII).
    #[error("signed header value is not valid text")]
    InvalidHeaderValue,
    /// The signature did not match the canonical request for the given key.
    #[error("signature verification failed")]
    VerificationFailed,
}

/// The canonical form of a request to be signed or verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRequest(String);

impl CanonicalRequest {
    /// Builds the canonical form of a request.
    ///
    /// See the [module-level documentation](self) for the exact format.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidHeaderValue`] if a signed header's value is not
    /// printable ASCII.
    pub fn new(
        method: &Method,
        path: &str,
        query: Option<&str>,
        headers: &HeaderMap,
        signed_headers: &[&HeaderName],
    ) -> Result<Self, Error> {
        let canonical_method = if *method == Method::HEAD {
            "GET"
        } else {
            method.as_str()
        };

        let canonical_path = percent_encode(path.as_bytes(), NON_ALPHANUMERIC).to_string();

        let mut pairs: Vec<(String, String)> = Vec::new();
        for pair in query.unwrap_or_default().split('&') {
            if pair.is_empty() {
                continue;
            }
            let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = percent_decode_str(raw_key).decode_utf8_lossy();
            if key.as_ref() == X_OS_SIG {
                continue;
            }
            let value = percent_decode_str(raw_value).decode_utf8_lossy();
            pairs.push((
                percent_encode(key.as_bytes(), NON_ALPHANUMERIC).to_string(),
                percent_encode(value.as_bytes(), NON_ALPHANUMERIC).to_string(),
            ));
        }
        pairs.sort();

        let canonical_query = pairs
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");

        let mut header_pairs: Vec<(&str, &str)> = Vec::with_capacity(signed_headers.len());
        for name in signed_headers {
            if let Some(value) = headers.get(*name) {
                let value = value.to_str().map_err(|_| Error::InvalidHeaderValue)?;
                header_pairs.push((name.as_str(), value.trim()));
            }
        }
        header_pairs.sort();

        let canonical_headers = header_pairs
            .iter()
            .map(|(name, value)| format!("{name}:{value}"))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(Self(format!(
            "{canonical_method}\n{canonical_path}\n{canonical_query}\n{canonical_headers}"
        )))
    }

    /// Returns the canonical form as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Signs this canonical form with Ed25519.
    ///
    /// Returns the base64url-encoded  signature, suitable as the value of the
    /// [`X_OS_SIG`] query parameter.
    pub fn sign(&self, key: &SigningKey) -> String {
        let signature = key.sign(self.0.as_bytes());
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    }

    /// Verifies a base64url-encoded Ed25519 signature against this canonical form.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidSignatureEncoding`] if `signature_b64` is not valid
    /// base64url or not a 64-byte signature, and [`Error::VerificationFailed`] if
    /// the signature does not match.
    pub fn verify(&self, key: &VerifyingKey, signature_b64: &str) -> Result<(), Error> {
        let bytes = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|_| Error::InvalidSignatureEncoding)?;
        let signature =
            Signature::from_slice(&bytes).map_err(|_| Error::InvalidSignatureEncoding)?;
        key.verify_strict(self.0.as_bytes(), &signature)
            .map_err(|_| Error::VerificationFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use http::HeaderValue;

    fn test_signing_key() -> SigningKey {
        SigningKey::from_bytes(&[0x42; 32])
    }

    /// Raw query string as it would appear on the wire (unsorted, includes the
    /// X-Os-Sig that must be excluded).
    fn sample_query() -> &'static str {
        "X-Os-Timestamp=1985-04-12T23:20:50.52Z\
         &X-Os-Key-Id=relay\
         &X-Os-Expires=3600\
         &X-Os-Sig=should-be-excluded"
    }

    fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn canonical_form_is_stable() {
        // Base case: full query, no signed headers. Pins path encoding (slashes
        // too), X-Os-Sig exclusion, query sort order, and the trailing empty header
        // line.
        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/org=17;project=42/foo/bar",
            Some(sample_query()),
            &HeaderMap::new(),
            &[],
        )
        .unwrap();
        assert_eq!(
            canonical.as_str(),
            "GET\n\
             %2Fv1%2Fobjects%2Ftesting%2Forg%3D17%3Bproject%3D42%2Ffoo%2Fbar\n\
             X%2DOs%2DExpires=3600&\
             X%2DOs%2DKey%2DId=relay&\
             X%2DOs%2DTimestamp=1985%2D04%2D12T23%3A20%3A50%2E52Z\n"
        );

        // Empty query: both the query and header components are empty, so the form
        // ends with two consecutive newlines.
        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            None,
            &HeaderMap::new(),
            &[],
        )
        .unwrap();
        assert_eq!(
            canonical.as_str(),
            "GET\n%2Fv1%2Fobjects%2Ftesting%2F%5F%2Fkey\n\n"
        );

        // Duplicate query keys are preserved (not deduplicated) and ordered by
        // (key, value).
        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some("dup=b&dup=a&x=1"),
            &HeaderMap::new(),
            &[],
        )
        .unwrap();
        assert_eq!(
            canonical.as_str(),
            "GET\n%2Fv1%2Fobjects%2Ftesting%2F%5F%2Fkey\ndup=a&dup=b&x=1\n"
        );

        // Multiple signed headers, passed unsorted with padded values: names are
        // already lowercase (the `HeaderName` type guarantees it), values trimmed,
        // sorted by name, joined with `\n`.
        let headers = header_map(&[
            ("Host", "objectstore.example.com"),
            ("Content-Type", "  application/json  "),
            ("X-Trace-Id", " abc "),
        ]);
        let host = HeaderName::from_static("host");
        let content_type = HeaderName::from_static("content-type");
        let x_trace_id = HeaderName::from_static("x-trace-id");
        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
            &headers,
            &[&host, &content_type, &x_trace_id],
        )
        .unwrap();
        assert_eq!(
            canonical.as_str(),
            "GET\n\
             %2Fv1%2Fobjects%2Ftesting%2F%5F%2Fkey\n\
             X%2DOs%2DExpires=3600&\
             X%2DOs%2DKey%2DId=relay&\
             X%2DOs%2DTimestamp=1985%2D04%2D12T23%3A20%3A50%2E52Z\n\
             content-type:application/json\n\
             host:objectstore.example.com\n\
             x-trace-id:abc"
        );
    }

    #[test]
    fn head_is_normalized_to_get() {
        let path = "/v1/objects/testing/_/key";
        assert_eq!(
            CanonicalRequest::new(
                &Method::HEAD,
                path,
                Some(sample_query()),
                &HeaderMap::new(),
                &[]
            ),
            CanonicalRequest::new(
                &Method::GET,
                path,
                Some(sample_query()),
                &HeaderMap::new(),
                &[]
            ),
        );
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let sk = test_signing_key();
        let vk = sk.verifying_key();

        let headers = header_map(&[("Content-Type", "application/json")]);
        let content_type = HeaderName::from_static("content-type");
        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
            &headers,
            &[&content_type],
        )
        .unwrap();
        let signature = canonical.sign(&sk);

        assert_eq!(canonical.verify(&vk, &signature), Ok(()));
    }

    #[test]
    fn verify_rejects_tampered_canonical() {
        let sk = test_signing_key();
        let vk = sk.verifying_key();

        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
            &HeaderMap::new(),
            &[],
        )
        .unwrap();
        let signature = canonical.sign(&sk);

        let tampered = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/other",
            Some(sample_query()),
            &HeaderMap::new(),
            &[],
        )
        .unwrap();
        assert_eq!(
            tampered.verify(&vk, &signature),
            Err(Error::VerificationFailed)
        );
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let sk = test_signing_key();
        let other_vk = SigningKey::from_bytes(&[0x01; 32]).verifying_key();

        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
            &HeaderMap::new(),
            &[],
        )
        .unwrap();
        let signature = canonical.sign(&sk);

        assert_eq!(
            canonical.verify(&other_vk, &signature),
            Err(Error::VerificationFailed)
        );
    }

    #[test]
    fn verify_rejects_bad_signature_encoding() {
        let vk = test_signing_key().verifying_key();
        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
            &HeaderMap::new(),
            &[],
        )
        .unwrap();

        // Not valid base64url.
        assert_eq!(
            canonical.verify(&vk, "not valid base64!!"),
            Err(Error::InvalidSignatureEncoding)
        );
        // Valid base64url but not a 64-byte signature.
        assert_eq!(
            canonical.verify(&vk, &URL_SAFE_NO_PAD.encode([0u8; 10])),
            Err(Error::InvalidSignatureEncoding)
        );
    }

    #[test]
    fn canonical_rejects_non_text_header_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-binary"),
            HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
        );
        let x_binary = HeaderName::from_static("x-binary");
        assert_eq!(
            CanonicalRequest::new(
                &Method::GET,
                "/v1/objects/testing/_/key",
                None,
                &headers,
                &[&x_binary],
            ),
            Err(Error::InvalidHeaderValue)
        );
    }
}
