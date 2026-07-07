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
use http::Method;
use percent_encoding::{NON_ALPHANUMERIC, percent_encode};

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
    /// An unknown signature algorithm was specified.
    #[error("unknown algorithm")]
    UnknownAlgorithm,
    /// The signature did not match the canonical request for the given key.
    #[error("signature verification failed")]
    VerificationFailed,
}

/// Builds the canonical string that gets signed for a pre-signed request.
///
/// See the [module-level documentation](self) for the exact format.
///
/// Note that header values must come from a validated HTTP header map (i.e., they must not contain
/// CR/LF).
pub fn canonical_request(
    method: &Method,
    path: &str,
    query: &[(&str, &str)],
    signed_headers: &[(&str, &str)],
) -> String {
    let canonical_method = if *method == Method::HEAD {
        "GET"
    } else {
        method.as_str()
    };

    let canonical_path = percent_encode(path.as_bytes(), NON_ALPHANUMERIC).to_string();

    let mut pairs: Vec<(String, String)> = query
        .iter()
        .filter(|&&(key, _)| key != X_OS_SIG)
        .map(|&(key, value)| {
            (
                percent_encode(key.as_bytes(), NON_ALPHANUMERIC).to_string(),
                percent_encode(value.as_bytes(), NON_ALPHANUMERIC).to_string(),
            )
        })
        .collect();
    pairs.sort();

    let canonical_query = pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");

    let mut headers: Vec<(String, &str)> = signed_headers
        .iter()
        .map(|&(name, value)| (name.to_ascii_lowercase(), value.trim()))
        .collect();
    headers.sort();

    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}"))
        .collect::<Vec<_>>()
        .join("\n");

    format!("{canonical_method}\n{canonical_path}\n{canonical_query}\n{canonical_headers}")
}

/// Signs a canonical request string with Ed25519.
///
/// Returns the base64url-encoded (no padding) signature, suitable as the value
/// of the [`X_OS_SIG`] query parameter.
pub fn sign(signing_key: &SigningKey, canonical: &str) -> String {
    let signature = signing_key.sign(canonical.as_bytes());
    URL_SAFE_NO_PAD.encode(signature.to_bytes())
}

/// Verifies a base64url-encoded Ed25519 signature over a canonical request
/// string.
///
/// Returns [`PresignError::InvalidSignatureEncoding`] if `signature_b64` is not
/// valid base64url or not a 64-byte signature, and
/// [`PresignError::VerificationFailed`] if the signature does not match.
pub fn verify(
    verifying_key: &VerifyingKey,
    canonical: &str,
    signature_b64: &str,
) -> Result<(), Error> {
    let bytes = URL_SAFE_NO_PAD
        .decode(signature_b64)
        .map_err(|_| Error::InvalidSignatureEncoding)?;
    let signature = Signature::from_slice(&bytes).map_err(|_| Error::InvalidSignatureEncoding)?;
    verifying_key
        .verify_strict(canonical.as_bytes(), &signature)
        .map_err(|_| Error::VerificationFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic signing key for tests (a fixed 32-byte Ed25519 seed).
    ///
    /// Avoids depending on `objectstore-test` (which would be a dependency cycle)
    /// or on a random source.
    fn test_signing_key() -> SigningKey {
        SigningKey::from_bytes(&[0x42; 32])
    }

    fn sample_query() -> Vec<(&'static str, &'static str)> {
        // Intentionally unsorted, and includes X-Os-Sig which must be excluded.
        vec![
            (X_OS_TIMESTAMP, "1985-04-12T23:20:50.52Z"),
            (X_OS_KEY_ID, "relay"),
            (X_OS_EXPIRES, "3600"),
            (X_OS_SIG, "should-be-excluded"),
        ]
    }

    #[test]
    fn canonical_form_is_stable() {
        let canonical = canonical_request(
            &Method::GET,
            "/v1/objects/testing/org=17;project=42/foo/bar",
            &sample_query(),
            &[],
        );

        // Known-answer test that pins the exact bytes: path fully percent-encoded
        // (slashes too), X-Os-Sig excluded, params sorted by encoded key, and a
        // trailing empty header line (no signed headers).
        assert_eq!(
            canonical,
            "GET\n\
             %2Fv1%2Fobjects%2Ftesting%2Forg%3D17%3Bproject%3D42%2Ffoo%2Fbar\n\
             X%2DOs%2DExpires=3600&\
             X%2DOs%2DKey%2DId=relay&\
             X%2DOs%2DTimestamp=1985%2D04%2D12T23%3A20%3A50%2E52Z\n"
        );
    }

    #[test]
    fn head_is_normalized_to_get() {
        let path = "/v1/objects/testing/_/key";
        let query = sample_query();
        assert_eq!(
            canonical_request(&Method::HEAD, path, &query, &[]),
            canonical_request(&Method::GET, path, &query, &[]),
        );
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let sk = test_signing_key();
        let vk = sk.verifying_key();

        let canonical = canonical_request(
            &Method::GET,
            "/v1/objects/testing/_/key",
            &sample_query(),
            &[("Content-Type", "application/json")],
        );
        let signature = sign(&sk, &canonical);

        assert_eq!(verify(&vk, &canonical, &signature), Ok(()));
    }

    #[test]
    fn verify_rejects_tampered_canonical() {
        let sk = test_signing_key();
        let vk = sk.verifying_key();

        let canonical = canonical_request(
            &Method::GET,
            "/v1/objects/testing/_/key",
            &sample_query(),
            &[],
        );
        let signature = sign(&sk, &canonical);

        let tampered = canonical_request(
            &Method::GET,
            "/v1/objects/testing/_/other",
            &sample_query(),
            &[],
        );
        assert_eq!(
            verify(&vk, &tampered, &signature),
            Err(Error::VerificationFailed)
        );
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let sk = test_signing_key();
        let other_vk = SigningKey::from_bytes(&[0x01; 32]).verifying_key();

        let canonical = canonical_request(
            &Method::GET,
            "/v1/objects/testing/_/key",
            &sample_query(),
            &[],
        );
        let signature = sign(&sk, &canonical);

        assert_eq!(
            verify(&other_vk, &canonical, &signature),
            Err(Error::VerificationFailed)
        );
    }

    #[test]
    fn verify_rejects_bad_signature_encoding() {
        let vk = test_signing_key().verifying_key();
        let canonical = canonical_request(
            &Method::GET,
            "/v1/objects/testing/_/key",
            &sample_query(),
            &[],
        );

        // Not valid base64url.
        assert_eq!(
            verify(&vk, &canonical, "not valid base64!!"),
            Err(Error::InvalidSignatureEncoding)
        );
        // Valid base64url but not a 64-byte signature.
        assert_eq!(
            verify(&vk, &canonical, &URL_SAFE_NO_PAD.encode([0u8; 10])),
            Err(Error::InvalidSignatureEncoding)
        );
    }

    #[test]
    fn canonical_headers_are_lowercased_trimmed_and_sorted() {
        // Intentionally unsorted, mixed-case names, padded values.
        let headers = [
            ("Host", "objectstore.example.com"),
            ("Content-Type", "  application/json  "),
        ];
        let canonical = canonical_request(
            &Method::GET,
            "/v1/objects/testing/_/key",
            &sample_query(),
            &headers,
        );

        // The canonical headers replace the previously-empty final component:
        // names lowercased, values trimmed, sorted by name, joined with `\n`.
        assert_eq!(
            canonical,
            "GET\n\
             %2Fv1%2Fobjects%2Ftesting%2F%5F%2Fkey\n\
             X%2DOs%2DExpires=3600&\
             X%2DOs%2DKey%2DId=relay&\
             X%2DOs%2DTimestamp=1985%2D04%2D12T23%3A20%3A50%2E52Z\n\
             content-type:application/json\n\
             host:objectstore.example.com"
        );
    }

    #[test]
    fn empty_signed_headers_matches_trailing_newline_form() {
        // Passing no signed headers must be byte-identical to the historical
        // three-component form, so existing GET/HEAD signatures keep verifying.
        let path = "/v1/objects/testing/_/key";
        let query = sample_query();
        let canonical = canonical_request(&Method::GET, path, &query, &[]);
        assert!(canonical.ends_with("\n"));
        assert!(!canonical.contains(':'));
    }
}
