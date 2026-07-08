//! Utilities for signing and verifying pre-signed URLs.
//!
//! A pre-signed URL lets a client that owns an Ed25519 keypair hand out a
//! time-limited URL that authorizes a specific request.
//!
//! Example:
//!
//! ```text
//! GET /v1/objects/<usecase>/<scopes>/<key>
//!     ?os_timestamp=2026-04-20T13:37:00.00Z
//!     &os_duration=3600
//!     &os_kid=relay
//!     &os_sig=<signature>
//! ```
//!
//! The signature covers a canonical form of the request.
//!
//! # Canonical form
//!
//! The canonical form is comprised of three newline-separated components:
//!
//! ```text
//! <normalized method>\n
//! <path>\n
//! <canonical query string>
//! ```
//!
//! - normalized method: the uppercase HTTP method, with `HEAD` mapped to `GET`;
//! - path: the request path, included verbatim as transmitted/received on the wire;
//! - canonical query string: every query parameter except `os_sig`, with keys
//!   lowercased, sorted lexicographically by name and value, joined with `&`.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Signer};
use http::Method;

pub use ed25519_dalek::{SigningKey as DalekSigningKey, VerifyingKey as DalekVerifyingKey};

/// Query parameter carrying the time at which the request was signed.
pub const PARAM_TIMESTAMP: &str = "os_timestamp";

/// Query parameter carrying the validity duration, in seconds.
pub const PARAM_DURATION: &str = "os_duration";

/// Query parameter naming the key ID used to sign the request.
pub const PARAM_KID: &str = "os_kid";

/// Query parameter carrying the base64url-encoded signature.
pub const PARAM_SIG: &str = "os_sig";

/// Errors returned when verifying a pre-signed request signature.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The user supplied a sequence of bytes that's not a valid Ed25519 key.
    #[error("invalid key")]
    InvalidKey,
    /// The signature is not valid base64url or doesn't decode to a 64-byte
    /// Ed25519 signature.
    #[error("invalid signature encoding")]
    InvalidSignatureEncoding,
    /// The signature did not match the canonical request for the given key.
    #[error("signature verification failed")]
    VerificationFailed,
}

/// The canonical form of a request to be signed or verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRequest(String);

/// An Ed25519 signing key.
pub type SigningKey = [u8; 32];

/// An Ed25519 verifying key.
pub type VerifyingKey = [u8; 32];

impl CanonicalRequest {
    /// Builds the canonical form of a request.
    ///
    /// `path` and `query` MUST be the raw request path and query string as transmitted/received
    /// on the wire, which means that servers should pass the raw contents of these components, and
    /// clients should ensure that the encoding used to compute the signature matches exactly the
    /// encoding used by their HTTP client.
    ///
    /// This is important to avoid any signature verification failures due to different
    /// encoding/decoding implementations between the client and server, and is based on the
    /// assumption that any intermediate proxies will never re-encode the URL, but always pass it
    /// through with the original encoding.
    pub fn new(method: &Method, path: &str, query: Option<&str>) -> Self {
        let normalized_method = if *method == Method::HEAD {
            "GET"
        } else {
            method.as_str()
        };

        let mut pairs: Vec<String> = query
            .unwrap_or_default()
            .split('&')
            .filter(|pair| !pair.is_empty())
            .filter_map(|pair| {
                if let Some((key, value)) = pair.split_once('=') {
                    let key = key.to_ascii_lowercase();
                    (key != PARAM_SIG).then(|| format!("{key}={value}"))
                } else {
                    let key = pair.to_ascii_lowercase();
                    (key != PARAM_SIG).then_some(key)
                }
            })
            .collect();
        pairs.sort_unstable();
        let canonical_query = pairs.join("&");

        Self(format!("{normalized_method}\n{path}\n{canonical_query}"))
    }

    /// Signs this canonical form with Ed25519.
    ///
    /// Returns the base64url-encoded signature, suitable as the value of the
    /// [`PARAM_SIG`] query parameter.
    pub fn sign(&self, key: &SigningKey) -> String {
        let key = DalekSigningKey::from_bytes(key);
        let signature = key.sign(self.0.as_bytes());
        URL_SAFE_NO_PAD.encode(signature.to_bytes())
    }

    /// Verifies a base64url-encoded Ed25519 signature against this canonical form.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidKey`] if `key` is not a valid Ed25519 verifying key,
    /// [`Error::InvalidSignatureEncoding`] if `signature_b64` is not valid base64url
    /// or not a 64-byte signature, and [`Error::VerificationFailed`] if the signature
    /// does not match.
    pub fn verify(&self, key: &VerifyingKey, signature_b64: &str) -> Result<(), Error> {
        let key = DalekVerifyingKey::from_bytes(key).map_err(|_| Error::InvalidKey)?;
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

    fn test_signing_key() -> DalekSigningKey {
        DalekSigningKey::from_bytes(&[0x42; 32])
    }

    /// Raw query string as it would appear on the wire (unsorted, includes the
    /// os_sig that must be excluded).
    fn sample_query() -> &'static str {
        "os_timestamp=1985-04-12T23:20:50.52Z\
         &os_kid=relay\
         &os_duration=3600\
         &os_sig=should-be-excluded"
    }

    #[test]
    fn canonical_form_is_stable() {
        // Base case: full query. Pins path encoding (slashes too), os_sig
        // exclusion, key lowercasing, and query sort order.
        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/org=17;project=42/foo/bar",
            Some(sample_query()),
        );
        assert_eq!(
            canonical.0,
            "GET\n\
             /v1/objects/testing/org=17;project=42/foo/bar\n\
             os_duration=3600&\
             os_kid=relay&\
             os_timestamp=1985-04-12T23:20:50.52Z"
        );

        // Empty query: the canonical query component is empty.
        let canonical = CanonicalRequest::new(&Method::GET, "/v1/objects/testing/_/key", None);
        assert_eq!(canonical.0, "GET\n/v1/objects/testing/_/key\n");

        // Duplicate query keys are preserved (not deduplicated) and ordered by
        // (key, value).
        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some("dup=b&dup=a&x=1"),
        );
        assert_eq!(
            canonical.0,
            "GET\n/v1/objects/testing/_/key\ndup=a&dup=b&x=1"
        );

        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
        );
        assert_eq!(
            canonical.0,
            "GET\n\
             /v1/objects/testing/_/key\n\
             os_duration=3600&\
             os_kid=relay&\
             os_timestamp=1985-04-12T23:20:50.52Z"
        );
    }

    #[test]
    fn head_is_normalized_to_get() {
        let path = "/v1/objects/testing/_/key";
        assert_eq!(
            CanonicalRequest::new(&Method::HEAD, path, Some(sample_query()),),
            CanonicalRequest::new(&Method::GET, path, Some(sample_query()),),
        );
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let sk = test_signing_key();
        let vk = sk.verifying_key();

        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
        );
        let signature = canonical.sign(sk.as_bytes());

        assert_eq!(canonical.verify(vk.as_bytes(), &signature), Ok(()));
    }

    #[test]
    fn verify_rejects_tampered_canonical() {
        let sk = test_signing_key();
        let vk = sk.verifying_key();

        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
        );
        let signature = canonical.sign(sk.as_bytes());

        let tampered = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/other",
            Some(sample_query()),
        );
        assert_eq!(
            tampered.verify(vk.as_bytes(), &signature),
            Err(Error::VerificationFailed)
        );
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let sk = test_signing_key();
        let other_vk = DalekSigningKey::from_bytes(&[0x01; 32]).verifying_key();

        let canonical = CanonicalRequest::new(
            &Method::GET,
            "/v1/objects/testing/_/key",
            Some(sample_query()),
        );
        let signature = canonical.sign(sk.as_bytes());

        assert_eq!(
            canonical.verify(other_vk.as_bytes(), &signature),
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
        );

        // Not valid base64url.
        assert_eq!(
            canonical.verify(vk.as_bytes(), "not valid base64!!"),
            Err(Error::InvalidSignatureEncoding)
        );
        // Valid base64url but not a 64-byte signature.
        assert_eq!(
            canonical.verify(vk.as_bytes(), &URL_SAFE_NO_PAD.encode([0u8; 10])),
            Err(Error::InvalidSignatureEncoding)
        );
    }
}
