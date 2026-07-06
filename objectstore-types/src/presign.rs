//! Canonicalization and signing primitives for pre-signed URLs.
//!
//! A pre-signed URL lets a client that owns an Ed25519 keypair (e.g. Relay or
//! Sentry) hand out a time-limited URL that authorizes a single **GET** or
//! **HEAD** object read, without the recipient needing an auth token. The
//! authorization lives entirely in query parameters:
//!
//! ```text
//! GET /v1/objects/<usecase>/<scopes>/<key>
//!     ?X-Os-Key-Id=relay
//!     &X-Os-Timestamp=1985-04-12T23:20:50.52Z
//!     &X-Os-Expires=3600            # validity in seconds
//!     &X-Os-Sig=<base64url signature>
//! ```
//!
//! The signature covers a **canonical form** of the request (see
//! [`canonical_request`]). The signer builds and signs it with [`sign`]; the
//! verifier rebuilds the identical canonical form from the received request and
//! checks the signature with [`verify`] against the public key selected by
//! `X-Os-Key-Id`.
//!
//! # Canonical form
//!
//! The canonical form is four newline-separated components:
//!
//! ```text
//! <method>\n
//! <canonical path>\n
//! <canonical query string>\n
//! <canonical headers>
//! ```
//!
//! - **method** — the uppercase HTTP method, with `HEAD` normalized to `GET` so
//!   that a single signature is valid for either (a `HEAD` is a bodyless `GET`).
//! - **canonical path** — the request path, percent-encoded as a single string
//!   using [`NON_ALPHANUMERIC`] (uppercase hex). The `/` separator is not
//!   treated specially; it is encoded like any other reserved byte.
//! - **canonical query string** — every query parameter except [`X_OS_SIG`],
//!   with keys and values percent-encoded (same set), sorted by encoded key then
//!   encoded value, and joined as `key=value` pairs with `&`.
//! - **canonical headers** — empty for now. Signing specific request headers is
//!   a future extension (`X-Os-Signed-Headers`), so the canonical form currently
//!   always ends with a trailing newline (an empty final line).
//!
//! The signature algorithm is fixed to Ed25519 today; `X-Os-Alg` is reserved for
//! a future extension.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Signer};
use http::Method;
use percent_encoding::{NON_ALPHANUMERIC, percent_encode};

pub use ed25519_dalek::{SigningKey, VerifyingKey};

/// Query parameter naming the key ID used to sign the request.
///
/// Its value selects the public key the verifier uses (the same `kid` mechanism
/// as JWT auth).
pub const X_OS_KEY_ID: &str = "X-Os-Key-Id";

/// Query parameter carrying the time at which the request was signed.
pub const X_OS_TIMESTAMP: &str = "X-Os-Timestamp";

/// Query parameter carrying the validity duration, in seconds.
pub const X_OS_EXPIRES: &str = "X-Os-Expires";

/// Query parameter carrying the base64url-encoded signature.
///
/// This parameter is excluded from the canonical query string, since it is the
/// output of signing that string.
pub const X_OS_SIG: &str = "X-Os-Sig";

/// Errors returned when verifying a pre-signed request signature.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PresignError {
    /// The signature was not valid base64url or did not decode to a 64-byte
    /// Ed25519 signature.
    #[error("invalid signature encoding")]
    InvalidSignatureEncoding,
    /// The signature did not match the canonical request for the given key.
    #[error("signature verification failed")]
    VerificationFailed,
}

/// Builds the canonical string that gets signed for a pre-signed request.
///
/// See the [module-level documentation](self) for the exact format. `HEAD` is
/// normalized to `GET`, and [`X_OS_SIG`] is excluded from the canonical query
/// string.
///
/// `path` must be the request path as it appears in the URL (the same value the
/// signer put on the wire and the verifier reads back, e.g. `uri.path()`); it is
/// percent-encoded wholesale. `query` is the list of decoded query parameter
/// key/value pairs; they are re-encoded canonically here, so ordering of the
/// input does not matter.
pub fn canonical_request(method: &Method, path: &str, query: &[(String, String)]) -> String {
    let method = if *method == Method::HEAD {
        "GET"
    } else {
        method.as_str()
    };

    let canonical_path = percent_encode(path.as_bytes(), NON_ALPHANUMERIC).to_string();

    let mut pairs: Vec<(String, String)> = query
        .iter()
        .filter(|(key, _)| key.as_str() != X_OS_SIG)
        .map(|(key, value)| {
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

    format!("{method}\n{canonical_path}\n{canonical_query}\n")
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
) -> Result<(), PresignError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(signature_b64)
        .map_err(|_| PresignError::InvalidSignatureEncoding)?;
    let signature =
        Signature::from_slice(&bytes).map_err(|_| PresignError::InvalidSignatureEncoding)?;
    verifying_key
        .verify_strict(canonical.as_bytes(), &signature)
        .map_err(|_| PresignError::VerificationFailed)
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

    fn sample_query() -> Vec<(String, String)> {
        // Intentionally unsorted, and includes X-Os-Sig which must be excluded.
        vec![
            (
                X_OS_TIMESTAMP.to_owned(),
                "1985-04-12T23:20:50.52Z".to_owned(),
            ),
            (X_OS_KEY_ID.to_owned(), "relay".to_owned()),
            (X_OS_EXPIRES.to_owned(), "3600".to_owned()),
            (X_OS_SIG.to_owned(), "should-be-excluded".to_owned()),
        ]
    }

    #[test]
    fn canonical_form_is_stable() {
        let canonical = canonical_request(
            &Method::GET,
            "/v1/objects/testing/org=17;project=42/foo/bar",
            &sample_query(),
        );

        // Known-answer test that pins the exact bytes: path fully percent-encoded
        // (slashes too), X-Os-Sig excluded, params sorted by encoded key, and a
        // trailing empty header line.
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
            canonical_request(&Method::HEAD, path, &query),
            canonical_request(&Method::GET, path, &query),
        );
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let sk = test_signing_key();
        let vk = sk.verifying_key();

        let canonical =
            canonical_request(&Method::GET, "/v1/objects/testing/_/key", &sample_query());
        let signature = sign(&sk, &canonical);

        assert_eq!(verify(&vk, &canonical, &signature), Ok(()));
    }

    #[test]
    fn verify_rejects_tampered_canonical() {
        let sk = test_signing_key();
        let vk = sk.verifying_key();

        let canonical =
            canonical_request(&Method::GET, "/v1/objects/testing/_/key", &sample_query());
        let signature = sign(&sk, &canonical);

        let tampered =
            canonical_request(&Method::GET, "/v1/objects/testing/_/other", &sample_query());
        assert_eq!(
            verify(&vk, &tampered, &signature),
            Err(PresignError::VerificationFailed)
        );
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let sk = test_signing_key();
        let other_vk = SigningKey::from_bytes(&[0x01; 32]).verifying_key();

        let canonical =
            canonical_request(&Method::GET, "/v1/objects/testing/_/key", &sample_query());
        let signature = sign(&sk, &canonical);

        assert_eq!(
            verify(&other_vk, &canonical, &signature),
            Err(PresignError::VerificationFailed)
        );
    }

    #[test]
    fn verify_rejects_bad_signature_encoding() {
        let vk = test_signing_key().verifying_key();
        let canonical =
            canonical_request(&Method::GET, "/v1/objects/testing/_/key", &sample_query());

        // Not valid base64url.
        assert_eq!(
            verify(&vk, &canonical, "not valid base64!!"),
            Err(PresignError::InvalidSignatureEncoding)
        );
        // Valid base64url but not a 64-byte signature.
        assert_eq!(
            verify(&vk, &canonical, &URL_SAFE_NO_PAD.encode([0u8; 10])),
            Err(PresignError::InvalidSignatureEncoding)
        );
    }
}
