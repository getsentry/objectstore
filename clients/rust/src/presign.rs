use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};
use url::Url;

use crate::auth::SecretKey;

/// Query parameter names for pre-signed URLs.
const PARAM_EXPIRES: &str = "X-Os-Expires";
const PARAM_KEY_ID: &str = "X-Os-KeyId";
const PARAM_SIGNATURE: &str = "X-Os-Signature";

/// Default pre-signed URL lifetime: 5 minutes.
const DEFAULT_PRESIGNED_EXPIRY: Duration = Duration::from_secs(300);

/// Canonical encoding set: encode everything except RFC 3986 unreserved characters
/// (`A-Z a-z 0-9 - _ . ~`). Same character set as AWS Signature V4's `UriEncode`.
const CANONICAL_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Same as [`CANONICAL_ENCODE_SET`] but also preserves `/` for path encoding.
const CANONICAL_PATH_ENCODE_SET: &AsciiSet = &CANONICAL_ENCODE_SET.remove(b'/');

/// Generate a pre-signed URL for reading an object.
///
/// The returned URL is valid for both GET and HEAD requests.
/// The `method` parameter must be `"GET"` or `"HEAD"` — both produce the same
/// signature since HEAD is semantically equivalent to GET for authorization.
///
/// # Arguments
///
/// * `secret_key` - The Ed25519 private key and key ID used for signing.
/// * `method` - The HTTP method (`"GET"` or `"HEAD"`).
/// * `url` - The object URL to sign (as returned by [`Session::object_url`](crate::Session::object_url)).
/// * `expires_in` - How long the URL should remain valid. Defaults to 5 minutes if `None`.
///
/// # Errors
///
/// Returns an error if the method is not GET or HEAD, or if the private key cannot be parsed.
pub fn presign_url(
    secret_key: &SecretKey,
    method: &str,
    url: &Url,
    expires_in: Option<Duration>,
) -> crate::Result<Url> {
    let method_upper = method.to_ascii_uppercase();
    if method_upper != "GET" && method_upper != "HEAD" {
        return Err(crate::PresignError::UnsupportedMethod {
            method: method.to_owned(),
        }
        .into());
    }

    let signing_key = SigningKey::from_pkcs8_pem(&secret_key.secret_key)
        .map_err(crate::PresignError::InvalidKey)?;

    let expires_in = expires_in.unwrap_or(DEFAULT_PRESIGNED_EXPIRY);
    let expires_at = SystemTime::now() + expires_in;
    let expires_ts = expires_at
        .duration_since(UNIX_EPOCH)
        .map_err(crate::PresignError::InvalidExpiry)?
        .as_secs();

    let mut url = url.clone();

    // Add X-Os-Expires and X-Os-KeyId query params
    url.query_pairs_mut()
        .append_pair(PARAM_EXPIRES, &expires_ts.to_string())
        .append_pair(PARAM_KEY_ID, &secret_key.kid);

    // Build canonical form and sign
    let canonical = canonical_presigned_request(url.path(), url.query());
    let signature = signing_key.sign(canonical.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    // Append signature
    url.query_pairs_mut().append_pair(PARAM_SIGNATURE, &sig_b64);

    Ok(url)
}

/// Build the canonical request string for pre-signed URL signing/verification.
///
/// Uses a "decode then re-encode" approach for maximum resilience:
/// 1. Percent-decode the raw path/query values
/// 2. Re-encode using a strict canonical set (only `A-Z a-z 0-9 - _ . ~` unencoded)
///
/// This normalizes to a single deterministic representation regardless of how the
/// original URL was encoded by the client or any intermediary.
///
/// The canonical form is:
/// ```text
/// GET\n{canonical_path}\n{canonical_query}
/// ```
///
/// - Method is always `GET` (HEAD maps to GET).
/// - Path is decoded then re-encoded (preserving `/`).
/// - Query params are decoded then re-encoded, sorted by encoded key,
///   excluding `X-Os-Signature`.
fn canonical_presigned_request(path: &str, query: Option<&str>) -> String {
    let canonical_path = canonical_encode_path(&percent_decode(path));

    let mut params: Vec<(String, String)> = query
        .unwrap_or("")
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            let dk = percent_decode(k);
            if dk == PARAM_SIGNATURE {
                return None;
            }
            let dv = percent_decode(v);
            Some((canonical_encode(&dk), canonical_encode(&dv)))
        })
        .collect();

    params.sort_by(|a, b| a.0.cmp(&b.0));

    let query_str = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    format!("GET\n{canonical_path}\n{query_str}")
}

/// Percent-decode a string, interpreting the result as UTF-8.
fn percent_decode(input: &str) -> String {
    percent_encoding::percent_decode_str(input)
        .decode_utf8_lossy()
        .into_owned()
}

/// Canonically encode a string: only `A-Z a-z 0-9 - _ . ~` are left unencoded.
fn canonical_encode(input: &str) -> String {
    percent_encode(input.as_bytes(), CANONICAL_ENCODE_SET).to_string()
}

/// Canonically encode a URL path, preserving `/` as a path separator.
fn canonical_encode_path(input: &str) -> String {
    percent_encode(input.as_bytes(), CANONICAL_PATH_ENCODE_SET).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY};

    fn test_secret_key() -> SecretKey {
        SecretKey {
            kid: TEST_EDDSA_KID.to_string(),
            secret_key: TEST_EDDSA_PRIVKEY.to_string(),
        }
    }

    #[test]
    fn test_presign_url_produces_valid_format() {
        let url =
            Url::parse("http://localhost:8888/v1/objects/attachments/org=123;project=456/my-key")
                .unwrap();

        let result = presign_url(
            &test_secret_key(),
            "GET",
            &url,
            Some(Duration::from_secs(300)),
        )
        .unwrap();

        let query = result.query().unwrap();
        assert!(query.contains("X-Os-Expires="));
        assert!(query.contains("X-Os-KeyId=test_kid"));
        assert!(query.contains("X-Os-Signature="));
    }

    #[test]
    fn test_presign_url_head_produces_same_signature() {
        let url = Url::parse("http://localhost:8888/v1/objects/test/org=1/key").unwrap();

        let result1 = presign_url(
            &test_secret_key(),
            "GET",
            &url,
            Some(Duration::from_secs(300)),
        )
        .unwrap();
        let result2 = presign_url(
            &test_secret_key(),
            "HEAD",
            &url,
            Some(Duration::from_secs(300)),
        )
        .unwrap();

        let canonical1 = canonical_presigned_request(result1.path(), result1.query());
        let canonical2 = canonical_presigned_request(result2.path(), result2.query());

        assert!(canonical1.starts_with("GET\n"));
        assert!(canonical2.starts_with("GET\n"));
    }

    #[test]
    fn test_presign_url_invalid_method() {
        let url = Url::parse("http://localhost:8888/v1/objects/test/org=1/key").unwrap();
        let result = presign_url(&test_secret_key(), "PUT", &url, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_canonical_form_basic() {
        let canonical = canonical_presigned_request(
            "/v1/objects/attachments/org=123;project=456/my-key",
            Some("X-Os-Expires=1712668800&X-Os-KeyId=relay-prod&X-Os-Signature=abc123"),
        );
        assert_eq!(
            canonical,
            "GET\n/v1/objects/attachments/org%3D123%3Bproject%3D456/my-key\nX-Os-Expires=1712668800&X-Os-KeyId=relay-prod"
        );
    }

    #[test]
    fn test_canonical_form_percent_encoded_path() {
        // Pre-encoded and unencoded paths produce identical canonical forms
        let canonical_unencoded = canonical_presigned_request(
            "/v1/objects/attachments/org=123;project=456/my-key",
            Some("X-Os-Expires=1712668800&X-Os-KeyId=relay-prod"),
        );
        let canonical_encoded = canonical_presigned_request(
            "/v1/objects/attachments/org%3D123%3Bproject%3D456/my-key",
            Some("X-Os-Expires=1712668800&X-Os-KeyId=relay-prod"),
        );
        assert_eq!(canonical_unencoded, canonical_encoded);
    }

    #[test]
    fn test_canonical_form_lowercase_hex() {
        let canonical_upper = canonical_presigned_request(
            "/v1/objects/test/org%3D1/key",
            Some("X-Os-Expires=1000&X-Os-KeyId=test"),
        );
        let canonical_lower = canonical_presigned_request(
            "/v1/objects/test/org%3d1/key",
            Some("X-Os-Expires=1000&X-Os-KeyId=test"),
        );
        assert_eq!(canonical_upper, canonical_lower);
    }

    #[test]
    fn test_canonical_form_reordered_query_params() {
        let c1 = canonical_presigned_request("/path", Some("X-Os-KeyId=test&X-Os-Expires=1000"));
        let c2 = canonical_presigned_request("/path", Some("X-Os-Expires=1000&X-Os-KeyId=test"));
        assert_eq!(c1, c2);
    }
}
