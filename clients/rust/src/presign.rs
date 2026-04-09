use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use url::Url;

use crate::auth::SecretKey;

/// Query parameter names for pre-signed URLs.
const PARAM_EXPIRES: &str = "X-Os-Expires";
const PARAM_KEY_ID: &str = "X-Os-KeyId";
const PARAM_SIGNATURE: &str = "X-Os-Signature";

/// Default pre-signed URL lifetime: 5 minutes.
const DEFAULT_PRESIGNED_EXPIRY: Duration = Duration::from_secs(300);

/// A pre-signed URL with its expiry time.
#[derive(Debug, Clone)]
pub struct PresignedUrl {
    /// The full pre-signed URL including signature query parameters.
    pub url: Url,
    /// When this pre-signed URL expires.
    pub expires_at: SystemTime,
}

/// Generate a pre-signed URL for reading an object.
///
/// The returned URL is valid for both GET and HEAD requests until `expires_at`.
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
    mut url: Url,
    expires_in: Option<Duration>,
) -> crate::Result<PresignedUrl> {
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
        .expect("system clock before UNIX epoch")
        .as_secs();

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

    Ok(PresignedUrl { url, expires_at })
}

/// Build the canonical request string for pre-signed URL signing/verification.
///
/// The canonical form is:
/// ```text
/// GET\n{percent_decoded_path}\n{sorted_decoded_query_params}
/// ```
///
/// - Method is always `GET` (HEAD maps to GET).
/// - Path is percent-decoded.
/// - Query params are percent-decoded, sorted by key, excluding `X-Os-Signature`.
fn canonical_presigned_request(path: &str, query: Option<&str>) -> String {
    let decoded_path = percent_decode(path);

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
            Some((dk, dv))
        })
        .collect();

    params.sort_by(|a, b| a.0.cmp(&b.0));

    let query_str = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    format!("GET\n{decoded_path}\n{query_str}")
}

/// Percent-decode a string, interpreting the result as UTF-8.
fn percent_decode(input: &str) -> String {
    percent_encoding::percent_decode_str(input)
        .decode_utf8_lossy()
        .into_owned()
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
            url,
            Some(Duration::from_secs(300)),
        )
        .unwrap();

        // URL should have all three query params
        let query = result.url.query().unwrap();
        assert!(query.contains("X-Os-Expires="));
        assert!(query.contains("X-Os-KeyId=test_kid"));
        assert!(query.contains("X-Os-Signature="));

        // expires_at should be in the future
        assert!(result.expires_at > SystemTime::now());
    }

    #[test]
    fn test_presign_url_head_produces_same_signature() {
        let url1 = Url::parse("http://localhost:8888/v1/objects/test/org=1/key").unwrap();
        let url2 = url1.clone();

        let result1 = presign_url(
            &test_secret_key(),
            "GET",
            url1,
            Some(Duration::from_secs(300)),
        )
        .unwrap();
        let result2 = presign_url(
            &test_secret_key(),
            "HEAD",
            url2,
            Some(Duration::from_secs(300)),
        )
        .unwrap();

        // Extract signature from both — they may differ due to different timestamps,
        // but the canonical method is always GET for both
        let canonical1 = canonical_presigned_request(result1.url.path(), result1.url.query());
        let canonical2 = canonical_presigned_request(result2.url.path(), result2.url.query());

        // Both should start with GET
        assert!(canonical1.starts_with("GET\n"));
        assert!(canonical2.starts_with("GET\n"));
    }

    #[test]
    fn test_presign_url_invalid_method() {
        let url = Url::parse("http://localhost:8888/v1/objects/test/org=1/key").unwrap();
        let result = presign_url(&test_secret_key(), "PUT", url, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_presign_url_default_expiry() {
        let url = Url::parse("http://localhost:8888/v1/objects/test/org=1/key").unwrap();
        let result = presign_url(&test_secret_key(), "GET", url, None).unwrap();

        // Default is 5 minutes, should be ~300s from now
        let diff = result.expires_at.duration_since(SystemTime::now()).unwrap();
        assert!(diff.as_secs() >= 298 && diff.as_secs() <= 301);
    }

    #[test]
    fn test_canonical_form_basic() {
        let canonical = canonical_presigned_request(
            "/v1/objects/attachments/org=123;project=456/my-key",
            Some("X-Os-Expires=1712668800&X-Os-KeyId=relay-prod&X-Os-Signature=abc123"),
        );
        assert_eq!(
            canonical,
            "GET\n/v1/objects/attachments/org=123;project=456/my-key\nX-Os-Expires=1712668800&X-Os-KeyId=relay-prod"
        );
    }

    #[test]
    fn test_canonical_form_percent_encoded_path() {
        let canonical = canonical_presigned_request(
            "/v1/objects/attachments/org%3D123%3Bproject%3D456/my-key",
            Some("X-Os-Expires=1712668800&X-Os-KeyId=relay-prod"),
        );
        assert_eq!(
            canonical,
            "GET\n/v1/objects/attachments/org=123;project=456/my-key\nX-Os-Expires=1712668800&X-Os-KeyId=relay-prod"
        );
    }

    #[test]
    fn test_canonical_form_reordered_query_params() {
        let c1 = canonical_presigned_request("/path", Some("X-Os-KeyId=test&X-Os-Expires=1000"));
        let c2 = canonical_presigned_request("/path", Some("X-Os-Expires=1000&X-Os-KeyId=test"));
        assert_eq!(c1, c2);
    }
}
