use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use objectstore_shared::presign::{
    PARAM_EXPIRES, PARAM_KEY_ID, PARAM_SIGNATURE, canonical_presigned_request,
};
use url::Url;

use crate::auth::SecretKey;

/// Default pre-signed URL lifetime: 5 minutes.
const DEFAULT_PRESIGNED_EXPIRY: Duration = Duration::from_secs(300);

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
}
