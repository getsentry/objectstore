use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::auth::util::StringOrWildcard;

/// Query parameter names for pre-signed URLs.
const PARAM_EXPIRES: &str = "X-Os-Expires";
const PARAM_KEY_ID: &str = "X-Os-KeyId";
const PARAM_SIGNATURE: &str = "X-Os-Signature";

/// Pre-signed URL parameters extracted from the query string.
#[derive(Debug)]
pub struct PreSignedParams {
    pub expires: u64,
    pub key_id: String,
    pub signature: Vec<u8>,
}

/// Attempt to extract pre-signed URL parameters from a URI's query string.
///
/// Returns `Some` only if all three required parameters (`X-Os-Expires`, `X-Os-KeyId`,
/// `X-Os-Signature`) are present and parseable. Returns `None` if any are missing,
/// indicating this is not a pre-signed URL request.
pub fn extract_presigned_params(uri: &http::Uri) -> Option<PreSignedParams> {
    let query = uri.query()?;

    let mut expires: Option<u64> = None;
    let mut key_id: Option<String> = None;
    let mut signature: Option<Vec<u8>> = None;

    for pair in query.split('&') {
        let (k, v) = pair.split_once('=')?;
        let k = percent_decode(k);
        let v = percent_decode(v);
        match k.as_str() {
            PARAM_EXPIRES => expires = Some(v.parse().ok()?),
            PARAM_KEY_ID => key_id = Some(v),
            PARAM_SIGNATURE => signature = Some(URL_SAFE_NO_PAD.decode(v.as_bytes()).ok()?),
            _ => {}
        }
    }

    Some(PreSignedParams {
        expires: expires?,
        key_id: key_id?,
        signature: signature?,
    })
}

/// Build the canonical request string for pre-signed URL verification.
///
/// The canonical form is:
/// ```text
/// GET\n{percent_decoded_path}\n{sorted_decoded_query_params}
/// ```
///
/// - Method is always `GET` (HEAD maps to GET).
/// - Path is percent-decoded.
/// - Query params are percent-decoded, sorted by key, excluding `X-Os-Signature`.
pub(crate) fn canonical_presigned_request(path: &str, query: Option<&str>) -> String {
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

/// Extract usecase and scopes from a decoded URL path.
///
/// Expected format: `/v1/objects/{usecase}/{scopes}/{key...}` or with a prefix.
/// Scopes are semicolon-separated `key=value` pairs (e.g., `org=123;project=456`).
pub(crate) fn parse_path_context(
    decoded_path: &str,
) -> Option<(String, BTreeMap<String, StringOrWildcard>)> {
    // Find the `/v1/objects/` segment and take what follows
    let rest = decoded_path
        .find("/v1/objects/")
        .map(|i| &decoded_path[i + "/v1/objects/".len()..])?;

    let mut parts = rest.splitn(3, '/');
    let usecase = parts.next()?.to_string();
    let scopes_str = parts.next()?;
    // parts.next() would be the key, which we don't need

    if usecase.is_empty() {
        return None;
    }

    let scopes = if scopes_str == "_" || scopes_str.is_empty() {
        BTreeMap::new()
    } else {
        scopes_str
            .split(';')
            .filter(|s| !s.is_empty())
            .map(|s| {
                let (k, v) = s.split_once('=')?;
                Some((k.to_string(), StringOrWildcard::String(v.to_string())))
            })
            .collect::<Option<BTreeMap<_, _>>>()?
    };

    Some((usecase, scopes))
}

/// Percent-decode a string, interpreting the result as UTF-8.
pub(crate) fn percent_decode(input: &str) -> String {
    percent_encoding::percent_decode_str(input)
        .decode_utf8_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
    use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
    use jsonwebtoken::DecodingKey;
    use objectstore_types::auth::Permission;

    use crate::auth::context::AuthContext;
    use crate::auth::error::AuthError;
    use crate::auth::key_directory::{PublicKeyConfig, PublicKeyDirectory};
    use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY, TEST_EDDSA_PUBKEY};

    fn test_key_directory() -> PublicKeyDirectory {
        let public_key = PublicKeyConfig {
            key_versions: vec![DecodingKey::from_ed_pem(TEST_EDDSA_PUBKEY.as_bytes()).unwrap()],
            verifying_keys: vec![VerifyingKey::from_public_key_pem(&TEST_EDDSA_PUBKEY).unwrap()],
            max_permissions: HashSet::from([
                Permission::ObjectRead,
                Permission::ObjectWrite,
                Permission::ObjectDelete,
            ]),
        };
        PublicKeyDirectory {
            keys: BTreeMap::from([(TEST_EDDSA_KID.to_string(), public_key)]),
        }
    }

    fn sign_url(path: &str, expires: u64) -> (http::Uri, PreSignedParams) {
        let signing_key = SigningKey::from_pkcs8_pem(&TEST_EDDSA_PRIVKEY).unwrap();

        let query_without_sig =
            format!("{PARAM_EXPIRES}={expires}&{PARAM_KEY_ID}={TEST_EDDSA_KID}");
        let canonical = canonical_presigned_request(path, Some(&query_without_sig));
        let signature = signing_key.sign(canonical.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        let uri: http::Uri = format!("{path}?{query_without_sig}&{PARAM_SIGNATURE}={sig_b64}")
            .parse()
            .unwrap();

        let params = extract_presigned_params(&uri).unwrap();
        (uri, params)
    }

    fn future_expires() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300
    }

    fn past_expires() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 100
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
        let canonical1 = canonical_presigned_request(
            "/v1/objects/test/org=1/key",
            Some("X-Os-KeyId=test&X-Os-Expires=1000"),
        );
        let canonical2 = canonical_presigned_request(
            "/v1/objects/test/org=1/key",
            Some("X-Os-Expires=1000&X-Os-KeyId=test"),
        );
        assert_eq!(canonical1, canonical2);
    }

    #[test]
    fn test_extract_presigned_params_all_present() {
        let uri: http::Uri = "/path?X-Os-Expires=1234&X-Os-KeyId=kid&X-Os-Signature=AAAA"
            .parse()
            .unwrap();
        let params = extract_presigned_params(&uri).unwrap();
        assert_eq!(params.expires, 1234);
        assert_eq!(params.key_id, "kid");
    }

    #[test]
    fn test_extract_presigned_params_missing_param() {
        let uri: http::Uri = "/path?X-Os-Expires=1234&X-Os-KeyId=kid".parse().unwrap();
        assert!(extract_presigned_params(&uri).is_none());
    }

    #[test]
    fn test_extract_presigned_params_no_query() {
        let uri: http::Uri = "/path".parse().unwrap();
        assert!(extract_presigned_params(&uri).is_none());
    }

    #[test]
    fn test_from_presigned_url_valid() {
        let dir = test_key_directory();
        let path = "/v1/objects/attachments/org=123;project=456/my-key";
        let (uri, params) = sign_url(path, future_expires());

        let ctx = AuthContext::from_presigned_url(&params, &uri, &dir).unwrap();
        assert_eq!(ctx.usecase, "attachments");
        assert!(ctx.permissions.contains(&Permission::ObjectRead));
        assert_eq!(ctx.permissions.len(), 1);
        assert_eq!(
            ctx.scopes.get("org"),
            Some(&StringOrWildcard::String("123".into()))
        );
        assert_eq!(
            ctx.scopes.get("project"),
            Some(&StringOrWildcard::String("456".into()))
        );
    }

    #[test]
    fn test_from_presigned_url_expired() {
        let dir = test_key_directory();
        let path = "/v1/objects/attachments/org=123/my-key";
        let (uri, params) = sign_url(path, past_expires());

        let result = AuthContext::from_presigned_url(&params, &uri, &dir);
        assert!(matches!(result, Err(AuthError::BadRequest(_))));
    }

    #[test]
    fn test_from_presigned_url_tampered_path() {
        let dir = test_key_directory();
        let path = "/v1/objects/attachments/org=123/my-key";
        let (_, params) = sign_url(path, future_expires());

        let tampered_uri: http::Uri = format!(
            "/v1/objects/attachments/org=999/my-key?{PARAM_EXPIRES}={}&{PARAM_KEY_ID}={TEST_EDDSA_KID}&{PARAM_SIGNATURE}={}",
            params.expires,
            URL_SAFE_NO_PAD.encode(&params.signature),
        )
        .parse()
        .unwrap();

        let result = AuthContext::from_presigned_url(&params, &tampered_uri, &dir);
        assert!(matches!(result, Err(AuthError::VerificationFailure)));
    }

    #[test]
    fn test_from_presigned_url_unknown_key_id() {
        let dir = test_key_directory();
        let signing_key = SigningKey::from_pkcs8_pem(&TEST_EDDSA_PRIVKEY).unwrap();

        let path = "/v1/objects/attachments/org=123/key";
        let expires = future_expires();
        let query = format!("{PARAM_EXPIRES}={expires}&{PARAM_KEY_ID}=unknown-kid");
        let canonical = canonical_presigned_request(path, Some(&query));
        let sig = signing_key.sign(canonical.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig.to_bytes());

        let uri: http::Uri = format!("{path}?{query}&{PARAM_SIGNATURE}={sig_b64}")
            .parse()
            .unwrap();
        let params = extract_presigned_params(&uri).unwrap();

        let result = AuthContext::from_presigned_url(&params, &uri, &dir);
        assert!(matches!(result, Err(AuthError::InternalError(_))));
    }

    #[test]
    fn test_from_presigned_url_key_without_read_permission() {
        let public_key = PublicKeyConfig {
            key_versions: vec![DecodingKey::from_ed_pem(TEST_EDDSA_PUBKEY.as_bytes()).unwrap()],
            verifying_keys: vec![VerifyingKey::from_public_key_pem(&TEST_EDDSA_PUBKEY).unwrap()],
            max_permissions: HashSet::from([Permission::ObjectWrite]),
        };
        let dir = PublicKeyDirectory {
            keys: BTreeMap::from([(TEST_EDDSA_KID.to_string(), public_key)]),
        };

        let path = "/v1/objects/attachments/org=123/key";
        let (uri, params) = sign_url(path, future_expires());

        let result = AuthContext::from_presigned_url(&params, &uri, &dir);
        assert!(matches!(result, Err(AuthError::NotPermitted)));
    }

    #[test]
    fn test_from_presigned_url_empty_scopes() {
        let dir = test_key_directory();
        let path = "/v1/objects/attachments/_/my-key";
        let (uri, params) = sign_url(path, future_expires());

        let ctx = AuthContext::from_presigned_url(&params, &uri, &dir).unwrap();
        assert_eq!(ctx.usecase, "attachments");
        assert!(ctx.scopes.is_empty());
    }

    #[test]
    fn test_parse_path_context_standard() {
        let (usecase, scopes) =
            parse_path_context("/v1/objects/attachments/org=123;project=456/my-key").unwrap();
        assert_eq!(usecase, "attachments");
        assert_eq!(
            scopes.get("org"),
            Some(&StringOrWildcard::String("123".into()))
        );
        assert_eq!(
            scopes.get("project"),
            Some(&StringOrWildcard::String("456".into()))
        );
    }

    #[test]
    fn test_parse_path_context_with_prefix() {
        let (usecase, scopes) =
            parse_path_context("/api/prefix/v1/objects/attachments/org=1/key").unwrap();
        assert_eq!(usecase, "attachments");
        assert_eq!(
            scopes.get("org"),
            Some(&StringOrWildcard::String("1".into()))
        );
    }

    #[test]
    fn test_parse_path_context_empty_scopes() {
        let (usecase, scopes) = parse_path_context("/v1/objects/attachments/_/key").unwrap();
        assert_eq!(usecase, "attachments");
        assert!(scopes.is_empty());
    }
}
