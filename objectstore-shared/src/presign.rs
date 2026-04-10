//! Constants and functions for canonical form computation for pre-signed URLs.

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_encode};

/// Query parameters used for pre-signed URLs.
pub const PARAM_KEY_ID: &str = "X-Os-KeyId";
pub const PARAM_EXPIRES: &str = "X-Os-Expires";
pub const PARAM_SIGNATURE: &str = "X-Os-Signature";

/// Canonical encoding set: encode everything except RFC 3986 unreserved characters
/// (`A-Z a-z 0-9 - _ . ~`). Same character set as AWS Signature V4's `UriEncode`.
const CANONICAL_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

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
/// - Path is decoded then re-encoded.
/// - Query params are decoded then re-encoded, sorted by encoded key,
///   excluding `X-Os-Signature`.
pub fn canonical_presigned_request(path: &str, query: Option<&str>) -> String {
    let canonical_path = canonical_encode(&percent_decode(path));

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
pub fn percent_decode(input: &str) -> String {
    percent_encoding::percent_decode_str(input)
        .decode_utf8_lossy()
        .into_owned()
}

/// Canonically encode a string: only `A-Z a-z 0-9 - _ . ~` are left unencoded.
/// Uses uppercase hex digits for deterministic output.
pub fn canonical_encode(input: &str) -> String {
    percent_encode(input.as_bytes(), CANONICAL_ENCODE_SET).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonical_form_basic() {
        let canonical = canonical_presigned_request(
            "/v1/objects/attachments/org=123;project=456/my-key",
            Some("X-Os-Expires=1712668800&X-Os-KeyId=relay-prod&X-Os-Signature=abc123"),
        );
        assert_eq!(
            canonical,
            "GET\n%2Fv1%2Fobjects%2Fattachments%2Forg%3D123%3Bproject%3D456%2Fmy-key\nX-Os-Expires=1712668800&X-Os-KeyId=relay-prod"
        );
    }

    #[test]
    fn test_canonical_form_percent_encoded_path() {
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
