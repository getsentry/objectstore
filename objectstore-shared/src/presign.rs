//! Constants and functions for canonical form computation for pre-signed URLs.

/// Query parameters used for pre-signed URLs.
pub const PARAM_KEY_ID: &str = "X-Os-KeyId";
pub const PARAM_EXPIRES: &str = "X-Os-Expires";
pub const PARAM_SIGNATURE: &str = "X-Os-Signature";

/// Build the canonical request string for pre-signed URL signing/verification.
///
/// The canonical form is:
/// ```text
/// GET\n{canonical_path}\n{canonical_query}
/// ```
///
/// - Method is always `GET` (HEAD maps to GET).
/// - Path uses the encoded URI path as received by the service.
/// - Percent-encoded octets are normalized to uppercase hex digits.
/// - Query params use the encoded key/value pairs from the URI, excluding
///   `X-Os-Signature`, sorted by encoded key.
///
/// This keeps the original encoded path intact, so distinct keys such as
/// `/a/b` and `/a%2Fb` remain distinct in the canonical form.
pub fn canonical_presigned_request(path: &str, query: Option<&str>) -> String {
    let canonical_path = normalize_percent_encoding(path);

    let mut params: Vec<(String, String)> = query
        .unwrap_or("")
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            let canonical_key = normalize_percent_encoding(k);
            if canonical_key == PARAM_SIGNATURE {
                return None;
            }
            Some((canonical_key, normalize_percent_encoding(v)))
        })
        .collect();

    params.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

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

/// Normalize percent-encoded octets to use uppercase hex digits.
///
/// The input is expected to be a URI path or query component and therefore ASCII.
fn normalize_percent_encoding(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut normalized = String::with_capacity(input.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
            normalized.push('%');
            normalized.push(char::from(bytes[i + 1].to_ascii_uppercase()));
            normalized.push(char::from(bytes[i + 2].to_ascii_uppercase()));
            i += 3;
        } else {
            normalized.push(char::from(bytes[i]));
            i += 1;
        }
    }

    normalized
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
            "GET\n/v1/objects/attachments/org=123;project=456/my-key\nX-Os-Expires=1712668800&X-Os-KeyId=relay-prod"
        );
    }

    #[test]
    fn test_canonical_form_preserves_encoded_path_bytes() {
        let canonical_slash = canonical_presigned_request(
            "/v1/objects/test/_/folder/file.txt",
            Some("X-Os-Expires=1712668800&X-Os-KeyId=relay-prod"),
        );
        let canonical_encoded_slash = canonical_presigned_request(
            "/v1/objects/test/_/folder%2Ffile.txt",
            Some("X-Os-Expires=1712668800&X-Os-KeyId=relay-prod"),
        );
        assert_ne!(canonical_slash, canonical_encoded_slash);
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
    fn test_canonical_form_keeps_query_encoding() {
        let canonical = canonical_presigned_request(
            "/path",
            Some("X-Os-Expires=1000&foo=one%2Btwo&bar=hello+world&X-Os-KeyId=test"),
        );
        assert_eq!(
            canonical,
            "GET\n/path\nX-Os-Expires=1000&X-Os-KeyId=test&bar=hello+world&foo=one%2Btwo"
        );
    }

    #[test]
    fn test_canonical_form_reordered_query_params() {
        let c1 = canonical_presigned_request("/path", Some("X-Os-KeyId=test&X-Os-Expires=1000"));
        let c2 = canonical_presigned_request("/path", Some("X-Os-Expires=1000&X-Os-KeyId=test"));
        assert_eq!(c1, c2);
    }
}
