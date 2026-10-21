//! Presentation options for successful individual-object downloads.
//!
//! The request extractor validates these options without modifying the signed URI. Handlers
//! attach their typed headers only to successful GET/HEAD responses, including partial content.
//! Error responses retain their existing headers.
//!
//! # Query options
//!
//! - `response-content-disposition`: exactly `inline` or `attachment` (the default). The stored
//!   filename is retained and formatted using the existing download filename rules.
//!
//! Parameter names are case-insensitive, matching pre-signed URL canonicalization. Duplicate
//! supported options and invalid values are rejected with 400. Unrelated parameters are ignored
//! by this extractor and remain covered by the existing signature scheme.
//!
//! Content type and content encoding always come from stored metadata. Content-language and
//! expires overrides are not supported. No MIME allowlist is applied to inline responses.

use std::fmt::Write as _;

use axum::extract::FromRequestParts;
use http::HeaderValue;
use http::request::Parts;
use objectstore_types::headers::ExtValue;
use serde::Deserialize;
use serde::de::value::{Error, MapDeserializer};
use url::form_urlencoded;

use crate::endpoints::common::ApiError;

/// Whether a browser should download an object or display it inline.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentDisposition {
    /// Download the object (the default).
    #[default]
    Attachment,
    /// Display the object with browser sandboxing enabled.
    Inline,
}

impl ContentDisposition {
    /// Returns the disposition's HTTP header value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Attachment => "attachment",
            Self::Inline => "inline",
        }
    }

    /// Formats the selected disposition with a stored filename.
    ///
    /// The filename is sanitized (`/` and `\` become `-`, dots-only names become all dashes, non-ASCII
    /// and control characters become `_`) and then escaped for the RFC 6266 quoted-string (`"` is
    /// backslash-escaped).
    ///
    /// A filename that is not pure ASCII cannot be represented in that quoted-string, so it
    /// additionally gets an RFC 8187 `filename*` parameter carrying the full UTF-8 value. The
    /// quoted-string then serves as the ASCII fallback for clients that ignore `filename*`.
    pub fn with_filename(self, filename: &str) -> HeaderValue {
        let all_dots = filename.chars().all(|c| c == '.');

        let mut result = format!("{}; filename=\"", self.as_str());
        for c in filename.chars() {
            let c = match c {
                '/' | '\\' => '-',
                '.' if all_dots => '-',
                '"' => {
                    result.push('\\');
                    '"'
                }
                c if !c.is_ascii() || c.is_control() => '_',
                c => c,
            };
            result.push(c);
        }
        result.push('"');

        if !filename.is_ascii() {
            write!(result, "; filename*={}", ExtValue(filename))
                .expect("writing to a string cannot fail");
        }

        // INVARIANT: every character written above is visible ASCII — the quoted-string replaces
        // non-ASCII and control characters with `_`, and the `ext-value` is percent-encoded.
        HeaderValue::from_str(&result).expect("content disposition is a valid header value")
    }
}

/// Validated presentation options for successful object downloads.
///
/// Disposition defaults to attachment. Stored MIME type and encoding are never overridden,
/// and inline content is not restricted by a MIME allowlist.
#[derive(Debug, Default, Deserialize)]
pub struct ResponseOptions {
    /// Selected browser disposition, defaulting to attachment.
    #[serde(default, rename = "response-content-disposition")]
    pub disposition: ContentDisposition,
}

impl<S: Send + Sync> FromRequestParts<S> for ResponseOptions {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Query<ResponseOptions> matches names case-sensitively. Normalize them to match
        // pre-signed URL canonicalization without modifying the original URI.
        let pairs = form_urlencoded::parse(parts.uri.query().unwrap_or_default().as_bytes())
            .map(|(name, value)| (name.to_ascii_lowercase(), value.into_owned()));

        Self::deserialize(MapDeserializer::<_, Error>::new(pairs))
            .map_err(|error| ApiError::map_client("invalid response options", error))
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::FromRequestParts;
    use http::Request;

    use crate::extractors::response_options::{ContentDisposition, ResponseOptions};

    #[tokio::test]
    async fn decodes_options_without_changing_signed_uri() {
        let uri = "/object?unknown=b&unknown=a&%72esponse-content-disposition=inline";
        let (mut parts, ()) = Request::builder().uri(uri).body(()).unwrap().into_parts();
        let options = ResponseOptions::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        assert_eq!(options.disposition, ContentDisposition::Inline);
        assert_eq!(parts.uri.to_string(), uri);
    }

    #[tokio::test]
    async fn defaults_preserve_unrelated_parameters() {
        for uri in ["/object", "/object?unknown=b&unknown=a&flag&&"] {
            let (mut parts, ()) = Request::builder().uri(uri).body(()).unwrap().into_parts();
            let options = ResponseOptions::from_request_parts(&mut parts, &())
                .await
                .unwrap();
            assert_eq!(options.disposition, ContentDisposition::Attachment);
        }
    }
}
