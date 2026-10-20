//! Presentation and caching options for successful individual-object downloads.
//!
//! The request extractor validates these options without modifying the signed URI. Handlers
//! attach their typed headers only to successful GET/HEAD responses, including partial content.
//! Error responses retain their existing headers and cache behavior.
//!
//! # Query options
//!
//! - `response-content-disposition`: exactly `inline` or `attachment` (the default). The stored
//!   filename is retained and formatted using the existing download filename rules.
//! - `response-cache-control`: an HTTP-valid header value, passed through after URL decoding.
//!   When omitted, no cache policy is added. Cache lifetimes are independent of authorization
//!   expiry: cached responses can remain usable after a signed URL expires.
//!
//! Parameter names are case-insensitive, matching pre-signed URL canonicalization. Duplicate
//! supported options and invalid values are rejected with 400. Unrelated parameters are ignored
//! by this extractor and remain covered by the existing signature scheme.
//!
//! Content type and content encoding always come from stored metadata. Content-language and
//! expires overrides are not supported. No MIME allowlist is applied to inline responses.

use std::convert::Infallible;
use std::fmt::Write as _;

use axum::response::{IntoResponseParts, ResponseParts};
use http::{HeaderValue, header};
use objectstore_types::headers::ExtValue;

/// Whether a browser should download an object or display it inline.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentDisposition {
    /// Download the object (the default).
    #[default]
    Attachment,
    /// Display the object with browser sandboxing enabled.
    Inline,
}

impl ContentDisposition {
    fn as_str(self) -> &'static str {
        match self {
            Self::Attachment => "attachment",
            Self::Inline => "inline",
        }
    }
}

/// Validated `response-content-disposition` and `response-cache-control` query options.
///
/// Disposition defaults to attachment. Cache control is passed through without interpreting its
/// directives or coordinating its lifetime with URL expiry. Stored MIME type and encoding are
/// never overridden, and inline content is not restricted by a MIME allowlist.
#[derive(Debug, Default)]
pub(crate) struct ResponseOptions {
    /// Selected browser disposition, defaulting to attachment.
    pub(crate) disposition: ContentDisposition,
    /// Optional cache policy, validated only for HTTP header safety.
    pub(crate) cache_control: Option<HeaderValue>,
}

impl ResponseOptions {
    /// Builds success-only headers using the stored filename, when present.
    pub(crate) fn into_headers(self, filename: Option<&str>) -> ResponseOptionHeaders {
        let disposition = match filename {
            Some(filename) => format_content_disposition(self.disposition, filename),
            None => HeaderValue::from_static(self.disposition.as_str()),
        };
        ResponseOptionHeaders {
            disposition,
            cache_control: self.cache_control,
            inline: self.disposition == ContentDisposition::Inline,
        }
    }
}

/// Object presentation and caching headers, including mandatory browser safeguards.
///
/// All successful downloads get `nosniff`; inline responses also get a CSP sandbox without
/// relaxation tokens. Sandboxing is a browser safeguard, not content sanitization, and does not
/// prevent all external resource loads. Apply these parts only to successful object responses.
#[derive(Debug)]
pub(crate) struct ResponseOptionHeaders {
    disposition: HeaderValue,
    cache_control: Option<HeaderValue>,
    inline: bool,
}

impl IntoResponseParts for ResponseOptionHeaders {
    type Error = Infallible;

    fn into_response_parts(
        self,
        mut response: ResponseParts,
    ) -> Result<ResponseParts, Self::Error> {
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_DISPOSITION, self.disposition);
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        if let Some(cache_control) = self.cache_control {
            headers.insert(header::CACHE_CONTROL, cache_control);
        }
        if self.inline {
            headers.insert(
                header::CONTENT_SECURITY_POLICY,
                HeaderValue::from_static("sandbox"),
            );
        }
        Ok(response)
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
fn format_content_disposition(disposition: ContentDisposition, filename: &str) -> HeaderValue {
    let all_dots = filename.chars().all(|c| c == '.');

    let mut result = format!("{}; filename=\"", disposition.as_str());
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

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;

    use crate::response_options::{ContentDisposition, ResponseOptions};

    #[test]
    fn inline_filename_retains_escaping_and_unicode_encoding() {
        let response = (
            ResponseOptions {
                disposition: ContentDisposition::Inline,
                cache_control: None,
            }
            .into_headers(Some("réport.pdf")),
            (),
        )
            .into_response();
        assert_eq!(
            response.headers()["content-disposition"],
            "inline; filename=\"r_port.pdf\"; filename*=UTF-8''r%C3%A9port.pdf"
        );
        assert_eq!(response.headers()["content-security-policy"], "sandbox");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert!(!response.headers().contains_key("cache-control"));
    }
}
