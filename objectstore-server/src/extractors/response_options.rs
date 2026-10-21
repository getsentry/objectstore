//! Extracts validated response overrides while preserving the original URI for authentication.

use axum::extract::{FromRequestParts, Query};
use http::request::Parts;

use crate::endpoints::common::ApiError;
use crate::response_options::{ContentDisposition, ResponseOptions};

impl<S: Send + Sync> FromRequestParts<S> for ResponseOptions {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Query(pairs) = Query::<Vec<(String, String)>>::try_from_uri(&parts.uri)
            .map_err(|error| ApiError::map_client("invalid response options", error))?;
        let mut disposition = None;
        for (name, value) in pairs {
            if name.eq_ignore_ascii_case("response-content-disposition") {
                if disposition.is_some() {
                    return Err(ApiError::client("duplicate response-content-disposition"));
                }
                disposition = Some(match value.as_str() {
                    "inline" => ContentDisposition::Inline,
                    "attachment" => ContentDisposition::Attachment,
                    _ => return Err(ApiError::client("invalid response-content-disposition")),
                });
            }
        }
        Ok(Self {
            disposition: disposition.unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::FromRequestParts;
    use http::Request;

    use crate::response_options::{ContentDisposition, ResponseOptions};

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
