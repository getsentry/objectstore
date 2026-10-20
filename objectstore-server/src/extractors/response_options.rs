//! Extracts validated response overrides while preserving the original URI for authentication.

use axum::extract::{FromRequestParts, Query};
use http::{HeaderValue, request::Parts};

use crate::endpoints::common::ApiError;
use crate::response_options::{ContentDisposition, ResponseOptions};

impl<S: Send + Sync> FromRequestParts<S> for ResponseOptions {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Query(pairs) = Query::<Vec<(String, String)>>::try_from_uri(&parts.uri)
            .map_err(|error| ApiError::map_client("invalid response options", error))?;
        let mut disposition = None;
        let mut cache_control = None;
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
            } else if name.eq_ignore_ascii_case("response-cache-control") {
                if cache_control.is_some() {
                    return Err(ApiError::client("duplicate response-cache-control"));
                }
                cache_control = Some(HeaderValue::from_str(&value).map_err(|error| {
                    ApiError::map_client("invalid response-cache-control", error)
                })?);
            }
        }
        Ok(Self {
            disposition: disposition.unwrap_or_default(),
            cache_control,
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
        let uri = "/object?unknown=b&unknown=a&%72esponse-content-disposition=inline&RESPONSE-CACHE-CONTROL=private%2C%20x%3D%22a%26b%3D%2B%25%3F%23%22";
        let (mut parts, ()) = Request::builder().uri(uri).body(()).unwrap().into_parts();
        let options = ResponseOptions::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        assert_eq!(options.disposition, ContentDisposition::Inline);
        assert_eq!(options.cache_control.unwrap(), "private, x=\"a&b=+%?#\"");
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
            assert!(options.cache_control.is_none());
        }
    }
}
