//! HTTP response status checking with error body parsing.
//!
//! Provides [`ResponseExt`], an extension trait that replaces
//! [`reqwest::Response::error_for_status`] with a version that reads the
//! response body on 4xx/5xx errors and parses the structured error code and
//! message from it (JSON for GCS JSON API, XML for GCS XML API and S3).

use reqwest::{Response, header};
use serde::Deserialize;

use crate::error::{Error, Result};
use crate::stream;

const MAX_ERROR_BODY_LEN: usize = 1024;

/// GCS JSON API error envelope (`{"error": {"message": "...", ...}}`).
#[derive(Deserialize)]
struct JsonApiError {
    error: JsonApiErrorDetail,
}

/// Inner detail of a GCS JSON API error response.
#[derive(Deserialize)]
struct JsonApiErrorDetail {
    #[serde(default)]
    message: String,
    #[serde(default)]
    errors: Vec<JsonApiErrorEntry>,
}

/// Individual error entry in the GCS JSON API `errors` array.
#[derive(Deserialize)]
struct JsonApiErrorEntry {
    #[serde(default)]
    reason: String,
}

/// GCS XML API / S3 error body (`<Error><Code>...</Code><Message>...</Message></Error>`).
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct XmlApiError {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

/// Extension trait for [`reqwest::Response`] that preserves error response bodies.
///
/// Use `check_status` instead of [`reqwest::Response::error_for_status`] to avoid
/// losing the response body on 4xx/5xx errors. The method parses the structured
/// error body (JSON or XML) and returns an [`Error::BackendResponse`] with the
/// extracted error code and message.
///
/// Implemented for both [`reqwest::Response`] and `Result<Response, reqwest::Error>`
/// so it can be chained directly after `.send().await`.
pub trait ResponseExt {
    /// Checks the HTTP status and returns the response on success.
    ///
    /// On 4xx/5xx status codes, reads the response body and parses the error code
    /// and message from it (JSON for GCS JSON API, XML for GCS XML API and S3).
    /// For other error statuses (e.g., redirects), falls back to
    /// [`reqwest::Response::error_for_status`].
    ///
    /// When called on `Result<Response, reqwest::Error>`, transport errors are
    /// wrapped as [`Error::Reqwest`] with the same context string.
    async fn check_error(self, context: &'static str) -> Result<Response>;
}

impl ResponseExt for Response {
    async fn check_error(self, context: &'static str) -> Result<Response> {
        let status = self.status();

        if !(status.is_client_error() || status.is_server_error()) {
            return self
                .error_for_status()
                .map_err(|e| Error::reqwest(context, e));
        }

        let ct = self
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let (code, message) = if ct.starts_with("application/json") {
            parse_json_error(self).await
        } else if ct.starts_with("application/xml") || ct.starts_with("text/xml") {
            parse_xml_error(self).await
        } else {
            return self
                .error_for_status()
                .map_err(|e| Error::reqwest(context, e));
        };

        Err(Error::BackendResponse {
            context,
            status,
            code,
            message,
        })
    }
}

impl ResponseExt for Result<Response, reqwest::Error> {
    async fn check_error(self, context: &'static str) -> Result<Response> {
        match self {
            Ok(resp) => resp.check_error(context).await,
            Err(e) => Err(match stream::unpack_client_error(&e) {
                Some(ce) => Error::Client(ce),
                None => Error::reqwest(context, e),
            }),
        }
    }
}

async fn parse_json_error(resp: Response) -> (String, String) {
    let status = resp.status();
    match resp.json::<JsonApiError>().await {
        Ok(body) => {
            let code = body
                .error
                .errors
                .first()
                .map(|e| e.reason.clone())
                .unwrap_or_else(|| status.as_str().to_owned());
            (code, body.error.message)
        }
        Err(_) => (status.as_str().to_owned(), status.to_string()),
    }
}

async fn parse_xml_error(resp: Response) -> (String, String) {
    let status = resp.status();
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(_) => return (status.as_str().to_owned(), status.to_string()),
    };

    match quick_xml::de::from_reader::<_, XmlApiError>(bytes.as_ref()) {
        Ok(body) => (body.code, body.message),
        Err(_) => (status.as_str().to_owned(), status.to_string()),
    }
}
