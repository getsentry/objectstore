//! HTTP response status checking with error body parsing.
//!
//! Provides [`ResponseExt`], an extension trait that replaces
//! [`reqwest::Response::error_for_status`] with a version that reads the
//! response body on 4xx/5xx errors and parses the structured error code and
//! message from it (JSON for GCS JSON API, XML for GCS XML API and S3).

use reqwest::{Response, header};
use serde::Deserialize;

use crate::error::{BackendDetail, Error, Result};
use crate::stream;

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
    /// GCS XML API includes a `<Details>` element with additional context.
    #[serde(default)]
    details: String,
}

/// Extension trait for [`reqwest::Response`] that preserves error response bodies.
///
/// Use [`check_error`](Self::check_error) instead of
/// [`error_for_status`](reqwest::Response::error_for_status) to avoid losing the response body on
/// 4xx/5xx errors. The method parses the structured error body (JSON or XML) and returns an
/// [`Error::BackendResponse`] with the extracted error code and message.
///
/// Implemented for both [`reqwest::Response`] and `Result<Response, reqwest::Error>` so it can be
/// chained directly.
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

    /// Drains the response body of a response we are otherwise done with.
    ///
    /// reqwest only returns a connection to its pool once the response body has been fully read, so
    /// we need to explicitly drain it. Errors are swallowed, since the caller has already obtained
    /// everything it needs from the response.
    async fn drain_body(self);
}

impl ResponseExt for Response {
    async fn check_error(self, context: &'static str) -> Result<Response> {
        let status = self.status();
        if !(status.is_client_error() || status.is_server_error()) {
            return Ok(self);
        }

        let ct = self
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let detail = if ct.starts_with("application/json") {
            parse_json_error(self).await
        } else if ct.starts_with("application/xml") || ct.starts_with("text/xml") {
            parse_xml_error(self).await
        } else {
            let Err(e) = self.error_for_status_ref() else {
                return Ok(self);
            };
            self.drain_body().await;
            return Err(Error::reqwest(context, e));
        };

        Err(Error::BackendResponse {
            context,
            status,
            detail,
        })
    }

    async fn drain_body(mut self) {
        while let Ok(Some(_)) = self.chunk().await {}
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

    async fn drain_body(self) {
        if let Ok(resp) = self {
            resp.drain_body().await;
        }
    }
}

async fn parse_json_error(resp: Response) -> BackendDetail {
    match resp.json().await {
        Ok(JsonApiError { error }) => {
            let code = error
                .errors
                .into_iter()
                .next()
                .map(|e| e.reason)
                .unwrap_or_default();

            BackendDetail {
                code,
                message: error.message,
            }
        }
        Err(_) => BackendDetail::none(),
    }
}

async fn parse_xml_error(resp: Response) -> BackendDetail {
    if let Ok(bytes) = resp.bytes().await
        && let Ok(XmlApiError {
            code,
            message,
            details,
        }) = quick_xml::de::from_reader(bytes.as_ref())
    {
        BackendDetail {
            code,
            message: match details.as_str() {
                "" => message,
                _ => format!("{message}: {details}"),
            },
        }
    } else {
        BackendDetail::none()
    }
}
