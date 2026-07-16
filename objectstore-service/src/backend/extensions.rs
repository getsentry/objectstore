//! Extension traits for `reqwest` requests and responses.
//!
//! Provides [`SendTraced`], which sends a request inside a tracing span, and
//! [`ResponseExt`], which replaces [`reqwest::Response::error_for_status`] with a
//! version that reads the response body on 4xx/5xx errors and parses the
//! structured error code and message from it (JSON for GCS JSON API, XML for GCS
//! XML API and S3).

use std::fmt;

use reqwest::{Response, StatusCode, header};
use serde::Deserialize;
use tracing::Instrument;

use crate::error::{Error, ErrorKind, Result};
use crate::stream;

/// Structured error detail parsed from a backend HTTP error response.
///
/// Formats conditionally: includes only the fields that are non-empty.
#[derive(Debug)]
struct BackendDetail {
    /// Machine-readable error code (e.g., "InvalidArgument", "NoSuchKey").
    code: String,
    /// Human-readable error message from the response body.
    message: String,
}

impl BackendDetail {
    /// Creates a new [`BackendDetail`] with empty code and message.
    fn none() -> Self {
        Self {
            code: String::new(),
            message: String::new(),
        }
    }
}

impl fmt::Display for BackendDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.code.is_empty(), self.message.is_empty()) {
            (false, false) => write!(f, "{} (backend code {})", self.message, self.code),
            (true, false) => write!(f, "{}", self.message),
            (false, true) => write!(f, "backend code {}", self.code),
            (true, true) => Ok(()),
        }
    }
}

/// Classifies a backend HTTP error status into an [`ErrorKind`].
fn status_to_kind(status: StatusCode) -> ErrorKind {
    match status {
        StatusCode::TOO_MANY_REQUESTS => ErrorKind::BackendRateLimited,
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => ErrorKind::BackendTimeout,
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE => ErrorKind::BackendUnavailable,
        _ => ErrorKind::Internal,
    }
}

/// Extension trait that sends a request inside a tracing span.
pub trait SendTraced {
    /// Sends the request, wrapping it in a span that covers the full request
    /// duration and records the response status code.
    async fn send_traced(self) -> reqwest::Result<reqwest::Response>;
}

impl SendTraced for reqwest::RequestBuilder {
    async fn send_traced(self) -> reqwest::Result<reqwest::Response> {
        let (client, request) = self.build_split();
        let request = request?;
        let span = tracing::debug_span!(
            "http.request",
            method = %request.method(),
            url = %request.url(),
            http.status_code = tracing::field::Empty,
        );
        let send_future = async {
            let response = client.execute(request).await;
            if let Ok(response) = &response {
                tracing::Span::current().record("http.status_code", response.status().as_u16());
            }
            response
        };
        send_future.instrument(span).await
    }
}

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
/// Use [`check_error`](Self::check_error) instead of
/// [`error_for_status`](reqwest::Response::error_for_status) to avoid losing the response body on
/// 4xx/5xx errors. The method parses the structured error body (JSON or XML) and returns an
/// error classified by status ([`status_to_kind`]) with the extracted code and message as context.
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
    /// wrapped as an [`ErrorKind::Internal`] error with the same context string.
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
            let mut err = Error::from(e);
            err.kind = status_to_kind(status);
            err.context = Some(context.into());
            return Err(err);
        };

        let detail = detail.to_string();
        let message = if detail.is_empty() {
            format!("{context} ({status})")
        } else {
            format!("{context} ({status}). {detail}")
        };
        Err(Error::new(status_to_kind(status)).context(message))
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
                Some(ce) => Error::from(ce),
                None => {
                    let kind = if e.is_timeout() {
                        ErrorKind::BackendTimeout
                    } else if e.is_connect() || e.is_request() {
                        ErrorKind::BackendUnavailable
                    } else {
                        ErrorKind::Internal
                    };
                    let mut err = Error::from(e);
                    err.kind = kind;
                    err.context = Some(context.into());
                    err
                }
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
        && let Ok(XmlApiError { code, message }) = quick_xml::de::from_reader(bytes.as_ref())
    {
        BackendDetail { code, message }
    } else {
        BackendDetail::none()
    }
}
