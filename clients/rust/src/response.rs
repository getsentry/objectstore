//! Helpers for consuming HTTP responses.

use reqwest::Response;

/// Extension trait with methods to consume response bodies, to ensure that connections are
/// released cleanly.
pub(crate) trait ResponseExt {
    async fn drain_body(self);
    async fn error_for_status_and_drain(self) -> reqwest::Result<Response>;
}

impl ResponseExt for Response {
    async fn drain_body(mut self) {
        while let Ok(Some(_)) = self.chunk().await {}
    }

    async fn error_for_status_and_drain(self) -> reqwest::Result<Response> {
        let Err(error) = self.error_for_status_ref() else {
            return Ok(self);
        };

        self.drain_body().await;
        Err(error)
    }
}
