use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use url::Url;

use crate::{GetBuilder, ObjectKey, Session};

const DEFAULT_DURATION: Duration = Duration::from_secs(60);
const PRESIGNED_OPERATION_GET: &str = "GET";
const PRESIGNED_SIGNATURE_QUERY: &str = "X-Os-Signature";

impl GetBuilder {
    /// Converts this GET request builder into a presigned URL mint request.
    pub fn presigned_url(self) -> PresignedUrlBuilder {
        PresignedUrlBuilder {
            session: self.session,
            key: self.key,
            operation: PRESIGNED_OPERATION_GET,
            duration: DEFAULT_DURATION,
        }
    }
}

/// Builder for minting a presigned object URL.
#[derive(Debug)]
#[must_use = "call .send().await to mint a presigned URL"]
pub struct PresignedUrlBuilder {
    session: Session,
    key: ObjectKey,
    operation: &'static str,
    duration: Duration,
}

#[derive(Debug, Deserialize)]
struct PresignResponse {
    signature: String,
}

impl PresignedUrlBuilder {
    /// Sets how long from now the presigned URL should remain valid.
    ///
    /// Defaults to 60 seconds.
    pub fn duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Mints the presigned URL.
    pub async fn send(self) -> crate::Result<Url> {
        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after Unix epoch")
            .as_secs()
            + self.duration.as_secs();

        let response = self
            .session
            .presign_request(&self.key, self.operation, expires_at)?
            .send()
            .await?
            .error_for_status()?
            .json::<PresignResponse>()
            .await?;

        let mut url = self.session.object_url(&self.key);
        url.query_pairs_mut()
            .append_pair(PRESIGNED_SIGNATURE_QUERY, &response.signature);
        Ok(url)
    }
}
