use objectstore_types::metadata::Metadata;
use reqwest::StatusCode;

use crate::{ObjectKey, Session};

/// The response type for a successful [`head`](Session::head) call.
pub type HeadResponse = Option<Metadata>;

impl Session {
    /// Retrieves metadata for the object with the given `key` via an HTTP HEAD request.
    ///
    /// Returns `Some(Metadata)` if the object exists, `None` if not found.
    pub fn head(&self, key: &str) -> HeadBuilder {
        HeadBuilder {
            session: self.clone(),
            key: key.to_owned(),
        }
    }
}

/// A [`head`](Session::head) request builder.
#[derive(Debug)]
pub struct HeadBuilder {
    pub(crate) session: Session,
    pub(crate) key: ObjectKey,
}

impl HeadBuilder {
    /// Sends the HEAD request and parses object metadata from the response headers.
    ///
    /// Returns `Ok(Some(metadata))` if the object exists, `Ok(None)` if not found.
    pub async fn send(self) -> crate::Result<HeadResponse> {
        let response = self
            .session
            .request(reqwest::Method::HEAD, &self.key)?
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response.error_for_status()?;
        let metadata = Metadata::from_headers(response.headers(), "")?;
        Ok(Some(metadata))
    }
}
