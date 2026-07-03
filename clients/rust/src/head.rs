use objectstore_types::metadata::Metadata;
use reqwest::StatusCode;

use crate::{ObjectKey, Session};

/// The result from a successful [`head()`](Session::head) call.
///
/// Returns `Some(metadata)` if the object exists, `None` otherwise.
pub type HeadResponse = Option<Metadata>;

impl Session {
    /// Checks whether an object exists and retrieves its metadata.
    ///
    /// If the object exists and has a TTI expiration policy, this is considered an access, and
    /// therefore bumps its expiration.
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
    /// Sends the head request.
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
