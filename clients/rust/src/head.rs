use reqwest::StatusCode;

use crate::{ObjectKey, Session};

impl Session {
    /// Checks whether the object with the given `key` exists (HTTP HEAD).
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
    /// Sends the HEAD request. Returns `true` if the object exists, `false` if not found.
    pub async fn send(self) -> crate::Result<bool> {
        let response = self
            .session
            .request(reqwest::Method::HEAD, &self.key)?
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        response.error_for_status()?;
        Ok(true)
    }
}
