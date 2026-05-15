use objectstore_types::metadata::Metadata;
use reqwest::StatusCode;

use crate::{ObjectKey, Session};

#[derive(Debug)]
pub(crate) struct HeadBuilder {
    pub(crate) session: Session,
    pub(crate) key: ObjectKey,
}

impl HeadBuilder {
    pub(crate) async fn send(self) -> crate::Result<Option<Metadata>> {
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
