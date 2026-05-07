use crate::{ObjectKey, Session};

/// The result from a successful [`delete()`](Session::delete) call.
pub type DeleteResponse = ();

impl Session {
    /// Deletes the object with the given `key`.
    pub fn delete(&self, key: &str) -> DeleteBuilder {
        DeleteBuilder {
            session: self.clone(),
            key: key.to_owned(),
        }
    }
}

/// A [`delete`](Session::delete) request builder.
#[derive(Debug)]
pub struct DeleteBuilder {
    pub(crate) session: Session,
    pub(crate) key: ObjectKey,
}

impl DeleteBuilder {
    /// Sends the delete request.
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(
            name = "objectstore.delete",
            level = "debug",
            skip_all,
            fields(key = self.key.as_str())
        )
    )]
    pub async fn send(self) -> crate::Result<DeleteResponse> {
        self.session
            .request(reqwest::Method::DELETE, &self.key)?
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
