use crate::Session;

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

/// A DELETE request builder.
#[derive(Debug)]
pub struct DeleteBuilder {
    session: Session,
    key: String,
}

impl DeleteBuilder {
    /// Sends the `DELETE` request.
    pub async fn send(self) -> crate::Result<DeleteResponse> {
        self.session
            .request(reqwest::Method::DELETE, &self.key)?
            .send()
            .await?;
        Ok(())
    }
}
