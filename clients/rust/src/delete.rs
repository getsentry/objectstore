use crate::Session;

/// The result from a successful [`delete()`](Session::delete) call.
pub type DeleteResponse = ();

impl Session {
    /// Deletes the object with the given `id`.
    pub fn delete(&self, id: &str) -> DeleteBuilder {
        DeleteBuilder {
            session: self.clone(),
            id: id.to_owned(),
        }
    }
}

/// A DELETE request builder.
#[derive(Debug)]
pub struct DeleteBuilder {
    session: Session,
    id: String,
}

impl DeleteBuilder {
    /// Sends the `DELETE` request.
    pub async fn send(self) -> crate::Result<DeleteResponse> {
        self.session
            .request(reqwest::Method::DELETE, &self.id)?
            .send()
            .await?;
        Ok(())
    }
}
