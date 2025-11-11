use crate::Session;

/// The result from a successful [`delete()`](Session::delete) call.
pub type DeleteResponse = ();

impl Session {
    /// Deletes the object with the given `id`.
    pub fn delete<'a>(&'a self, id: &'a str) -> DeleteBuilder<'a> {
        DeleteBuilder { session: self, id }
    }
}

/// A DELETE request builder.
#[derive(Debug)]
pub struct DeleteBuilder<'a> {
    session: &'a Session,
    id: &'a str,
}

impl DeleteBuilder<'_> {
    /// Sends the `DELETE` request.
    pub async fn send(self) -> crate::Result<DeleteResponse> {
        let delete_url = format!("{}v1/{}", self.session.client.service_url(), self.id);
        self.session
            .request(reqwest::Method::DELETE, delete_url)?
            .send()
            .await?;
        Ok(())
    }
}
