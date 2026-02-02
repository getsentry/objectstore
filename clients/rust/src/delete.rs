use objectstore_types::key::ObjectKey;

use crate::Session;

/// The result from a successful [`delete()`](Session::delete) call.
pub type DeleteResponse = ();

impl Session {
    /// Deletes the object with the given `key`.
    ///
    /// The key will be validated and encoded according to RFC 3986. Reserved characters
    /// (like `/`, `?`, `#`, etc.) will be percent-encoded automatically.
    ///
    /// Note: Key validation is deferred to the `send()` call. If the key is invalid,
    /// `send()` will return an error.
    pub fn delete(&self, key: &str) -> DeleteBuilder {
        DeleteBuilder {
            session: self.clone(),
            key: ObjectKey::from_raw(key).map_err(Into::into),
        }
    }
}

/// A [`delete`](Session::delete) request builder.
pub struct DeleteBuilder {
    session: Session,
    key: crate::Result<ObjectKey>,
}

impl std::fmt::Debug for DeleteBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteBuilder")
            .field("session", &self.session)
            .field("key", &self.key.as_ref().map(|k| k.as_str()))
            .finish()
    }
}

impl DeleteBuilder {
    /// Sends the delete request.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key is invalid (empty, too long, or contains non-ASCII characters)
    /// - The request fails to send
    /// - The server returns an error response
    pub async fn send(self) -> crate::Result<DeleteResponse> {
        let key = self.key?;
        self.session
            .request(reqwest::Method::DELETE, &key)?
            .send()
            .await?;
        Ok(())
    }
}
