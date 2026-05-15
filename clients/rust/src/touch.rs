use crate::head::HeadBuilder;
use crate::{ObjectKey, Session};

/// Response type for touch operations.
pub type TouchResponse = bool;

impl Session {
    /// Creates a touch request for the given `key`.
    ///
    /// A touch checks whether the object exists and bumps its TTI.
    /// Can be used standalone via [`TouchBuilder::send`] or in batch
    /// requests via [`ManyBuilder::push`](crate::ManyBuilder::push).
    pub fn touch(&self, key: &str) -> TouchBuilder {
        TouchBuilder {
            session: self.clone(),
            key: key.to_owned(),
        }
    }
}

/// A [`touch`](Session::touch) request builder.
#[derive(Debug)]
pub struct TouchBuilder {
    pub(crate) session: Session,
    pub(crate) key: ObjectKey,
}

impl TouchBuilder {
    /// Sends the touch request. Returns `true` if the object exists, `false` if not found.
    ///
    /// Under the hood this performs a HEAD request and discards the metadata.
    pub async fn send(self) -> crate::Result<bool> {
        let head = HeadBuilder {
            session: self.session,
            key: self.key,
        };
        head.send().await
    }
}
