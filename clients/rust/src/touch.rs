use crate::head::HeadBuilder;
use crate::{ObjectKey, Session};

/// Response type for touch operations.
pub type TouchResponse = bool;

impl Session {
    /// Creates a touch request for the given `key`.
    ///
    /// A touch operation checks whether the object exists and, if it exists and uses a TTI
    /// expiration policy, bumps its TTI.
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
    pub async fn send(self) -> crate::Result<bool> {
        let head = HeadBuilder {
            session: self.session,
            key: self.key,
        };
        Ok(head.send().await?.is_some())
    }
}
