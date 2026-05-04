use crate::{ObjectKey, Session};

/// Response type for exists operations.
pub type ExistsResponse = bool;

impl Session {
    /// Creates an exists check for the given `key` (for use in batch requests).
    pub fn exists(&self, key: &str) -> ExistsBuilder {
        ExistsBuilder {
            session: self.clone(),
            key: key.to_owned(),
        }
    }
}

/// An [`exists`](Session::exists) request builder, for use with [`ManyBuilder::push`](crate::ManyBuilder::push).
#[derive(Debug)]
pub struct ExistsBuilder {
    pub(crate) session: Session,
    pub(crate) key: ObjectKey,
}
