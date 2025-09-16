use std::fmt::{self, Display};

/// The fully scoped path of an object.
///
/// This consists of a usecase, the scope, and the user-defined object key.
#[derive(Debug)]
pub struct ObjectPath {
    /// The usecase, or "product" this object belongs to.
    ///
    /// This can be defined on-the-fly by the client, but special server logic
    /// (such as the concrete backend/bucket) can be tied to this as well.
    pub usecase: String,

    /// The scope of the object, used for compartmentalization.
    ///
    /// This is treated as a prefix, and includes such things as the organization and project.
    pub scope: String,

    /// This is a user-defined key, which uniquely identifies the object within its usecase/scope.
    pub key: String,
}

impl Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.usecase, self.scope, self.key)
    }
}
