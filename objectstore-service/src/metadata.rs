use objectstore_types::Scope;

/// The full object key
///
/// This consists of a usecase, the scope, and the free-form path/key, which might be auto-generated.
#[derive(Debug)]
pub struct ObjectKey {
    /// The usecase, or "product" this object belongs to.
    ///
    /// This can be defined on-the-fly by the client, but special server logic
    /// (such as the concrete backend/bucket) can be tied to this as well.
    pub usecase: String,

    /// The scope of the object, used for access control and compartmentalization.
    pub scope: Scope,

    /// This is the storage key of the object, unique within the usecase/scope.
    pub key: String,
}
