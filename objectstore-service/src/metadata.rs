use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The storage scope for each object
///
/// Each object is stored within a scope. The scope is used for access control, as well as the ability
/// to quickly run queries on all the objects associated with a scope.
/// The scope could also be used as a sharding/routing key in the future.
///
/// The organization / project scope defined here is hierarchical in the sense that
/// analytical aggregations on an organzation level take into account all the project-level objects.
/// However, accessing an object requires supplying both of these original values in order to retrieve it.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct Scope {
    /// The organization ID
    pub organization: u64,

    /// The project ID, if we have a project scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<u64>,
}

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

/// The per-object expiration policy
///
/// We support automatic time-to-live and time-to-idle policies.
/// Setting this to `Manual` means that the object has no automatic policy, and will not be
/// garbage-collected automatically. It essentially lives forever until manually deleted.
#[derive(Debug, PartialEq, Eq)]
pub enum ExpirationPolicy {
    /// Manual expiration, meaning no automatic cleanup.
    Manual,
    /// Time to live, with expiration after the specified duration.
    TimeToLive(Duration),
    /// Time to idle, with expiration once the object has not been accessed within the specified duration.
    TimeToIdle(Duration),
}
