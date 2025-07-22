use serde::Deserialize;
use std::time::Duration;

/// Denotes the storage usecase
///
/// Different decisions can be made based on this usecase, such as using a different underlying
/// storage backend.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Usecase {
    Attachments,
}

/// The storage scope for each object
///
/// Each object is stored within a scope. The scope is used for access control, as well as the ability
/// to quickly run queries on all the objects associated with a scope.
/// The scope could also be used as a sharding/routing key in the future.
///
/// While the storage service offers organization and project scopes, it does not maintain any
/// association between those two, and it is up to the user to provide all projects belonging to an
/// organization.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Organization(u64),
    Project(u64),
}

/// The per-object expiration policy
///
/// We support time-to-live and time-to-idle policies.
#[derive(Debug, PartialEq, Eq)]
pub enum ExpirationPolicy {
    TimeToLive(Duration),
    TimeToIdle(Duration),
}

/// The full object key
///
/// This consists of a usecase, the scope, and the free-form path/key, which might be auto-generated.
pub struct ObjectKey {
    pub usecase: Usecase,
    pub scope: Scope,
    pub key: String,
}
