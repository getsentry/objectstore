//! Authentication and authorization types.
//!
//! Permissions are carried in JWT tokens and checked by the server's
//! authorization layer before each operation.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// Permissions that control whether different operations are authorized.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Read / download objects (serialized as `"object.read"`).
    #[serde(rename = "object.read")]
    ObjectRead,

    /// Create / overwrite objects (serialized as `"object.write"`).
    #[serde(rename = "object.write")]
    ObjectWrite,

    /// Delete objects (serialized as `"object.delete"`).
    #[serde(rename = "object.delete")]
    ObjectDelete,
}

impl Permission {
    /// Convenience function for creating a set with read, write, and delete permissions.
    pub fn rwd() -> HashSet<Permission> {
        HashSet::from([
            Permission::ObjectRead,
            Permission::ObjectWrite,
            Permission::ObjectDelete,
        ])
    }
}
