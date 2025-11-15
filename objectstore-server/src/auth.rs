use serde::{Deserialize, Serialize};

/// Permissions that control whether different operations are authorized.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum Permission {
    /// The permission required to read objects from objectstore.
    #[serde(rename = "object.read")]
    ObjectRead,

    /// The permission required to write/overwrite objects in objectstore.
    #[serde(rename = "object.write")]
    ObjectWrite,

    /// The permission required to delete objects from objectstore.
    #[serde(rename = "object.delete")]
    ObjectDelete,
}
