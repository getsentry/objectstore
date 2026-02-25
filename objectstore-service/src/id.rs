//! Definitions for object identifiers, including usecases and scopes.
//!
//! This module contains types to define and manage object identifiers:
//!
//!  - [`ObjectId`] is the main identifier type for objects, consisting of a usecase, scopes, and a
//!    key. Every object stored in the object store has a unique `ObjectId`.
//!  - [`Scope`] and [`Scopes`] define hierarchical scopes for objects, which are part of the
//!    `ObjectId`.
//!
//! ## Usecases
//!
//! A usecase (sometimes called a "product") is a top-level namespace like
//! `"attachments"` or `"debug-files"`. It groups related objects and can have
//! usecase-specific server configuration (rate limits, killswitches, etc.).
//!
//! ## Scopes
//!
//! [`Scopes`] are ordered key-value pairs that
//! form a hierarchy within a usecase — for example,
//! `organization=17, project=42`. They serve as both an organizational structure
//! and an authorization boundary. See the
//! [`objectstore-types` docs](objectstore_types) for details on scope validation
//! and formatting.
//!
//! ## Storage paths
//!
//! An [`ObjectId`] is converted to a storage path for backends via
//! [`ObjectId::as_storage_path`]:
//!
//! ```text
//! {usecase}/{scope1_key}.{scope1_value}/{scope2_key}.{scope2_value}/objects/{key}
//! ```
//!
//! For example: `attachments/org.17/project.42/objects/abc123`

use std::fmt;

use objectstore_types::scope::{Scope, Scopes};

/// Defines where an object, or batch of objects, belongs within the object store.
///
/// This is part of the full object identifier for single objects, see [`ObjectId`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectContext {
    /// The usecase, or "product" this object belongs to.
    ///
    /// This can be defined on-the-fly by the client, but special server logic
    /// (such as the concrete backend/bucket) can be tied to this as well.
    pub usecase: String,

    /// The scopes of the object, used for compartmentalization and authorization.
    ///
    /// Scopes are hierarchical key-value pairs that act as containers for objects. The first,
    /// top-level scope can contain sub scopes, like a structured nested folder system. As such,
    /// scopes are used for isolation and access authorization.
    ///
    /// # Ordering
    ///
    /// Note that the order of scopes matters! For example, `organization=17,project=42` indicates
    /// that project _42_ is part of organization _17_. If an object were created with these scopes
    /// reversed, it counts as a different object.
    ///
    /// Not every object within a usecase needs to have the same scopes. It is perfectly valid to
    /// create objects with disjunct or a subset of scopes. However, by convention, we recommend to
    /// use the same scopes for all objects within a usecase where possible.
    ///
    /// # Creation
    ///
    /// To create scopes, collect from an iterator of [`Scope`]s. Since scopes must be validated,
    /// you must use [`Scope::create`] to create them:
    ///
    /// ```
    /// use objectstore_service::id::ObjectContext;
    /// use objectstore_types::scope::{Scope, Scopes};
    ///
    /// let object_id = ObjectContext {
    ///     usecase: "my_usecase".to_string(),
    ///     scopes: Scopes::from_iter([
    ///         Scope::create("organization", "17").unwrap(),
    ///         Scope::create("project", "42").unwrap(),
    ///     ]),
    /// };
    /// ```
    pub scopes: Scopes,
}

/// The fully qualified identifier of an object.
///
/// This consists of a usecase and the scopes, which make up the object's context and define where
/// the object belongs within objectstore, as well as the unique key within the context.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectId {
    /// The usecase and scopes this object belongs to.
    pub context: ObjectContext,

    /// This key uniquely identifies the object within its usecase and scopes.
    ///
    /// Note that keys can be reused across different contexts. Only in combination with the context
    /// a key makes a unique identifier.
    ///
    /// Keys can be assigned by the service. For this, use [`ObjectId::random`].
    pub key: ObjectKey,
}

/// A key that uniquely identifies an object within its usecase and scopes.
pub type ObjectKey = String;

impl ObjectId {
    /// Creates a new `ObjectId` with the given `context` and `key`.
    pub fn new(context: ObjectContext, key: String) -> Self {
        Self::optional(context, Some(key))
    }

    /// Creates a new `ObjectId` from all of its parts.
    pub fn from_parts(usecase: String, scopes: Scopes, key: String) -> Self {
        Self::new(ObjectContext { usecase, scopes }, key)
    }

    /// Creates a unique `ObjectId` with a random key.
    ///
    /// This can be used when creating an object with a server-generated key.
    pub fn random(context: ObjectContext) -> Self {
        Self::optional(context, None)
    }

    /// Creates a new `ObjectId`, generating a key if none is provided.
    ///
    /// This creates a unique key like [`ObjectId::random`] if no `key` is provided, or otherwise
    /// uses the provided `key`.
    pub fn optional(context: ObjectContext, key: Option<String>) -> Self {
        Self {
            context,
            key: key.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        }
    }

    /// Returns the key of the object.
    ///
    /// See [`key`](field@ObjectId::key) for more information.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the context of the object.
    ///
    /// See [`context`](field@ObjectId::context) for more information.
    pub fn context(&self) -> &ObjectContext {
        &self.context
    }

    /// Returns the usecase of the object.
    ///
    /// See [`ObjectContext::usecase`] for more information.
    pub fn usecase(&self) -> &str {
        &self.context.usecase
    }

    /// Returns the scopes of the object.
    ///
    /// See [`ObjectContext::scopes`] for more information.
    pub fn scopes(&self) -> &Scopes {
        &self.context.scopes
    }

    /// Returns an iterator over all scopes of the object.
    ///
    /// See [`ObjectContext::scopes`] for more information.
    pub fn iter_scopes(&self) -> impl Iterator<Item = &Scope> {
        self.context.scopes.iter()
    }

    /// Returns a view that formats this ID as a storage path.
    ///
    /// This will format a hierarchical path in the format
    /// `{usecase}/{scope1.key}.{scope1.value}/.../{key}` that is intended to be used by backends to
    /// reference the object in a storage system.
    pub fn as_storage_path(&self) -> AsStoragePath<'_> {
        AsStoragePath { inner: self }
    }
}

/// A view returned by [`ObjectId::as_storage_path`].
#[derive(Debug)]
pub struct AsStoragePath<'a> {
    inner: &'a ObjectId,
}

impl fmt::Display for AsStoragePath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.inner.context.usecase)?;
        if !self.inner.context.scopes.is_empty() {
            write!(f, "{}/", self.inner.context.scopes.as_storage_path())?;
        }
        write!(f, "objects/{}", self.inner.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_path() {
        let object_id = ObjectId {
            context: ObjectContext {
                usecase: "testing".to_string(),
                scopes: Scopes::from_iter([
                    Scope::create("org", "12345").unwrap(),
                    Scope::create("project", "1337").unwrap(),
                ]),
            },
            key: "foo/bar".to_string(),
        };

        let path = object_id.as_storage_path().to_string();
        assert_eq!(path, "testing/org.12345/project.1337/objects/foo/bar");
    }

    #[test]
    fn test_storage_path_empty_scopes() {
        let object_id = ObjectId {
            context: ObjectContext {
                usecase: "testing".to_string(),
                scopes: Scopes::empty(),
            },
            key: "foo/bar".to_string(),
        };

        let path = object_id.as_storage_path().to_string();
        assert_eq!(path, "testing/objects/foo/bar");
    }
}
