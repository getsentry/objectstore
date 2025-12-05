//! Definitions for object identifiers, including usecases and scopes.
//!
//! This module contains types to define and manage object identifiers:
//!
//!  - [`ObjectId`] is the main identifier type for objects, consisting of a usecase, scopes, and a
//!    key. Every object stored in the object store has a unique `ObjectId`.
//!  - [`Scope`] and [`Scopes`] define hierarchical scopes for objects, which are part of the
//!    `ObjectId`.

use std::fmt;
use thiserror::Error;

/// A single scope value of an object.
///
/// Scopes are used in a hierarchy in object IDs, see [`ObjectId::scopes`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Scope {
    /// Identifies the scope.
    ///
    /// Examples are `organization` or `project`.
    name: String,
    /// The value of the scope.
    ///
    /// This can be the identifier of a
    value: String,
}

impl Scope {
    /// Creates and validates a new scope.
    ///
    /// The name and value must be non-empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use objectstore_service::id::Scope;
    ///
    /// let scope = Scope::create("organization", "17").unwrap();
    /// assert_eq!(scope.name(), "organization");
    /// assert_eq!(scope.value(), "17");
    ///
    /// // Empty names or values are invalid
    /// let invalid_scope = Scope::create("", "value");
    /// assert!(invalid_scope.is_err());
    /// ```
    pub fn create(name: &str, value: &str) -> Result<Self, InvalidScopeError> {
        if name.is_empty() || value.is_empty() {
            return Err(InvalidScopeError);
        }

        Ok(Self {
            name: name.to_owned(),
            value: value.to_owned(),
        })
    }

    /// Returns the name of the scope.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the value of the scope.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// An error indicating that a scope is invalid, returned by [`Scope::create`].
#[derive(Debug, Error)]
#[error("invalid scope: key and value must be non-empty")]
pub struct InvalidScopeError;

/// An ordered set of resource scopes.
///
/// Scopes are used to create identifiers for objects, see [`ObjectId::scopes`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scopes {
    scopes: Vec<Scope>,
}

impl Scopes {
    /// Returns an empty set of scopes.
    pub fn empty() -> Self {
        Self { scopes: vec![] }
    }

    /// Returns `true` if there are no scopes.
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }

    /// Returns the scope with the given key, if it exists.
    pub fn get(&self, key: &str) -> Option<&Scope> {
        self.scopes.iter().find(|s| s.name() == key)
    }

    /// Returns the value of the scope with the given key, if it exists.
    pub fn get_value(&self, key: &str) -> Option<&str> {
        self.get(key).map(|s| s.value())
    }

    /// Returns an iterator over all scopes.
    pub fn iter(&self) -> impl Iterator<Item = &Scope> {
        self.into_iter()
    }

    /// Returns a view that formats the scopes as path for storage.
    ///
    /// This will serialize the scopes as `{scope1.key}.{scope1.value}/...`, which is intended to be
    /// used by backends to reference the object in a storage system. This becomes part of the
    /// storage path of an [`ObjectId`].
    pub fn as_storage_path(&self) -> AsStoragePath<'_, Self> {
        AsStoragePath { inner: self }
    }
}

impl<'a> IntoIterator for &'a Scopes {
    type IntoIter = std::slice::Iter<'a, Scope>;
    type Item = &'a Scope;

    fn into_iter(self) -> Self::IntoIter {
        self.scopes.iter()
    }
}

impl FromIterator<Scope> for Scopes {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Scope>,
    {
        Self {
            scopes: iter.into_iter().collect(),
        }
    }
}

/// The fully qualified identifier of an object.
///
/// This consists of a usecase, the scopes, and the key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectId {
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
    /// use objectstore_service::id::{ObjectId, Scope, Scopes};
    ///
    /// let object_id = ObjectId {
    ///     usecase: "my_usecase".to_string(),
    ///     scopes: Scopes::from_iter([
    ///         Scope::create("organization", "17").unwrap(),
    ///         Scope::create("project", "42").unwrap(),
    ///     ]),
    ///     key: "my_object_key".to_string(),
    /// };
    /// ```
    pub scopes: Scopes,

    /// This key uniquely identifies the object within its usecase and scopes.
    ///
    /// Note that keys can be reused across different scopes. Only in combination with the usecase
    /// and scopes a key makes a unique identifier.
    ///
    /// Keys can be assigned by the service. For this, use [`ObjectId::random`].
    pub key: String,
}

impl ObjectId {
    /// Creates a unique `ObjectId` with a random key.
    ///
    /// This can be used when creating an object with a server-generated key.
    pub fn random(usecase: String, scopes: Scopes) -> Self {
        Self::optional(usecase, scopes, None)
    }

    /// Creates a new `ObjectId`, generating a key if none is provided.
    ///
    /// This creates a unique key like [`ObjectId::random`] if no `key` is provided, or otherwise
    /// uses the provided `key`.
    pub fn optional(usecase: String, scopes: Scopes, key: Option<String>) -> Self {
        Self {
            usecase,
            scopes,
            key: key.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        }
    }

    /// Returns a view that formats this ID as a storage path.
    ///
    /// This will format a hierarchical path in the format
    /// `{usecase}/{scope1.key}.{scope1.value}/.../{key}` that is intended to be used by backends to
    /// reference the object in a storage system.
    pub fn as_storage_path(&self) -> AsStoragePath<'_, Self> {
        AsStoragePath { inner: self }
    }
}

/// A view that formats a supported type as a storage path.
///
/// See [`ObjectId::as_storage_path`] and [`Scopes::as_storage_path`] for more information.
#[derive(Debug)]
pub struct AsStoragePath<'a, T> {
    inner: &'a T,
}

impl fmt::Display for AsStoragePath<'_, Scopes> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, scope) in self.inner.iter().enumerate() {
            if i > 0 {
                write!(f, "/")?;
            }
            write!(f, "{}.{}", scope.name, scope.value)?;
        }
        Ok(())
    }
}

impl fmt::Display for AsStoragePath<'_, ObjectId> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.inner.usecase)?;
        if !self.inner.scopes.is_empty() {
            write!(f, "{}/", self.inner.scopes.as_storage_path())?;
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
            usecase: "testing".to_string(),
            scopes: Scopes::from_iter([
                Scope::create("org", "12345").unwrap(),
                Scope::create("project", "1337").unwrap(),
            ]),
            key: "foo/bar".to_string(),
        };

        let path = object_id.as_storage_path().to_string();
        assert_eq!(path, "testing/org.12345/project.1337/objects/foo/bar");
    }

    #[test]
    fn test_storage_path_empty_scopes() {
        let object_id = ObjectId {
            usecase: "testing".to_string(),
            scopes: Scopes::empty(),
            key: "foo/bar".to_string(),
        };

        let path = object_id.as_storage_path().to_string();
        assert_eq!(path, "testing/objects/foo/bar");
    }
}
