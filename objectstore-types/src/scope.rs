//! Definitions for object scops.
//!
//! This module contains types to define and manage the hierarchical organization of objects:
//!
//!  - [`Scope`] is a single key-value pair representing one level of hierarchy
//!  - [`Scopes`] is an ordered collection of [`Scope`]s

use std::fmt::{self, Write};

/// A single scope value of an object.
///
/// Scopes are used in a hierarchy in object IDs.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Scope {
    /// Identifies the scope.
    ///
    /// Examples are `organization` or `project`.
    pub name: String,
    /// The value of the scope.
    ///
    /// This can be the identifier of a
    pub value: String,
}

impl Scope {
    /// Creates and validates a new scope.
    ///
    /// The name and value must be non-empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use objectstore_types::scope::Scope;
    ///
    /// let scope = Scope::create("organization", "17").unwrap();
    /// assert_eq!(scope.name(), "organization");
    /// assert_eq!(scope.value(), "17");
    ///
    /// // Empty names or values are invalid
    /// let invalid_scope = Scope::create("", "value");
    /// assert!(invalid_scope.is_err());
    /// ```
    pub fn create<V>(name: &str, value: V) -> Result<Self, InvalidScopeError>
    where
        V: fmt::Display,
    {
        let value = value.to_string();
        if name.is_empty() || value.is_empty() {
            return Err(InvalidScopeError);
        }

        Ok(Self {
            name: name.to_owned(),
            value,
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
#[derive(Debug, thiserror::Error)]
#[error("invalid scope: key and value must be non-empty")]
pub struct InvalidScopeError;

/// An ordered set of resource scopes.
///
/// Scopes are used to create hierarchical identifiers for objects.
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

    /// Pushes a new scope to the collection.
    pub fn push<V>(&mut self, key: &str, value: V) -> Result<(), InvalidScopeError>
    where
        V: fmt::Display,
    {
        self.scopes.push(Scope::create(key, value)?);
        Ok(())
    }

    /// Returns a view that formats the scopes as path for storage.
    ///
    /// This will serialize the scopes as `{name}.{value}/...`, which is intended to be used by
    /// backends to reference the object in a storage system. This becomes part of the storage path
    /// of an [`ObjectId`].
    pub fn as_storage_path(&self) -> AsStoragePath<'_> {
        AsStoragePath { inner: self }
    }

    /// Returns a view that formats the scopes as path for web API usage.
    ///
    /// This will serialize the scopes as `{name}={value};...`, which is intended to be used by
    /// clients to format URL paths.
    pub fn as_api_path(&self) -> AsApiPath<'_> {
        AsApiPath { inner: self }
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

/// A view returned by [`Scopes::as_storage_path`].
#[derive(Debug)]
pub struct AsStoragePath<'a> {
    inner: &'a Scopes,
}

impl fmt::Display for AsStoragePath<'_> {
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

/// A view returned by [`Scopes::as_api_path`].
#[derive(Debug)]
pub struct AsApiPath<'a> {
    inner: &'a Scopes,
}

impl fmt::Display for AsApiPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some((first, rest)) = self.inner.scopes.split_first() {
            write!(f, "{}={}", first.name, first.value)?;
            for scope in rest {
                write!(f, ";{}={}", scope.name, scope.value)?;
            }
            Ok(())
        } else {
            f.write_char('_')
        }
    }
}
