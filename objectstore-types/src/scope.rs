//! Definitions for object scops.
//!
//! This module contains types to define and manage the hierarchical organization of objects:
//!
//!  - [`Scope`] is a single key-value pair representing one level of hierarchy
//!  - [`Scopes`] is an ordered collection of [`Scope`]s

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
