//! New object ID, supposed to replace ObjectPath
//!
//! TODO(ja): Doc

use std::fmt;

/// TODO(ja): Doc
#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Scope {
    /// TODO(ja): Doc
    key: String,
    /// TODO(ja): Doc
    value: String,
}

impl Scope {
    /// TODO(ja): Doc
    pub fn create(key: &str, value: &str) -> Result<Self, InvalidScopeError> {
        if key.is_empty() || value.is_empty() {
            // TODO: More specific error handling
            return Err(InvalidScopeError);
        }

        Ok(Self {
            key: key.to_owned(),
            value: value.to_owned(),
        })
    }

    /// TODO(ja): Doc
    pub fn key(&self) -> &str {
        &self.key
    }

    /// TODO(ja): Doc
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// TODO(ja): Doc
#[derive(Debug)]
pub struct InvalidScopeError;

impl fmt::Display for InvalidScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid scope: key and value must be non-empty")
    }
}

// TODO(ja): Use thiserror instead
impl std::error::Error for InvalidScopeError {}

/// TODO(ja): Doc
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scopes {
    /// TODO(ja): Doc
    scopes: Vec<Scope>,
}

/// TODO(ja): Doc
// TODO: Move to service crate
impl Scopes {
    /// TODO(ja): Doc
    pub fn empty() -> Self {
        Self { scopes: vec![] }
    }

    /// TODO(ja): Doc
    pub fn get(&self, key: &str) -> Option<&Scope> {
        self.scopes.iter().find(|s| s.key() == key)
    }

    /// TODO(ja): Doc
    pub fn get_value(&self, key: &str) -> Option<&str> {
        self.get(key).map(|s| s.value())
    }

    /// TODO(ja): Doc
    pub fn iter(&self) -> impl Iterator<Item = &Scope> {
        self.into_iter()
    }

    /// TODO(ja): Doc
    pub fn as_storage_path(&self) -> AsStoragePath<'_, Self> {
        AsStoragePath { inner: self }
    }

    /// TODO(ja): Doc
    #[deprecated(note = "use `as_storage_path` for formatting instead")]
    pub fn into_storage_strings(self) -> Vec<String> {
        self.scopes
            .into_iter()
            .map(|s| format!("{}.{}", s.key(), s.value()))
            .collect()
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

    /// The scopes of the object, used for compartmentalization.
    ///
    /// This is treated as a prefix, and includes such things as the organization and project.
    pub scopes: Scopes,

    /// This key uniquely identifies the object within its usecase/scope.
    pub key: String,
}

impl ObjectId {
    /// TODO(ja): Doc
    pub fn as_storage_path(&self) -> AsStoragePath<'_, Self> {
        AsStoragePath { inner: self }
    }
}

/// TODO(ja): Doc
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
            write!(f, "{}.{}", scope.key, scope.value)?;
        }
        Ok(())
    }
}

impl fmt::Display for AsStoragePath<'_, ObjectId> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.inner.usecase,
            self.inner.scopes.as_storage_path(),
            self.inner.key
        )
    }
}
