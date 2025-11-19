use std::fmt::{self, Display};

use serde::de;

/// Magic URL segment that separates objectstore context from an object's user-provided key.
const PATH_CONTEXT_SEPARATOR: &str = "objects";

/// An [`ObjectPath`] that may or may not have a user-provided key.
// DO NOT derive Eq, see the implementation of PartialEq below.
#[derive(Debug, Clone)]
pub struct OptionalObjectPath {
    /// The usecase, or "product" this object belongs to.
    pub usecase: String,
    /// The scope of the object, used for compartmentalization.
    pub scope: Vec<String>,
    /// The optional, user-provided key.
    pub key: Option<String>,
}

impl OptionalObjectPath {
    /// Converts to an [`ObjectPath`], generating a unique `key` if none was provided.
    pub fn create_key(self) -> ObjectPath {
        ObjectPath {
            usecase: self.usecase,
            scope: self.scope,
            key: self.key.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        }
    }

    /// Converts to an [`ObjectPath`], returning an error if no `key` was provided.
    pub fn require_key(self) -> Result<ObjectPath, &'static str> {
        Ok(ObjectPath {
            usecase: self.usecase,
            scope: self.scope,
            key: self
                .key
                .ok_or("object key is required but was not provided")?,
        })
    }
}

impl Display for OptionalObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.usecase)?;
        for scope in &self.scope {
            write!(f, "{}/", scope)?;
        }
        write!(f, "{PATH_CONTEXT_SEPARATOR}/")?;
        if let Some(key) = &self.key {
            key.fmt(f)?;
        }
        Ok(())
    }
}

impl PartialEq for OptionalObjectPath {
    fn eq(&self, other: &Self) -> bool {
        self.usecase == other.usecase
            && self.scope == other.scope
            && self.key == other.key
            && self.key.is_some() // Two paths without keys are considered unequal!
    }
}

struct OptionalObjectPathVisitor;
impl<'de> serde::de::Visitor<'de> for OptionalObjectPathVisitor {
    type Value = OptionalObjectPath;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "a string of the following format: `{{usecase}}/{{scope1}}/.../{PATH_CONTEXT_SEPARATOR}/{{key}}`"
        )
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut iter = s.chars().peekable();

        // The first segment in an object path is the usecase (e.g. attachments, debug-files)
        let usecase: String = iter.by_ref().take_while(|c| *c != '/').collect();
        if usecase.is_empty() {
            return Err(E::custom("path is empty or contains leading '/'"));
        }

        // Next is the "scope". This _should_ be one or more key-value pairs where the key and
        // value are separated with a '.' and each pair is separated from the next with a '/'.
        //
        // Examples:
        //   org.123/proj.456
        //   state.wa/city.seattle
        //
        // We know the scope is over when we encounter:
        // - the end of the path
        // - the separator string "data/"
        let mut scope = vec![];
        while iter.by_ref().peek().is_some() {
            let segment: String = iter.by_ref().take_while(|c| *c != '/').collect();

            if segment == PATH_CONTEXT_SEPARATOR {
                break;
            } else if segment.is_empty() {
                return Err(E::custom("scope must not be empty"));
            } else {
                scope.push(segment);
            }
        }

        if scope.is_empty() {
            return Err(E::custom("scope must not be empty"));
        }

        // The rest of the path is a user-provided key.
        let key = if iter.peek().is_some() {
            Some(iter.collect())
        } else {
            None
        };

        Ok(OptionalObjectPath {
            usecase,
            scope,
            key,
        })
    }
}

impl<'de> de::Deserialize<'de> for OptionalObjectPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(OptionalObjectPathVisitor)
    }
}

/// The fully scoped path of an object.
///
/// This consists of a usecase, the scope, and the object key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectPath {
    /// The usecase, or "product" this object belongs to.
    ///
    /// This can be defined on-the-fly by the client, but special server logic
    /// (such as the concrete backend/bucket) can be tied to this as well.
    pub usecase: String,

    /// The scope of the object, used for compartmentalization.
    ///
    /// This is treated as a prefix, and includes such things as the organization and project.
    pub scope: Vec<String>,

    /// This key uniquely identifies the object within its usecase/scope.
    pub key: String,
}

impl Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.usecase)?;
        for scope in &self.scope {
            write!(f, "{}/", scope)?;
        }
        write!(f, "{PATH_CONTEXT_SEPARATOR}/{}", self.key)
    }
}

impl<'de> de::Deserialize<'de> for ObjectPath {
    fn deserialize<D>(deserializer: D) -> Result<ObjectPath, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let optional_path = OptionalObjectPath::deserialize(deserializer)?;
        optional_path.require_key().map_err(de::Error::custom)
    }
}
