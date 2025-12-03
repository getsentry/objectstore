use std::fmt::{self, Display};

use serde::de;

use crate::{SentryScope, StringOrWildcard};

/// Magic URL segment that separates objectstore context from an object's user-provided key.
const PATH_CONTEXT_SEPARATOR: &str = "objects";
const PATH_ORG_KEY: &str = "org";
const PATH_PROJECT_KEY: &str = "project";

/// An [`ObjectPath`] that may or may not have a user-provided key.
// DO NOT derive Eq, see the implementation of PartialEq below.
#[derive(Debug, Clone)]
pub struct OptionalObjectPath {
    /// The usecase, or "product" this object belongs to.
    pub usecase: String,
    /// The scope of the object, used for compartmentalization.
    pub scope: SentryScope,
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
        write!(
            f,
            "{}/{}/{PATH_CONTEXT_SEPARATOR}/",
            self.usecase, self.scope
        )?;
        if let Some(ref key) = self.key {
            f.write_str(key)?;
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

        // Next is the "scope", which for the foreseeable future is a Sentry organization and
        // optionally a Sentry project. The organization is serialized as `org.{value}` and the
        // project, if it exists, is serialized as `proj.{value}`. The two are separated with a
        // slash.
        //
        // Example:
        //   org.123/proj.456
        //
        // The scope is followed by a separator string `"data/"` in case we ever introduce more
        // scope schemes and need to make this parsing more dynamic.

        // Parse the organization. Burn the `org.` and then read the value.
        if PATH_ORG_KEY != iter.by_ref().take_while(|c| *c != '.').collect::<String>() {
            return Err(E::custom("scope must begin with `org.` key"));
        }
        let org = iter.by_ref().take_while(|c| *c != '/').collect();

        // Using a clone of `iter`, check whether the `project.` key is present. If it is,
        // advance `iter` to skip it and then parse the associated value.
        let mut project = None;
        if PATH_PROJECT_KEY == iter.clone().take_while(|c| *c != '.').collect::<String>() {
            project = Some(StringOrWildcard::String(
                iter.by_ref()
                    .skip_while(|c| *c != '.')
                    .take_while(|c| *c != '/')
                    .collect(),
            ));
        }

        if PATH_CONTEXT_SEPARATOR != iter.by_ref().take_while(|c| *c != '/').collect::<String>() {
            return Err(E::custom(
                "scope must be followed by path context separator",
            ));
        }

        let scope = SentryScope { org, project };

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
    pub scope: SentryScope,

    /// This key uniquely identifies the object within its usecase/scope.
    pub key: String,
}

impl Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{PATH_CONTEXT_SEPARATOR}/{}",
            self.usecase, self.scope, self.key
        )
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
