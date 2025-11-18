use std::fmt::{self, Display};

use serde::de;

/// Magic URL segment that separates objectstore context from an object's user-provided key.
const PATH_CONTEXT_SEPARATOR: &str = "objects";

/// The fully scoped path of an object.
///
/// This consists of a usecase, the scope, and the user-defined object key.
#[derive(Debug, Clone)]
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

    /// This is a user-defined key, which uniquely identifies the object within its usecase/scope.
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

struct ObjectPathVisitor;
impl<'de> serde::de::Visitor<'de> for ObjectPathVisitor {
    type Value = ObjectPath;

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
        // If no key is provided, generate a UUIDv4 by default.
        let key = match iter.peek() {
            None => uuid::Uuid::new_v4().to_string(),
            Some(_) => iter.collect(),
        };

        Ok(ObjectPath {
            usecase,
            scope,
            key,
        })
    }
}

impl<'de> de::Deserialize<'de> for ObjectPath {
    fn deserialize<D>(deserializer: D) -> Result<ObjectPath, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(ObjectPathVisitor)
    }
}
