use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};

/// Simple enum for deserializing strings where `'*'` represents a special wildcard value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StringOrWildcard {
    /// Regular, non-wildcard string value.
    String(String),
    /// Wildcard value.
    Wildcard,
}

impl<'de> Deserialize<'de> for StringOrWildcard {
    fn deserialize<D>(deserializer: D) -> Result<StringOrWildcard, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "*" => StringOrWildcard::Wildcard,
            _ => StringOrWildcard::String(value),
        })
    }
}

impl Serialize for StringOrWildcard {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl Display for StringOrWildcard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StringOrWildcard::Wildcard => f.write_str("*"),
            StringOrWildcard::String(s) => f.write_str(s),
        }
    }
}

/// Represents a Sentry organization and, where applicable, a specific project within that
/// organization.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct SentryScope {
    /// Sentry organization ID.
    pub org: String,

    /// Optional Sentry project ID.
    /// - If not specified, the scope applies at the org-level only and not to any projects
    /// - If specified as a wildcard, the scope applies to all projects within the org
    /// - If specified as a specific project, the scope applies to that project within the org
    pub project: Option<StringOrWildcard>,
}

impl SentryScope {
    /// Utility function for determining whether this `SentryScope` is a superset of another
    /// `SentryScope`.
    pub fn contains(&self, other: &SentryScope) -> bool {
        // If the organizations don't match, we don't contain `other`
        if self.org != other.org {
            return false;
        }

        match (&self.project, &other.project) {
            // If neither scope has a project, we contain `other`
            (None, None) => true,
            // If both scopes have a project, and they're equal, we contain `other`
            (Some(StringOrWildcard::String(l)), Some(StringOrWildcard::String(r))) => l == r,
            // If both scopes have a project, and ours is a wildcard, we contain `other`
            (Some(StringOrWildcard::Wildcard), Some(_)) => true,
            // Otherwise, we don't contain `other`
            _ => false,
        }
    }
}

impl Display for SentryScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "org.{}", self.org)?;
        if let Some(StringOrWildcard::String(s)) = &self.project {
            write!(f, "/project.{}", s)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize_string_or_wildcard() {
        assert_eq!(
            serde_json::from_str::<StringOrWildcard>("\"*\"").unwrap(),
            StringOrWildcard::Wildcard
        );
        assert_eq!(
            serde_json::from_str::<StringOrWildcard>("\"abcde\"").unwrap(),
            StringOrWildcard::String("abcde".into()),
        );
        assert_eq!(
            serde_json::from_str::<StringOrWildcard>("\"*abcde\"").unwrap(),
            StringOrWildcard::String("*abcde".into()),
        );
    }

    #[test]
    fn test_serialize_string_or_wildcard() {
        assert_eq!(
            serde_json::to_string(&StringOrWildcard::Wildcard).unwrap(),
            "\"*\"".to_string(),
        );
        assert_eq!(
            serde_json::to_string(&StringOrWildcard::String("abcde".into())).unwrap(),
            "\"abcde\"".to_string(),
        );
    }

    #[test]
    fn test_display_string_or_wildcard() {
        assert_eq!("*", StringOrWildcard::Wildcard.to_string());
        assert_eq!(
            "abcde",
            StringOrWildcard::String("abcde".into()).to_string()
        );
        assert_eq!(
            "*abcde",
            StringOrWildcard::String("*abcde".into()).to_string()
        );
    }
}
