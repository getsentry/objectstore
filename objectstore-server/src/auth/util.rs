use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// Simple enum for deserializing strings where `'*'` represents a special wildcard value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StringOrWildcard {
    /// Wildcard value.
    Wildcard,
    /// Regular, non-wildcard string value.
    String(String),
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
