//! Validated usecase names shared by clients and the server.

use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

/// Minimum length of a usecase name in bytes.
pub const MIN_USECASE_NAME_LENGTH: usize = 3;

/// Maximum length of a usecase name in bytes.
pub const MAX_USECASE_NAME_LENGTH: usize = 64;

/// A validated, case-sensitive name for an Objectstore workload.
///
/// Names must be 3–64 ASCII characters from `[A-Za-z0-9_-]`. Any permitted
/// character may appear in any position, including names consisting entirely of
/// underscores or hyphens. Names are never normalized.
///
/// Construction and deserialization both validate the name. The contents cannot
/// be mutated after construction.
///
/// ```
/// use objectstore_types::usecase::Usecase;
///
/// let name = Usecase::new("debug-files")?;
/// assert_eq!(name.as_str(), "debug-files");
/// assert!(Usecase::new("testing/org=999/injected").is_err());
/// # Ok::<(), objectstore_types::usecase::InvalidUsecaseName>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Usecase(String);

impl Usecase {
    /// Creates a validated usecase name from a borrowed or owned string.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidUsecaseName`] if the name violates the rules on
    /// [`Usecase`].
    pub fn new(name: impl Into<String>) -> Result<Self, InvalidUsecaseName> {
        let name = name.into();
        if !(MIN_USECASE_NAME_LENGTH..=MAX_USECASE_NAME_LENGTH).contains(&name.len()) {
            return Err(InvalidUsecaseName::InvalidLength);
        }
        for c in name.chars() {
            if !c.is_ascii_alphanumeric() && c != '_' && c != '-' {
                return Err(InvalidUsecaseName::InvalidChar(c));
            }
        }
        Ok(Self(name))
    }

    /// Returns the usecase name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Usecase {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Usecase {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Usecase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Usecase {
    type Err = InvalidUsecaseName;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::new(name)
    }
}

impl TryFrom<&str> for Usecase {
    type Error = InvalidUsecaseName;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Self::new(name)
    }
}

impl TryFrom<String> for Usecase {
    type Error = InvalidUsecaseName;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::new(name)
    }
}

impl From<Usecase> for String {
    fn from(name: Usecase) -> Self {
        name.0
    }
}

impl<'de> Deserialize<'de> for Usecase {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// An error indicating that a usecase name is invalid.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidUsecaseName {
    /// The name is outside the permitted length range.
    #[error("usecase name must be 3–64 ASCII characters long")]
    InvalidLength,
    /// The name contains a character outside the permitted set.
    #[error("invalid character {0:?} in usecase name; expected ASCII letters, digits, '_' or '-'")]
    InvalidChar(char),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_names() {
        for name in [
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-",
            "___",
        ] {
            assert_eq!(Usecase::new(name).unwrap().as_str(), name);
        }
        for name in ["", "_", "ab", &"a".repeat(65)] {
            assert_eq!(Usecase::new(name), Err(InvalidUsecaseName::InvalidLength));
        }
        for (name, character) in [
            ("testing/org=999/injected", '/'),
            ("test.name", '.'),
            ("test name", ' '),
            ("café", 'é'),
        ] {
            assert_eq!(
                Usecase::new(name),
                Err(InvalidUsecaseName::InvalidChar(character))
            );
        }
    }

    #[test]
    fn serde_validates_and_uses_strings() {
        let name = Usecase::new("Debug_Files-123").unwrap();
        let serialized = serde_json::to_string(&name).unwrap();
        assert_eq!(serialized, r#""Debug_Files-123""#);
        assert_eq!(serde_json::from_str::<Usecase>(&serialized).unwrap(), name);
        assert!(serde_json::from_str::<Usecase>(r#""_""#).is_err());
    }

    #[test]
    fn string_conversions_preserve_names() {
        let input = "Debug_Files-123";
        let name: Usecase = input.parse().unwrap();
        assert_eq!(name.as_ref(), input);
        assert_eq!(name.to_string(), input);
        let owned = String::from(name);
        assert_eq!(Usecase::try_from(owned).unwrap().as_str(), input);
    }
}
