//! Validated usecase names shared by clients and the server.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
///
/// Use [`Usecase::from_static`] for statically known names and [`Usecase::new`]
/// for dynamic names.
#[derive(Clone, Debug)]
pub struct Usecase(Name);

/// Storage for a validated [`Usecase`] name.
#[derive(Clone, Debug)]
enum Name {
    /// A name validated by [`Usecase::from_static`], stored without allocating.
    Static(&'static str),
    /// A name validated at runtime by [`Usecase::new`].
    Dynamic(Arc<str>),
}

impl Usecase {
    /// Creates a validated usecase name from a borrowed or owned string.
    ///
    /// Use [`Self::from_static`] for statically defined names.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidUsecaseName`] if the name violates the rules on
    /// [`Usecase`].
    pub fn new(name: impl Into<String>) -> Result<Self, InvalidUsecaseName> {
        let name = name.into();
        validate(&name)?;
        Ok(Self(Name::Dynamic(name.into())))
    }

    /// Creates a usecase name from a name known at compile time.
    ///
    /// Use [`Self::new`] for dynamic names.
    ///
    /// # Panics
    ///
    /// Panics if the name violates the rules on [`Usecase`]. In a const
    /// context, this panic is reported as a compile error.
    pub const fn from_static(name: &'static str) -> Self {
        match validate(name) {
            Ok(()) => Self(Name::Static(name)),
            Err(InvalidUsecaseName::InvalidLength) => {
                panic!("invalid usecase name: must be 3–64 ASCII characters long")
            }
            Err(InvalidUsecaseName::InvalidChar(_)) => {
                panic!("invalid usecase name: expected only ASCII letters, digits, '_' or '-'")
            }
        }
    }

    /// Returns the usecase name as a string slice.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            Name::Static(name) => name,
            Name::Dynamic(name) => name,
        }
    }
}

/// Checks whether a name follows the rules on [`Usecase`].
const fn validate(name: &str) -> Result<(), InvalidUsecaseName> {
    let bytes = name.as_bytes();
    if bytes.len() < MIN_USECASE_NAME_LENGTH || bytes.len() > MAX_USECASE_NAME_LENGTH {
        return Err(InvalidUsecaseName::InvalidLength);
    }
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if !b.is_ascii_alphanumeric() && b != b'_' && b != b'-' {
            // All preceding bytes are ASCII, so `i` is a char boundary.
            return Err(InvalidUsecaseName::InvalidChar(decode_char(bytes, i)));
        }
        i += 1;
    }
    Ok(())
}

impl PartialEq for Usecase {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Usecase {}

impl PartialOrd for Usecase {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Usecase {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for Usecase {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
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
        name.as_str().to_owned()
    }
}

impl Serialize for Usecase {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
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

/// Decodes the UTF-8 encoded char starting at byte index `i` of a valid `str`.
const fn decode_char(bytes: &[u8], i: usize) -> char {
    let lead = bytes[i] as u32;
    let (len, mut code) = match lead {
        0x00..0x80 => (1, lead),
        0xC0..0xE0 => (2, lead & 0x1F),
        0xE0..0xF0 => (3, lead & 0x0F),
        _ => (4, lead & 0x07),
    };
    let mut j = 1;
    while j < len {
        code = (code << 6) | (bytes[i + j] as u32 & 0x3F);
        j += 1;
    }
    match char::from_u32(code) {
        Some(c) => c,
        None => char::REPLACEMENT_CHARACTER,
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
            ("ab€cd", '€'),
            ("ab🦀cd", '🦀'),
        ] {
            assert_eq!(
                Usecase::new(name),
                Err(InvalidUsecaseName::InvalidChar(character))
            );
            assert_eq!(
                validate(name),
                Err(InvalidUsecaseName::InvalidChar(character))
            );
        }
    }

    #[test]
    fn from_static_validates_in_const_context() {
        const NAME: Usecase = Usecase::from_static("debug-files");
        assert_eq!(NAME.as_str(), "debug-files");
    }

    #[test]
    #[should_panic(expected = "invalid usecase name")]
    fn from_static_panics_on_invalid_name() {
        Usecase::from_static("testing/org=999/injected");
    }

    #[test]
    fn static_and_dynamic_names_are_equal() {
        use std::collections::HashSet;

        let dynamic = Usecase::new("debug-files").unwrap();
        let static_ = Usecase::from_static("debug-files");
        assert_eq!(dynamic, static_);
        assert_eq!(dynamic.cmp(&static_), Ordering::Equal);
        let set = HashSet::from([dynamic]);
        assert!(set.contains(&static_));
        assert!(set.contains("debug-files"));
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
