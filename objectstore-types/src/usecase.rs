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

/// Message for [`InvalidUsecaseName::InvalidLength`].
const INVALID_LENGTH: &str = "invalid usecase name: must be 3-64 ASCII characters long";

/// Message for [`InvalidUsecaseName::InvalidChar`], without the position.
const INVALID_CHAR: &str = "invalid usecase name: expected ASCII letters, digits, '_' or '-'";

/// Storage for a validated [`Usecase`] name.
#[derive(Clone)]
enum Repr {
    /// A name validated by [`Usecase::from_static`], stored without allocating.
    Static(&'static str),
    /// A name validated at runtime by [`Usecase::new`].
    Dynamic(Arc<str>),
}

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
#[derive(Clone)]
pub struct Usecase(Repr);

impl Usecase {
    /// Creates a validated usecase name from a borrowed or owned string.
    ///
    /// Use [`Self::from_static`] for statically defined names.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidUsecaseName`] if the name violates the rules on
    /// [`Usecase`].
    pub fn new(name: impl AsRef<str>) -> Result<Self, InvalidUsecaseName> {
        let name = name.as_ref();
        validate(name)?;
        Ok(Self(Repr::Dynamic(name.into())))
    }

    /// Creates a usecase name from a name known at compile time.
    ///
    /// Use [`Self::new`] for dynamic names. The name is stored without
    /// allocating, so the result can initialize a `static`:
    ///
    /// ```
    /// use objectstore_types::usecase::Usecase;
    ///
    /// static ATTACHMENTS: Usecase = Usecase::from_static("attachments");
    /// assert_eq!(ATTACHMENTS.as_str(), "attachments");
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if the name violates the rules on [`Usecase`]. In a const
    /// context, this panic is reported as a compile error:
    ///
    /// ```compile_fail
    /// use objectstore_types::usecase::Usecase;
    ///
    /// static BROKEN: Usecase = Usecase::from_static("attachments/v2");
    /// ```
    #[track_caller]
    pub const fn from_static(name: &'static str) -> Self {
        match validate(name) {
            Ok(()) => Self(Repr::Static(name)),
            // Const panics only accept a single `&str`, so the position is omitted.
            Err(InvalidUsecaseName::InvalidLength) => panic!("{}", INVALID_LENGTH),
            Err(InvalidUsecaseName::InvalidChar(_)) => panic!("{}", INVALID_CHAR),
        }
    }

    /// Returns the usecase name as a string slice.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            Repr::Static(name) => name,
            Repr::Dynamic(name) => name,
        }
    }
}

impl fmt::Debug for Usecase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Usecase").field(&self.as_str()).finish()
    }
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
        deserializer.deserialize_str(UsecaseVisitor)
    }
}

/// An error indicating that a usecase name is invalid.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidUsecaseName {
    /// The name is outside the permitted length range.
    #[error("{message}", message = INVALID_LENGTH)]
    InvalidLength,
    /// The name contains a character outside the permitted set, starting at
    /// this byte position.
    #[error("{message}, at position {0}", message = INVALID_CHAR)]
    InvalidChar(usize),
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
            return Err(InvalidUsecaseName::InvalidChar(i));
        }
        i += 1;
    }
    Ok(())
}

/// Deserializes a [`Usecase`] from a string or UTF-8 bytes.
struct UsecaseVisitor;

impl serde::de::Visitor<'_> for UsecaseVisitor {
    type Value = Usecase;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a usecase name")
    }

    fn visit_str<E>(self, name: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Usecase::new(name).map_err(E::custom)
    }

    fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let name = std::str::from_utf8(bytes)
            .map_err(|_| E::invalid_value(serde::de::Unexpected::Bytes(bytes), &self))?;
        self.visit_str(name)
    }
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
        for (name, position) in [
            ("testing/org=999/injected", 7),
            ("test.name", 4),
            ("test name", 4),
            ("café", 3),
            ("ab€cd", 2),
            ("ab🦀cd", 2),
        ] {
            assert_eq!(
                Usecase::new(name),
                Err(InvalidUsecaseName::InvalidChar(position))
            );
        }
        assert_eq!(
            InvalidUsecaseName::InvalidChar(7).to_string(),
            "invalid usecase name: expected ASCII letters, digits, '_' or '-', at position 7"
        );
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
        assert_eq!(format!("{dynamic:?}"), r#"Usecase("debug-files")"#);
        assert_eq!(format!("{static_:?}"), r#"Usecase("debug-files")"#);
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
        // Escapes can't be borrowed from the input, so this takes the transient path.
        let escaped = r#""Debug\u005fFiles-123""#;
        assert_eq!(serde_json::from_str::<Usecase>(escaped).unwrap(), name);
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
