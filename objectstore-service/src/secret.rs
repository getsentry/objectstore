//! Newtype for redacted, zeroized secret values used in backend configuration.

use std::fmt;

use secrecy::{CloneableSecret, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};

/// Newtype around `String` that may protect against accidental
/// logging of secrets in our configuration struct. Use with
/// [`secrecy::SecretBox`].
#[derive(Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ConfigSecret(String);

impl ConfigSecret {
    /// Returns the secret value as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<&str> for ConfigSecret {
    fn from(str: &str) -> Self {
        ConfigSecret(str.to_string())
    }
}

impl std::ops::Deref for ConfigSecret {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for ConfigSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "[redacted]")
    }
}

impl CloneableSecret for ConfigSecret {}
impl SerializableSecret for ConfigSecret {}
impl Zeroize for ConfigSecret {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}
