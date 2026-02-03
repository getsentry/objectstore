//! Object key definitions and validation.

/// Maximum length of an object key in ASCII characters, after URL encoding the necessary
/// characters.
pub const MAX_KEY_LENGTH: usize = 128;

/// An object key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    inner: String,
}

/// An error indicating that an object key is invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ObjectKeyError {
    /// The key is empty.
    #[error("object key must not be empty")]
    Empty,
    /// The key exceeds the maximum length.
    #[error("object key exceeds maximum length of {MAX_KEY_LENGTH} characters")]
    TooLong,
}

impl ObjectKey {
    /// Creates an `ObjectKey` from the given key.
    pub fn new(key: impl AsRef<str>) -> Result<Self, ObjectKeyError> {
        let key = key.as_ref();

        if key.is_empty() {
            return Err(ObjectKeyError::Empty);
        }

        let encoded = urlencoding::encode(key);
        if encoded.len() > MAX_KEY_LENGTH {
            return Err(ObjectKeyError::TooLong);
        }

        Ok(Self {
            inner: encoded.to_string(),
        })
    }

    /// Creates an `ObjectKey` from a key which is assumed to be already properly encoded.
    ///
    /// If the key is not properly encoded, this can lead to errors or panics down
    /// the line.
    pub fn from_encoded_unchecked(key: impl AsRef<str>) -> Self {
        Self {
            inner: key.as_ref().to_owned(),
        }
    }

    pub fn as_encoded(&self) -> &str {
        &self.inner
    }
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let decoded =
            urlencoding::decode(&self.inner).expect("encoded ObjectKey can always be decoded");
        write!(f, "{}", decoded);
        Ok(())
    }
}

impl From<&ObjectKey> for http::HeaderValue {
    fn from(key: &ObjectKey) -> Self {
        http::HeaderValue::from_str(&key.inner)
            .expect("encoded ObjectKey is always a valid HeaderValue")
    }
}
