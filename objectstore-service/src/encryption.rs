//! Authenticated encryption for serializable values.

use std::collections::BTreeMap;
use std::fmt;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Serialize, de::DeserializeOwned};

use crate::error::{Error, ErrorKind, Result, ResultExt as _};

/// AES-GCM nonce length in bytes.
const NONCE_LENGTH: usize = 12;
/// AES-GCM authentication tag length in bytes.
const TAG_LENGTH: usize = 16;
/// Encrypted envelope format version.
const FORMAT_VERSION: u8 = 0;

/// Errors produced while decrypting an encrypted value.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CipherError {
    #[error("encrypted value is malformed")]
    MalformedEnvelope,
    #[error("unsupported encrypted value format version {0}")]
    UnsupportedVersion(u8),
    #[error("encrypted value has an invalid key ID")]
    InvalidKeyId,
    #[error("encrypted value could not be authenticated")]
    Authentication,
    #[error("encrypted value could not be deserialized")]
    Deserialization(#[source] serde_json::Error),
}

/// Encrypts and decrypts serializable values with AES-256-GCM.
///
/// The active key encrypts new values, while the key ID embedded in an existing envelope selects
/// its decryption key.
pub struct Cipher {
    active_key_id: String,
    active_key: LessSafeKey,
    decryption_keys: BTreeMap<String, LessSafeKey>,
    random: SystemRandom,
}

impl Cipher {
    /// Constructs a cipher with a fresh process-local AES-256-GCM key.
    ///
    /// Values encrypted with this key cannot be decrypted after a service restart. Configure a
    /// persistent keyring with [`Self::new`] when encrypted values must survive restarts.
    ///
    /// Returns an error if secure random key generation fails.
    pub fn ephemeral() -> anyhow::Result<Self> {
        let key_id = "ephemeral";
        let random = SystemRandom::new();
        let mut key = [0; 32];
        random
            .fill(&mut key)
            .map_err(|_| anyhow::anyhow!("failed to generate encryption key"))?;
        Self::new(key_id, BTreeMap::from([(key_id.to_owned(), key.to_vec())]))
    }

    /// Validates and constructs a cipher from raw AES-256-GCM keys.
    ///
    /// Returns an error for invalid key IDs or sizes, or when the active key is absent.
    pub fn new(
        active_key_id: impl Into<String>,
        keys: BTreeMap<String, Vec<u8>>,
    ) -> anyhow::Result<Self> {
        let active_key_id = active_key_id.into();
        validate_key_id(&active_key_id)?;

        let mut validated = BTreeMap::new();
        for (key_id, key) in keys {
            validate_key_id(&key_id)?;
            anyhow::ensure!(
                key.len() == AES_256_GCM.key_len(),
                "encryption key {key_id:?} must contain exactly 32 bytes, got {}",
                key.len()
            );
            let key = UnboundKey::new(&AES_256_GCM, &key)
                .map(LessSafeKey::new)
                .map_err(|_| anyhow::anyhow!("invalid encryption key material"))?;
            validated.insert(key_id, key);
        }

        let active_key = validated.remove(&active_key_id).ok_or_else(|| {
            anyhow::anyhow!("active encryption key {active_key_id:?} is not configured")
        })?;

        Ok(Self {
            active_key_id,
            active_key,
            decryption_keys: validated,
            random: SystemRandom::new(),
        })
    }

    /// Encrypts a serializable value.
    ///
    /// Envelope format:
    /// ```text
    /// +------------++------------------------------+------------------------+---------------------------+--------------------+
    /// |            || Header                       | Nonce                  | Ciphertext                | Authentication tag |
    /// +------------++------------------------------+------------------------+---------------------------+--------------------+
    /// | Contains   || version + ID length + key ID | random                 | serde_json(value)         | AES-GCM verifier   |
    /// | Protection || public (authenticated)       | public (authenticated) | encrypted + authenticated | public; checked    |
    /// | Encoding   || 1 B + 1 B + UTF-8            | 12 B                   | variable                  | 16 B               |
    /// +------------++------------------------------+------------------------+---------------------------+--------------------+
    /// ```
    pub(crate) fn encrypt<T>(&self, value: &T) -> Result<Vec<u8>>
    where
        T: Serialize + ?Sized,
    {
        let key_id = self.active_key_id.as_bytes();
        let key_id_length = u8::try_from(key_id.len()).context(
            ErrorKind::Internal,
            "encryption key ID exceeds maximum length",
        )?;

        // Plaintext header.
        let mut header = Vec::with_capacity(2 + key_id.len());
        header.push(FORMAT_VERSION);
        header.push(key_id_length);
        header.extend_from_slice(key_id);

        let mut ciphertext = serde_json::to_vec(value)
            .context(ErrorKind::Internal, "failed to serialize encrypted value")?;

        let mut nonce = [0; NONCE_LENGTH];
        self.random
            .fill(&mut nonce)
            .map_err(|_| Error::new(ErrorKind::Internal, "failed to generate encryption nonce"))?;

        // Encrypt the serialized value in place and append the authentication tag.
        // The tag also authenticates the plaintext header.
        self.active_key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(&header),
                &mut ciphertext,
            )
            .map_err(|_| Error::new(ErrorKind::Internal, "failed to encrypt value"))?;

        let mut envelope = Vec::with_capacity(header.len() + nonce.len() + ciphertext.len());
        envelope.extend_from_slice(&header);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);
        Ok(envelope)
    }

    /// Decrypts and deserializes an encrypted value.
    pub(crate) fn decrypt<T>(&self, envelope: &[u8]) -> std::result::Result<T, CipherError>
    where
        T: DeserializeOwned,
    {
        let (&version, rest) = envelope
            .split_first()
            .ok_or(CipherError::MalformedEnvelope)?;
        if version != FORMAT_VERSION {
            return Err(CipherError::UnsupportedVersion(version));
        }

        // Parse the plaintext header and nonce.
        // They remain untrusted until AES-GCM verifies the authentication tag below.
        let (&key_id_length, rest) = rest.split_first().ok_or(CipherError::MalformedEnvelope)?;
        let key_id_length = usize::from(key_id_length);
        if key_id_length == 0 {
            return Err(CipherError::MalformedEnvelope);
        }
        let (key_id, rest) = rest
            .split_at_checked(key_id_length)
            .ok_or(CipherError::MalformedEnvelope)?;
        let (nonce, ciphertext) = rest
            .split_at_checked(NONCE_LENGTH)
            .ok_or(CipherError::MalformedEnvelope)?;

        // Use the plaintext key ID to select a key, without trusting the ID until authentication.
        let key_id = std::str::from_utf8(key_id).map_err(|_| CipherError::InvalidKeyId)?;
        let key = if key_id == self.active_key_id {
            &self.active_key
        } else {
            self.decryption_keys
                .get(key_id)
                .ok_or(CipherError::InvalidKeyId)?
        };
        let header_length = 2 + key_id_length;
        let header = &envelope[..header_length];
        let nonce: [u8; NONCE_LENGTH] = nonce.try_into().expect("nonce length was checked");
        if ciphertext.len() < TAG_LENGTH {
            return Err(CipherError::MalformedEnvelope);
        }

        // Verify the tag over the ciphertext and original header.
        let mut ciphertext = ciphertext.to_vec();
        let plaintext = key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(header),
                &mut ciphertext,
            )
            .map_err(|_| CipherError::Authentication)?;

        serde_json::from_slice(plaintext).map_err(CipherError::Deserialization)
    }
}

impl fmt::Debug for Cipher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut key_ids = self.decryption_keys.keys().collect::<Vec<_>>();
        key_ids.push(&self.active_key_id);
        key_ids.sort_unstable();

        f.debug_struct("Cipher")
            .field("active_key_id", &self.active_key_id)
            .field("key_ids", &key_ids)
            .finish_non_exhaustive()
    }
}

fn validate_key_id(key_id: &str) -> anyhow::Result<()> {
    let key_id_length = key_id.len();
    u8::try_from(key_id_length).map_err(|_| {
        anyhow::anyhow!(
            "encryption key ID must be at most {} bytes, got {key_id_length}",
            u8::MAX
        )
    })?;
    anyhow::ensure!(
        !key_id.is_empty()
            && key_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid encryption key ID {key_id:?}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher(active: &str, keys: &[(&str, u8)]) -> Cipher {
        Cipher::new(
            active,
            keys.iter()
                .map(|(key_id, byte)| (key_id.to_string(), vec![*byte; 32]))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn encryption_is_randomized_and_supports_multiple_types() {
        let cipher = cipher("v1", &[("v1", 7)]);
        let value = vec!["first".to_owned(), "second".to_owned()];

        let first = cipher.encrypt(&value).unwrap();
        let second = cipher.encrypt(&value).unwrap();

        assert_ne!(first, second);
        assert_eq!(first[0], FORMAT_VERSION);
        assert_eq!(cipher.decrypt::<Vec<String>>(&first).unwrap(), value);

        let number = cipher.encrypt(&42_u64).unwrap();
        assert_eq!(cipher.decrypt::<u64>(&number).unwrap(), 42);
    }

    #[test]
    fn encryption_rejects_tampering_plaintext_and_wrong_types() {
        let cipher = cipher("v1", &[("v1", 7)]);
        let token = cipher.encrypt("value").unwrap();

        let mut tampered = token.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(matches!(
            cipher.decrypt::<String>(&tampered),
            Err(CipherError::Authentication)
        ));

        let mut unsupported_version = token.clone();
        unsupported_version[0] = FORMAT_VERSION + 1;
        assert!(matches!(
            cipher.decrypt::<String>(&unsupported_version),
            Err(CipherError::UnsupportedVersion(_))
        ));
        assert!(cipher.decrypt::<String>(b"plaintext").is_err());
        assert!(matches!(
            cipher.decrypt::<u64>(&token),
            Err(CipherError::Deserialization(_))
        ));
    }

    #[test]
    fn rotation_decrypts_old_keys_and_removal_invalidates_them() {
        let old = cipher("v1", &[("v1", 1)]);
        let old_value = old.encrypt("old value").unwrap();

        let rotated = cipher("v2", &[("v1", 1), ("v2", 2)]);
        assert_eq!(rotated.decrypt::<String>(&old_value).unwrap(), "old value");
        let new_value = rotated.encrypt("new value").unwrap();
        assert_eq!(new_value[2..4], *b"v2");

        let removed = cipher("v2", &[("v2", 2)]);
        assert!(matches!(
            removed.decrypt::<String>(&old_value),
            Err(CipherError::InvalidKeyId)
        ));
    }

    #[test]
    fn configuration_validates_ids_lengths_and_active_key() {
        let error = Cipher::new("missing", BTreeMap::new()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "active encryption key \"missing\" is not configured"
        );

        let error =
            Cipher::new("bad key", BTreeMap::from([("bad key".into(), vec![0; 32])])).unwrap_err();
        assert_eq!(error.to_string(), "invalid encryption key ID \"bad key\"");

        let error = Cipher::new("v1", BTreeMap::from([("v1".into(), vec![0; 31])])).unwrap_err();
        assert_eq!(
            error.to_string(),
            "encryption key \"v1\" must contain exactly 32 bytes, got 31"
        );
    }
}
