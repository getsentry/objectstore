//! Shared types for Objectstore's resumable upload protocol.

use std::collections::BTreeMap;
use std::fmt;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};

use crate::error::{Error, Result};
use crate::id::ObjectId;

pub use objectstore_types::resumable::{SessionToken, UploadOffset, UploadProgress};

/// Version of the encrypted session-token envelope.
const TOKEN_ENVELOPE_VERSION: u8 = 1;
/// AES-GCM nonce length in bytes.
const TOKEN_NONCE_LENGTH: usize = 12;
/// AES-GCM authentication tag length in bytes.
const TOKEN_TAG_LENGTH: usize = 16;

/// Validated encryption keys for resumable-upload session tokens.
///
/// This type intentionally redacts key material from its debug representation. The active key is
/// used for new sessions, while the key ID embedded in an existing envelope selects the key used
/// to decrypt it.
pub struct ResumableUploadEncryption {
    active_key: EncryptionKey,
    decryption_keys: BTreeMap<String, LessSafeKey>,
    random: SystemRandom,
}

struct EncryptionKey {
    id: String,
    key: LessSafeKey,
}

impl ResumableUploadEncryption {
    /// Constructs an encryptor with a fresh process-local AES-256 key.
    ///
    /// Tokens encrypted with this key cannot be decrypted after a service restart. Configure a
    /// persistent keyring with [`Self::new`] when resumable sessions must survive restarts.
    ///
    /// Returns an error if secure random key generation fails.
    pub fn ephemeral() -> anyhow::Result<Self> {
        let key_id = "ephemeral";
        let random = SystemRandom::new();
        let mut key = [0; 32];
        random
            .fill(&mut key)
            .map_err(|_| anyhow::anyhow!("failed to generate resumable upload encryption key"))?;
        Self::new(key_id, BTreeMap::from([(key_id.to_owned(), key.to_vec())]))
    }

    /// Validates and constructs a token encryptor from raw AES-256 keys.
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
                "resumable upload encryption key {key_id:?} must contain exactly 32 bytes, got {}",
                key.len()
            );
            let key = UnboundKey::new(&AES_256_GCM, &key)
                .map(LessSafeKey::new)
                .map_err(|_| anyhow::anyhow!("invalid resumable upload encryption key material"))?;
            validated.insert(key_id, key);
        }

        let active_key = validated.remove(&active_key_id).ok_or_else(|| {
            anyhow::anyhow!(
                "active resumable upload encryption key {active_key_id:?} is not configured"
            )
        })?;

        Ok(Self {
            active_key: EncryptionKey {
                id: active_key_id,
                key: active_key,
            },
            decryption_keys: validated,
            random: SystemRandom::new(),
        })
    }

    /// Encrypts a backend token and binds it to the canonical object identity.
    pub(crate) fn encrypt(&self, id: &ObjectId, token: SessionToken) -> Result<SessionToken> {
        let key_id = self.active_key.id.as_bytes();

        let mut nonce_bytes = [0; TOKEN_NONCE_LENGTH];
        self.random
            .fill(&mut nonce_bytes)
            .map_err(|_| Error::generic("failed to generate resumable token nonce"))?;

        let mut header = Vec::with_capacity(2 + key_id.len());
        header.push(TOKEN_ENVELOPE_VERSION);
        header.push(key_id.len() as u8);
        header.extend_from_slice(key_id);

        let mut ciphertext = token.into_bytes();
        self.active_key
            .key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(aad(&header, id)),
                &mut ciphertext,
            )
            .map_err(|_| Error::generic("failed to encrypt resumable token"))?;

        let mut envelope = Vec::with_capacity(header.len() + nonce_bytes.len() + ciphertext.len());
        envelope.extend_from_slice(&header);
        envelope.extend_from_slice(&nonce_bytes);
        envelope.extend_from_slice(&ciphertext);
        Ok(SessionToken::new(envelope))
    }

    /// Decrypts an external token, returning one uniform error for every invalid envelope.
    pub(crate) fn decrypt(&self, id: &ObjectId, token: SessionToken) -> Result<SessionToken> {
        self.decrypt_inner(id, token)
            .ok_or(Error::UnknownUploadSession)
    }

    fn decrypt_inner(&self, id: &ObjectId, token: SessionToken) -> Option<SessionToken> {
        let envelope = token.into_bytes();
        let (&version, rest) = envelope.split_first()?;
        if version != TOKEN_ENVELOPE_VERSION {
            return None;
        }
        let (&key_id_length, rest) = rest.split_first()?;
        let key_id_length = usize::from(key_id_length);
        if key_id_length == 0 {
            return None;
        }
        let (key_id, rest) = rest.split_at_checked(key_id_length)?;
        let (nonce, ciphertext) = rest.split_at_checked(TOKEN_NONCE_LENGTH)?;
        if ciphertext.len() < TOKEN_TAG_LENGTH {
            return None;
        }

        let key_id = std::str::from_utf8(key_id).ok()?;
        let key = if key_id == self.active_key.id {
            &self.active_key.key
        } else {
            self.decryption_keys.get(key_id)?
        };
        let header_length = 2 + key_id_length;
        let header = &envelope[..header_length];
        let nonce: [u8; TOKEN_NONCE_LENGTH] = nonce.try_into().ok()?;
        let mut ciphertext = ciphertext.to_vec();
        let plaintext = key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad(header, id)),
                &mut ciphertext,
            )
            .ok()?;
        Some(SessionToken::new(plaintext.to_vec()))
    }
}

impl fmt::Debug for ResumableUploadEncryption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut key_ids = self.decryption_keys.keys().collect::<Vec<_>>();
        key_ids.push(&self.active_key.id);
        key_ids.sort_unstable();

        f.debug_struct("ResumableUploadEncryption")
            .field("active_key_id", &self.active_key.id)
            .field("key_ids", &key_ids)
            .field("keys", &"[redacted]")
            .finish()
    }
}

fn validate_key_id(key_id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !key_id.is_empty()
            && key_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid resumable upload encryption key ID {key_id:?}"
    );
    Ok(())
}

fn aad(header: &[u8], id: &ObjectId) -> Vec<u8> {
    let object_name = id.as_storage_path().to_string();
    let mut aad = Vec::with_capacity(header.len() + object_name.len());
    aad.extend_from_slice(header);
    aad.extend_from_slice(object_name.as_bytes());
    aad
}

#[cfg(test)]
mod tests {
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::id::ObjectContext;

    fn id(key: &str) -> ObjectId {
        ObjectId::new(
            ObjectContext {
                usecase: "testing".into(),
                scopes: Scopes::from_iter([Scope::create("org", "42").unwrap()]),
            },
            key.into(),
        )
    }

    fn encryption(active: &str, keys: &[(&str, u8)]) -> ResumableUploadEncryption {
        ResumableUploadEncryption::new(
            active,
            keys.iter()
                .map(|(key_id, byte)| (key_id.to_string(), vec![*byte; 32]))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn encryption_is_randomized_and_round_trips_arbitrary_bytes() {
        let encryption = encryption("v1", &[("v1", 7)]);
        let id = id("object");
        let plaintext = SessionToken::new([0, 0xff, b'?', b'/', 0]);

        let first = encryption.encrypt(&id, plaintext.clone()).unwrap();
        let second = encryption.encrypt(&id, plaintext.clone()).unwrap();
        assert_ne!(first, second);
        assert_eq!(encryption.decrypt(&id, first).unwrap(), plaintext);
        assert_eq!(encryption.decrypt(&id, second).unwrap(), plaintext);
    }

    #[test]
    fn encryption_rejects_tampering_wrong_objects_and_plaintext() {
        let encryption = encryption("v1", &[("v1", 7)]);
        let object = id("object");
        let token = encryption
            .encrypt(&object, SessionToken::new(b"backend token"))
            .unwrap();

        let mut tampered = token.clone().into_bytes();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(matches!(
            encryption.decrypt(&object, SessionToken::new(tampered)),
            Err(Error::UnknownUploadSession)
        ));
        assert!(matches!(
            encryption.decrypt(&id("other"), token),
            Err(Error::UnknownUploadSession)
        ));
        assert!(matches!(
            encryption.decrypt(&object, SessionToken::new(b"backend token")),
            Err(Error::UnknownUploadSession)
        ));
    }

    #[test]
    fn rotation_decrypts_old_keys_and_removal_invalidates_them() {
        let object = id("object");
        let old = encryption("v1", &[("v1", 1)]);
        let old_token = old
            .encrypt(&object, SessionToken::new(b"backend token"))
            .unwrap();

        let rotated = encryption("v2", &[("v1", 1), ("v2", 2)]);
        assert_eq!(
            rotated
                .decrypt(&object, old_token.clone())
                .unwrap()
                .as_bytes(),
            b"backend token"
        );
        let new_token = rotated
            .encrypt(&object, SessionToken::new(b"new token"))
            .unwrap();
        assert_eq!(new_token.as_bytes()[2..4], *b"v2");

        let removed = encryption("v2", &[("v2", 2)]);
        assert!(matches!(
            removed.decrypt(&object, old_token),
            Err(Error::UnknownUploadSession)
        ));
    }

    #[test]
    fn configuration_validates_ids_lengths_and_active_key() {
        let error = ResumableUploadEncryption::new("missing", BTreeMap::new()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "active resumable upload encryption key \"missing\" is not configured"
        );

        let error = ResumableUploadEncryption::new(
            "bad key",
            BTreeMap::from([("bad key".into(), vec![0; 32])]),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid resumable upload encryption key ID \"bad key\""
        );

        let error =
            ResumableUploadEncryption::new("v1", BTreeMap::from([("v1".into(), vec![0; 31])]))
                .unwrap_err();
        assert_eq!(
            error.to_string(),
            "resumable upload encryption key \"v1\" must contain exactly 32 bytes, got 31"
        );
    }

    #[test]
    fn debug_output_redacts_keys() {
        let encryption = encryption("v1", &[("v1", 7)]);
        let debug = format!("{encryption:?}");
        assert!(debug.contains("v1"));
        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("7, 7"));
    }
}
