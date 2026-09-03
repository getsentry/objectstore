//! Shared types for Objectstore's resumable upload protocol.

use std::collections::BTreeMap;
use std::fmt;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};

use crate::error::{Error, ErrorKind, Result, ResultExt as _};
use crate::id::ObjectId;

pub use objectstore_types::resumable::{SessionToken, UploadOffset, UploadProgress};

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
    active_key_id: String,
    active_key: TokenAead,
    decryption_keys: BTreeMap<String, TokenAead>,
    random: SystemRandom,
}

struct TokenAead(LessSafeKey);

struct EncryptedToken {
    nonce: [u8; TOKEN_NONCE_LENGTH],
    ciphertext: Vec<u8>,
}

#[derive(Deserialize, Serialize)]
struct SessionTokenEnvelope {
    storage_path: String,
    backend_token: String,
}

impl TokenAead {
    fn new(key: &[u8]) -> anyhow::Result<Self> {
        let key = UnboundKey::new(&AES_256_GCM, key)
            .map(LessSafeKey::new)
            .map_err(|_| anyhow::anyhow!("invalid resumable upload encryption key material"))?;
        Ok(Self(key))
    }

    fn encrypt(
        &self,
        random: &SystemRandom,
        aad: &[u8],
        mut plaintext: Vec<u8>,
    ) -> Result<EncryptedToken> {
        let mut nonce = [0; TOKEN_NONCE_LENGTH];
        random.fill(&mut nonce).map_err(|_| {
            Error::new(
                ErrorKind::Internal,
                "failed to generate resumable token nonce",
            )
        })?;
        self.0
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                &mut plaintext,
            )
            .map_err(|_| Error::new(ErrorKind::Internal, "failed to encrypt resumable token"))?;
        Ok(EncryptedToken {
            nonce,
            ciphertext: plaintext,
        })
    }

    fn decrypt(
        &self,
        nonce: [u8; TOKEN_NONCE_LENGTH],
        aad: &[u8],
        mut ciphertext: Vec<u8>,
    ) -> Option<Vec<u8>> {
        if ciphertext.len() < TOKEN_TAG_LENGTH {
            return None;
        }
        let plaintext = self
            .0
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                &mut ciphertext,
            )
            .ok()?;
        Some(plaintext.to_vec())
    }
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
            let key = TokenAead::new(&key)?;
            validated.insert(key_id, key);
        }

        let active_key = validated.remove(&active_key_id).ok_or_else(|| {
            anyhow::anyhow!(
                "active resumable upload encryption key {active_key_id:?} is not configured"
            )
        })?;

        Ok(Self {
            active_key_id,
            active_key,
            decryption_keys: validated,
            random: SystemRandom::new(),
        })
    }

    /// Encrypts a backend token and binds it to the canonical object identity.
    pub(crate) fn encrypt(&self, id: &ObjectId, backend_token: String) -> Result<SessionToken> {
        let key_id = self.active_key_id.as_bytes();

        let mut header = Vec::with_capacity(1 + key_id.len());
        header.push(key_id.len() as u8);
        header.extend_from_slice(key_id);

        let plaintext = serde_json::to_vec(&SessionTokenEnvelope {
            storage_path: id.as_storage_path().to_string(),
            backend_token,
        })
        .context(
            ErrorKind::Internal,
            "failed to serialize resumable session token",
        )?;
        let encrypted = self.active_key.encrypt(&self.random, &header, plaintext)?;

        let mut envelope =
            Vec::with_capacity(header.len() + encrypted.nonce.len() + encrypted.ciphertext.len());
        envelope.extend_from_slice(&header);
        envelope.extend_from_slice(&encrypted.nonce);
        envelope.extend_from_slice(&encrypted.ciphertext);
        Ok(SessionToken::new(envelope))
    }

    /// Decrypts an external token, returning one uniform error for every invalid envelope.
    pub(crate) fn decrypt(&self, id: &ObjectId, token: SessionToken) -> Result<String> {
        self.decrypt_inner(id, token)
            .ok_or_else(|| ErrorKind::UnknownUploadSession.into())
    }

    fn decrypt_inner(&self, id: &ObjectId, token: SessionToken) -> Option<String> {
        let envelope = token.into_bytes();
        let (&key_id_length, rest) = envelope.split_first()?;
        let key_id_length = usize::from(key_id_length);
        if key_id_length == 0 {
            return None;
        }
        let (key_id, rest) = rest.split_at_checked(key_id_length)?;
        let (nonce, ciphertext) = rest.split_at_checked(TOKEN_NONCE_LENGTH)?;

        let key_id = std::str::from_utf8(key_id).ok()?;
        let key = if key_id == self.active_key_id {
            &self.active_key
        } else {
            self.decryption_keys.get(key_id)?
        };
        let header_length = 1 + key_id_length;
        let header = &envelope[..header_length];
        let nonce: [u8; TOKEN_NONCE_LENGTH] = nonce.try_into().ok()?;
        let plaintext = key.decrypt(nonce, header, ciphertext.to_vec())?;
        let envelope: SessionTokenEnvelope = serde_json::from_slice(&plaintext).ok()?;
        if envelope.storage_path != id.as_storage_path().to_string() {
            return None;
        }
        Some(envelope.backend_token)
    }
}

impl fmt::Debug for ResumableUploadEncryption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut key_ids = self.decryption_keys.keys().collect::<Vec<_>>();
        key_ids.push(&self.active_key_id);
        key_ids.sort_unstable();

        f.debug_struct("ResumableUploadEncryption")
            .field("active_key_id", &self.active_key_id)
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
    fn encryption_is_randomized_and_round_trips_backend_tokens() {
        let encryption = encryption("v1", &[("v1", 7)]);
        let id = id("object");
        let backend_token = "backend token".to_owned();

        let first = encryption.encrypt(&id, backend_token.clone()).unwrap();
        let second = encryption.encrypt(&id, backend_token.clone()).unwrap();
        assert_ne!(first, second);
        assert_eq!(encryption.decrypt(&id, first).unwrap(), backend_token);
        assert_eq!(encryption.decrypt(&id, second).unwrap(), backend_token);
    }

    #[test]
    fn encryption_rejects_tampering_wrong_objects_and_plaintext() {
        let encryption = encryption("v1", &[("v1", 7)]);
        let object = id("object");
        let token = encryption.encrypt(&object, "backend token".into()).unwrap();

        let mut tampered = token.clone().into_bytes();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(matches!(
            encryption.decrypt(&object, SessionToken::new(tampered)),
            Err(error) if error.kind() == ErrorKind::UnknownUploadSession
        ));
        assert!(matches!(
            encryption.decrypt(&id("other"), token),
            Err(error) if error.kind() == ErrorKind::UnknownUploadSession
        ));
        assert!(matches!(
            encryption.decrypt(&object, SessionToken::new(b"backend token")),
            Err(error) if error.kind() == ErrorKind::UnknownUploadSession
        ));
    }

    #[test]
    fn rotation_decrypts_old_keys_and_removal_invalidates_them() {
        let object = id("object");
        let old = encryption("v1", &[("v1", 1)]);
        let old_token = old.encrypt(&object, "backend token".into()).unwrap();

        let rotated = encryption("v2", &[("v1", 1), ("v2", 2)]);
        assert_eq!(
            rotated.decrypt(&object, old_token.clone()).unwrap(),
            "backend token"
        );
        let new_token = rotated.encrypt(&object, "new token".into()).unwrap();
        assert_eq!(new_token.as_bytes()[1..3], *b"v2");

        let removed = encryption("v2", &[("v2", 2)]);
        assert!(matches!(
            removed.decrypt(&object, old_token),
            Err(error) if error.kind() == ErrorKind::UnknownUploadSession
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
