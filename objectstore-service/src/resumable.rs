//! Utilities for Resumable Uploads.

use std::collections::BTreeMap;
use std::fmt;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::error::{Error, ErrorKind, Result, ResultExt as _};
use crate::id::ObjectId;

pub use objectstore_types::resumable::{
    SessionToken as EncryptedSessionToken, UploadOffset, UploadProgress,
};

/// AES-GCM nonce length in bytes.
const NONCE_LENGTH: usize = 12;
/// AES-GCM authentication tag length in bytes.
const TAG_LENGTH: usize = 16;

/// Opaque session state encoded and decoded by a storage backend.
pub type BackendToken = String;

/// Structured token encrypted at the service boundary.
#[derive(Deserialize, Serialize)]
pub(crate) struct SessionToken {
    #[serde(
        serialize_with = "serialize_object_id",
        deserialize_with = "deserialize_object_id"
    )]
    pub(crate) object_id: ObjectId,
    pub(crate) backend_token: BackendToken,
}

fn serialize_object_id<S>(id: &ObjectId, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_str(&id.as_storage_path())
}

fn deserialize_object_id<'de, D>(deserializer: D) -> std::result::Result<ObjectId, D::Error>
where
    D: Deserializer<'de>,
{
    let path = String::deserialize(deserializer)?;
    ObjectId::from_storage_path(&path)
        .ok_or_else(|| de::Error::custom("invalid object storage path"))
}

/// Encrypts and decrypts Resumable Upload session tokens.
///
/// The active key encrypts new sessions, while the key ID embedded in an existing token selects
/// its decryption key.
pub struct Encryptor {
    active_key_id: String,
    active_key: LessSafeKey,
    decryption_keys: BTreeMap<String, LessSafeKey>,
    random: SystemRandom,
}

impl Encryptor {
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
            .map_err(|_| anyhow::anyhow!("failed to generate resumable token encryption key"))?;
        Self::new(key_id, BTreeMap::from([(key_id.to_owned(), key.to_vec())]))
    }

    /// Validates and constructs an encryptor from raw AES-256 keys.
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
                "resumable token encryption key {key_id:?} must contain exactly 32 bytes, got {}",
                key.len()
            );
            let key = UnboundKey::new(&AES_256_GCM, &key)
                .map(LessSafeKey::new)
                .map_err(|_| anyhow::anyhow!("invalid resumable token encryption key material"))?;
            validated.insert(key_id, key);
        }

        let active_key = validated.remove(&active_key_id).ok_or_else(|| {
            anyhow::anyhow!(
                "active resumable token encryption key {active_key_id:?} is not configured"
            )
        })?;

        Ok(Self {
            active_key_id,
            active_key,
            decryption_keys: validated,
            random: SystemRandom::new(),
        })
    }

    /// Encrypts a structured Resumable Upload session token.
    pub(crate) fn encrypt(&self, session: SessionToken) -> Result<EncryptedSessionToken> {
        let key_id = self.active_key_id.as_bytes();

        let mut header = Vec::with_capacity(1 + key_id.len());
        header.push(key_id.len() as u8);
        header.extend_from_slice(key_id);

        let mut ciphertext = serde_json::to_vec(&session).context(
            ErrorKind::Internal,
            "failed to serialize resumable session token",
        )?;

        let mut nonce = [0; NONCE_LENGTH];
        self.random.fill(&mut nonce).map_err(|_| {
            Error::new(
                ErrorKind::Internal,
                "failed to generate resumable token nonce",
            )
        })?;
        self.active_key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(&header),
                &mut ciphertext,
            )
            .map_err(|_| Error::new(ErrorKind::Internal, "failed to encrypt resumable token"))?;

        let mut envelope = Vec::with_capacity(header.len() + nonce.len() + ciphertext.len());
        envelope.extend_from_slice(&header);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(&ciphertext);
        Ok(EncryptedSessionToken::new(envelope))
    }

    /// Decrypts a Resumable Upload session token.
    pub(crate) fn decrypt(&self, token: EncryptedSessionToken) -> Result<SessionToken> {
        self.decrypt_inner(token)
            .ok_or_else(|| ErrorKind::UnknownUploadSession.into())
    }

    fn decrypt_inner(&self, token: EncryptedSessionToken) -> Option<SessionToken> {
        let envelope = token.into_bytes();
        let (&key_id_length, rest) = envelope.split_first()?;
        let key_id_length = usize::from(key_id_length);
        if key_id_length == 0 {
            return None;
        }
        let (key_id, rest) = rest.split_at_checked(key_id_length)?;
        let (nonce, ciphertext) = rest.split_at_checked(NONCE_LENGTH)?;

        let key_id = std::str::from_utf8(key_id).ok()?;
        let key = if key_id == self.active_key_id {
            &self.active_key
        } else {
            self.decryption_keys.get(key_id)?
        };
        let header_length = 1 + key_id_length;
        let header = &envelope[..header_length];
        let nonce: [u8; NONCE_LENGTH] = nonce.try_into().ok()?;
        if ciphertext.len() < TAG_LENGTH {
            return None;
        }
        let mut ciphertext = ciphertext.to_vec();
        let plaintext = key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(header),
                &mut ciphertext,
            )
            .ok()?;
        serde_json::from_slice(plaintext).ok()
    }
}

impl fmt::Debug for Encryptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut key_ids = self.decryption_keys.keys().collect::<Vec<_>>();
        key_ids.push(&self.active_key_id);
        key_ids.sort_unstable();

        f.debug_struct("Encryptor")
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
        "invalid resumable token encryption key ID {key_id:?}"
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

    fn encryption(active: &str, keys: &[(&str, u8)]) -> Encryptor {
        Encryptor::new(
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

        let first = encryption
            .encrypt(SessionToken {
                object_id: id.clone(),
                backend_token: backend_token.clone(),
            })
            .unwrap();
        let second = encryption
            .encrypt(SessionToken {
                object_id: id.clone(),
                backend_token: backend_token.clone(),
            })
            .unwrap();
        assert_ne!(first, second);
        let first = encryption.decrypt(first).unwrap();
        let second = encryption.decrypt(second).unwrap();
        assert_eq!(first.object_id, id);
        assert_eq!(first.backend_token, backend_token);
        assert_eq!(second.object_id, id);
        assert_eq!(second.backend_token, backend_token);
    }

    #[test]
    fn encryption_rejects_tampering_and_plaintext() {
        let encryption = encryption("v1", &[("v1", 7)]);
        let object = id("object");
        let token = encryption
            .encrypt(SessionToken {
                object_id: object,
                backend_token: "backend token".to_owned(),
            })
            .unwrap();

        let mut tampered = token.clone().into_bytes();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(matches!(
            encryption.decrypt(EncryptedSessionToken::new(tampered)),
            Err(error) if error.kind() == ErrorKind::UnknownUploadSession
        ));
        assert!(matches!(
            encryption.decrypt(EncryptedSessionToken::new(b"backend token")),
            Err(error) if error.kind() == ErrorKind::UnknownUploadSession
        ));
    }

    #[test]
    fn rotation_decrypts_old_keys_and_removal_invalidates_them() {
        let object = id("object");
        let old = encryption("v1", &[("v1", 1)]);
        let old_token = old
            .encrypt(SessionToken {
                object_id: object.clone(),
                backend_token: "backend token".to_owned(),
            })
            .unwrap();

        let rotated = encryption("v2", &[("v1", 1), ("v2", 2)]);
        assert_eq!(
            rotated.decrypt(old_token.clone()).unwrap().backend_token,
            "backend token"
        );
        let new_token = rotated
            .encrypt(SessionToken {
                object_id: object,
                backend_token: "new token".to_owned(),
            })
            .unwrap();
        assert_eq!(new_token.as_bytes()[1..3], *b"v2");

        let removed = encryption("v2", &[("v2", 2)]);
        assert!(matches!(
            removed.decrypt(old_token),
            Err(error) if error.kind() == ErrorKind::UnknownUploadSession
        ));
    }

    #[test]
    fn configuration_validates_ids_lengths_and_active_key() {
        let error = Encryptor::new("missing", BTreeMap::new()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "active resumable token encryption key \"missing\" is not configured"
        );

        let error = Encryptor::new("bad key", BTreeMap::from([("bad key".into(), vec![0; 32])]))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid resumable token encryption key ID \"bad key\""
        );

        let error = Encryptor::new("v1", BTreeMap::from([("v1".into(), vec![0; 31])])).unwrap_err();
        assert_eq!(
            error.to_string(),
            "resumable token encryption key \"v1\" must contain exactly 32 bytes, got 31"
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
