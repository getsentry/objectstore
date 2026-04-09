use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use anyhow::Context;
use ed25519_dalek::pkcs8::DecodePublicKey;
use jsonwebtoken::DecodingKey;
use objectstore_types::auth::Permission;

use crate::config::{AuthZ, AuthZVerificationKey};

fn read_key_from_file(
    filename: &Path,
) -> anyhow::Result<(DecodingKey, ed25519_dalek::VerifyingKey)> {
    let key_content = std::fs::read_to_string(filename).context("reading key")?;
    let decoding_key = DecodingKey::from_ed_pem(key_content.as_bytes()).context("parsing key")?;
    let verifying_key = ed25519_dalek::VerifyingKey::from_public_key_pem(&key_content)
        .context("parsing Ed25519 public key")?;
    Ok((decoding_key, verifying_key))
}

/// Configures the EdDSA public key(s) and permissions used to verify tokens from a single `kid`.
///
/// Note: [`jsonwebtoken::DecodingKey`] redacts key content in its `Debug` implementation.
#[derive(Debug)]
pub struct PublicKeyConfig {
    /// Versions of this key's key material which may be used to verify signatures.
    ///
    /// If a key is being rotated, the old and new versions of that key should both be
    /// configured so objectstore can verify signatures while the updated key is still
    /// rolling out. Otherwise, this should only contain the most recent version of a key.
    pub key_versions: Vec<DecodingKey>,

    /// Ed25519 verifying keys for pre-signed URL signature verification.
    ///
    /// These are parsed from the same PEM files as `key_versions`, but expose the raw
    /// Ed25519 public key for use outside of JWT verification.
    pub verifying_keys: Vec<ed25519_dalek::VerifyingKey>,

    /// The maximum set of permissions that this key's signer is authorized to grant.
    ///
    /// If a request's auth token grants full permission but it was signed by a key that
    /// is only allowed to grant read permission, then the request only has read
    /// permission.
    pub max_permissions: HashSet<Permission>,
}

impl TryFrom<&AuthZVerificationKey> for PublicKeyConfig {
    type Error = anyhow::Error;

    fn try_from(key_config: &AuthZVerificationKey) -> Result<Self, anyhow::Error> {
        let (key_versions, verifying_keys) = key_config
            .key_files
            .iter()
            .map(|filename| read_key_from_file(filename))
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        Ok(Self {
            max_permissions: key_config.max_permissions.clone(),
            key_versions,
            verifying_keys,
        })
    }
}

/// Directory of keys that may be used to verify a request's auth token.
///
/// The auth token is read from the `X-Os-Auth` header (preferred) or the
/// standard `Authorization` header (fallback). This directory contains a map keyed
/// on a key's ID. When verifying a JWT, the `kid` field should be read from the
/// JWT header and used to index into this directory to select the appropriate key.
#[derive(Debug)]
pub struct PublicKeyDirectory {
    /// Mapping from key ID to key configuration.
    pub keys: BTreeMap<String, PublicKeyConfig>,
}

impl TryFrom<&AuthZ> for PublicKeyDirectory {
    type Error = anyhow::Error;

    fn try_from(auth_config: &AuthZ) -> Result<Self, Self::Error> {
        Ok(Self {
            keys: auth_config
                .keys
                .iter()
                .map(|(kid, key)| Ok((kid.clone(), key.try_into()?)))
                .collect::<Result<BTreeMap<String, PublicKeyConfig>, anyhow::Error>>()?,
        })
    }
}
