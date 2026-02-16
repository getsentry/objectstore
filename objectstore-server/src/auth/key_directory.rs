use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use anyhow::Context;
use jsonwebtoken::DecodingKey;
use objectstore_types::auth::Permission;

use crate::config::{AuthZ, AuthZVerificationKey};

fn read_key_from_file(filename: &Path) -> anyhow::Result<DecodingKey> {
    let key_content = std::fs::read_to_string(filename).context("reading key")?;
    DecodingKey::from_ed_pem(key_content.as_bytes()).context("parsing key")
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

    /// The maximum set of permissions that this key's signer is authorized to grant.
    ///
    /// If a request's `Authorization` header grants full permission but it was signed by
    /// a key that is only allowed to grant read permission, then the request only has
    /// read permission.
    pub max_permissions: HashSet<Permission>,
}

impl TryFrom<&AuthZVerificationKey> for PublicKeyConfig {
    type Error = anyhow::Error;

    fn try_from(key_config: &AuthZVerificationKey) -> Result<Self, anyhow::Error> {
        Ok(Self {
            max_permissions: key_config.max_permissions.clone(),
            key_versions: key_config
                .key_files
                .iter()
                .map(|filename| read_key_from_file(filename))
                .collect::<anyhow::Result<Vec<DecodingKey>>>()?,
        })
    }
}

/// Directory of keys that may be used to verify a request's `Authorization` header.
///
/// This directory contains a map that is keyed on a key's ID. When verifying a JWT
/// from the `Authorization` header, the `kid` field should be read from the JWT
/// header and used to index into this directory to select the appropriate key.
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
