use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use anyhow::Context;
use jsonwebtoken::DecodingKey;
use objectstore_types::auth::Permission;

use crate::config::{AuthZ, AuthZVerificationKey};

async fn read_key_from_file(filename: &Path) -> anyhow::Result<DecodingKey> {
    let key_content = tokio::fs::read_to_string(filename)
        .await
        .with_context(|| format!("reading key from {filename:?}"))?;
    DecodingKey::from_ed_pem(key_content.as_bytes())
        .with_context(|| format!("parsing key from {filename:?}"))
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
    /// If a request's auth token grants full permission but it was signed by a key that
    /// is only allowed to grant read permission, then the request only has read
    /// permission.
    pub max_permissions: HashSet<Permission>,
}

impl PublicKeyConfig {
    /// Loads key material and permissions from an [`AuthZVerificationKey`] configuration.
    pub async fn from_config(key_config: &AuthZVerificationKey) -> anyhow::Result<Self> {
        let mut key_versions = Vec::with_capacity(key_config.key_files.len());
        for filename in &key_config.key_files {
            let key = read_key_from_file(filename)
                .await
                .inspect_err(|e| objectstore_log::error!("{:?}", e))?;
            key_versions.push(key);
        }

        Ok(Self {
            max_permissions: key_config.max_permissions.clone(),
            key_versions,
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

impl PublicKeyDirectory {
    /// Loads the full key directory from an [`AuthZ`] configuration.
    pub async fn from_config(auth_config: &AuthZ) -> anyhow::Result<Self> {
        let mut keys = BTreeMap::new();
        for (kid, key_config) in &auth_config.keys {
            let config = PublicKeyConfig::from_config(key_config).await?;
            keys.insert(kid.clone(), config);
        }

        Ok(Self { keys })
    }
}
