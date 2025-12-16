use std::collections::{BTreeMap, HashSet};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use objectstore_types::Permission;
use serde::{Deserialize, Serialize};

use crate::ScopeInner;

const DEFAULT_EXPIRY_SECONDS: u64 = 60;
const DEFAULT_PERMISSIONS: [Permission; 3] = [
    Permission::ObjectRead,
    Permission::ObjectWrite,
    Permission::ObjectDelete,
];

/// Key configuration that will be used to sign tokens in Objectstore requests.
pub struct SecretKey {
    /// A key ID that Objectstore must use to load the corresponding public key.
    pub kid: String,

    /// An EdDSA private key.
    pub secret_key: String,
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretKey")
            .field("kid", &self.kid)
            .field("secret_key", &"[redacted]")
            .finish()
    }
}

/// A utility to generate auth tokens to be used in Objectstore requests.
///
/// Tokens are signed with an EdDSA private key and have certain permissions and expiry timeouts
/// applied.
#[derive(Debug)]
pub struct TokenGenerator {
    kid: String,
    encoding_key: EncodingKey,
    expiry_seconds: u64,
    permissions: HashSet<Permission>,
}

#[derive(Serialize, Deserialize)]
struct JwtRes {
    #[serde(rename = "os:usecase")]
    usecase: String,

    #[serde(flatten)]
    scopes: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct JwtClaims {
    exp: u64,
    permissions: HashSet<Permission>,
    res: JwtRes,
}

impl TokenGenerator {
    /// Create a new [`TokenGenerator`] for a given key configuration.
    pub fn new(secret_key: SecretKey) -> crate::Result<TokenGenerator> {
        let encoding_key = EncodingKey::from_ed_pem(secret_key.secret_key.as_bytes())?;
        Ok(TokenGenerator {
            kid: secret_key.kid,
            encoding_key,
            expiry_seconds: DEFAULT_EXPIRY_SECONDS,
            permissions: HashSet::from(DEFAULT_PERMISSIONS),
        })
    }

    /// Set the expiry duration for tokens signed by this generator.
    pub fn expiry_seconds(mut self, expiry_seconds: u64) -> Self {
        self.expiry_seconds = expiry_seconds;
        self
    }

    /// Set the permissions that will be granted to tokens signed by this generator.
    pub fn permissions(mut self, permissions: &[Permission]) -> Self {
        self.permissions = HashSet::from_iter(permissions.iter().cloned());
        self
    }

    /// Sign a new token for the passed-in scope using the configured expiry and permissions.
    pub(crate) fn sign_for_scope(&self, scope: &ScopeInner) -> crate::Result<String> {
        let claims = JwtClaims {
            exp: get_current_timestamp() + self.expiry_seconds,
            permissions: self.permissions.clone(),
            res: JwtRes {
                usecase: scope.usecase().name().into(),
                scopes: scope
                    .scopes()
                    .iter()
                    .map(|scope| (scope.name().to_string(), scope.value().to_string()))
                    .collect(),
            },
        };

        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.kid.clone());

        Ok(encode(&header, &claims, &self.encoding_key)?)
    }
}
