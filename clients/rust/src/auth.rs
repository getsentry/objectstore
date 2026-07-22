use std::collections::{BTreeMap, HashSet};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use objectstore_types::scope;
use serde::{Deserialize, Serialize};

use crate::ScopeInner;

pub use objectstore_types::auth::Permission;

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

/// Authentication provider for Objectstore requests.
///
/// Can be either a [`TokenGenerator`] that signs a fresh JWT per request,
/// or a static pre-signed JWT string.
pub enum TokenProvider {
    /// A pre-signed JWT token string, used as-is for every request.
    Static(String),
    /// A generator that signs a fresh JWT for each request using an EdDSA keypair.
    Generator(TokenGenerator),
}

impl std::fmt::Debug for TokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenProvider::Static(_) => f.write_str("TokenProvider::Static([redacted])"),
            TokenProvider::Generator(g) => {
                f.debug_tuple("TokenProvider::Generator").field(g).finish()
            }
        }
    }
}

/// Conversion into an optional [`TokenProvider`] for [`ClientBuilder::token`].
///
/// This is implemented for [`TokenGenerator`], `String`, and `&str`, each of which yields a
/// configured provider. It is also implemented for any `Option<T>` where `T: IntoTokenProvider`,
/// so a `None` resolves to no authentication and a `Some(value)` to the inner provider. This lets
/// callers pass optional auth configuration to [`ClientBuilder::token`] without an explicit
/// conditional.
///
/// [`ClientBuilder::token`]: crate::ClientBuilder::token
pub trait IntoTokenProvider {
    /// Converts `self` into an optional [`TokenProvider`].
    fn into_token_provider(self) -> Option<TokenProvider>;
}

impl<T> IntoTokenProvider for Option<T>
where
    T: IntoTokenProvider,
{
    fn into_token_provider(self) -> Option<TokenProvider> {
        self.and_then(|t| t.into_token_provider())
    }
}

impl IntoTokenProvider for TokenGenerator {
    fn into_token_provider(self) -> Option<TokenProvider> {
        Some(TokenProvider::Generator(self))
    }
}

impl IntoTokenProvider for String {
    fn into_token_provider(self) -> Option<TokenProvider> {
        Some(TokenProvider::Static(self))
    }
}

impl IntoTokenProvider for &str {
    fn into_token_provider(self) -> Option<TokenProvider> {
        Some(TokenProvider::Static(self.to_owned()))
    }
}

/// A utility to generate auth tokens to be used in Objectstore requests.
///
/// Tokens are signed with an EdDSA private key and have certain permissions and expiry timeouts
/// applied.
///
/// Use this for internal services that have access to an EdDSA keypair. A `TokenGenerator`
/// implements [`IntoTokenProvider`], so it can be passed directly to
/// [`ClientBuilder::token`](crate::ClientBuilder::token), where it becomes a
/// [`TokenProvider::Generator`].
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
        self.permissions = HashSet::from_iter(permissions.iter().copied());
        self
    }

    /// Sign a token for the given [`Scope`](crate::Scope), returning the JWT string.
    ///
    /// Use this to produce a static token that can be handed to an external service
    /// which then passes it to [`ClientBuilder::token`](crate::ClientBuilder::token).
    ///
    /// The token is signed with the generator's default permissions and expiry. Use
    /// [`sign_with`](Self::sign_with) to override them for a single token.
    ///
    /// # Errors
    ///
    /// Returns an error if the scope is invalid or the JWT cannot be signed.
    pub fn sign(&self, scope: &crate::Scope) -> crate::Result<String> {
        self.sign_with(scope, None, None)
    }

    /// Sign a token for the given [`Scope`](crate::Scope), optionally overriding the
    /// generator's default permissions and/or expiry for this token only.
    ///
    /// When `permissions` is `Some`, they must be a subset of the permissions granted to this
    /// generator, otherwise an [`Error::PermissionEscalation`](crate::Error::PermissionEscalation)
    /// is returned. When `expiry_seconds` is `Some`, it overrides the generator's default expiry.
    ///
    /// # Errors
    ///
    /// Returns an error if the scope is invalid, a requested permission is not granted to this
    /// generator, or the JWT cannot be signed.
    pub fn sign_with(
        &self,
        scope: &crate::Scope,
        permissions: Option<&[Permission]>,
        expiry_seconds: Option<u64>,
    ) -> crate::Result<String> {
        let scope = match &scope.0 {
            Ok(inner) => inner,
            Err(crate::Error::InvalidScope(err)) => {
                return Err(err.clone().into());
            }
            // Return an ad-hoc `Unreachable` variant to avoid panicking.
            // It should be impossible to run into a different error variant other than
            // `InvalidScope`, unless we add a new variant and forget to update this code path.
            _ => return Err(scope::InvalidScopeError::Unreachable.into()),
        };
        self.sign_for_scope(scope, permissions, expiry_seconds)
    }

    /// Sign a new token for the passed-in scope, applying the given permission and expiry
    /// overrides (falling back to the generator's defaults when `None`).
    pub(crate) fn sign_for_scope(
        &self,
        scope: &ScopeInner,
        permissions: Option<&[Permission]>,
        expiry_seconds: Option<u64>,
    ) -> crate::Result<String> {
        let claims = JwtClaims {
            exp: get_current_timestamp() + expiry_seconds.unwrap_or(self.expiry_seconds),
            permissions: self.resolve_permissions(permissions)?,
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

    /// Resolves the permissions to embed in a token, validating that any explicitly requested
    /// permissions are a subset of those granted to this generator.
    fn resolve_permissions(
        &self,
        requested: Option<&[Permission]>,
    ) -> crate::Result<HashSet<Permission>> {
        let Some(requested) = requested else {
            return Ok(self.permissions.clone());
        };

        let requested: HashSet<Permission> = requested.iter().copied().collect();
        let mut escalated: Vec<Permission> =
            requested.difference(&self.permissions).copied().collect();
        if !escalated.is_empty() {
            escalated.sort_by_key(Permission::to_string);
            return Err(crate::Error::PermissionEscalation { escalated });
        }
        Ok(requested)
    }
}
