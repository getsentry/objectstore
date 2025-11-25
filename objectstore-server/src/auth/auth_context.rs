use std::collections::HashSet;

use jsonwebtoken::{DecodingKey, Header, TokenData, Validation, decode, decode_header};
use objectstore_service::ObjectPath;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::AuthZ;

/// Permissions that control whether different operations are authorized.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum Permission {
    /// The permission required to read objects from objectstore.
    #[serde(rename = "object.read")]
    ObjectRead,

    /// The permission required to write/overwrite objects in objectstore.
    #[serde(rename = "object.write")]
    ObjectWrite,

    /// The permission required to delete objects from objectstore.
    #[serde(rename = "object.delete")]
    ObjectDelete,
}

/// `AuthContext`
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub struct AuthContext {
    /// The objectstore usecase that this request may act on.
    pub usecase: String,

    /// The objectstore scope that this request may act on.
    ///
    /// An `AuthContext` with `vec!["org.123"]` as its scope can act on any object whose
    /// scope starts with `org.123`.
    pub scope: Vec<String>,

    /// The permissions that this request has been granted.
    pub permissions: HashSet<Permission>,

    /// If true, authorization checks are performed and logged but failures are suppressed.
    /// If false, authorization failures result in errors.
    pub enforce: bool,
}

/// Error type for different authorization failure scenarios.
#[derive(Error, Debug, PartialEq)]
pub enum AuthError {
    /// Indicates the auth token was missing from the request or malformed.
    #[error("could not process JWT")]
    InvalidToken,

    /// Indicates that JWT verification failed. Possible causes include the token being signed with
    /// an unknown key or its `exp` field having passed.
    #[error("could not verify JWT signature")]
    InvalidSignature,

    /// Indicates that the requested operation is not authorized and auth enforcement is enabled.
    #[error("operation not allowed")]
    NotPermitted,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct JwtExt {
    os_usecase: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct JwtClaims {
    resources: String,
    permissions: HashSet<Permission>,
    ext: JwtExt,
}

fn jwt_validation_params(jwt_header: &Header) -> Validation {
    let mut validation = Validation::new(jwt_header.alg);
    validation.set_audience(&["objectstore"]);
    validation.set_issuer(&["sentry", "relay"]);
    validation.set_required_spec_claims(&["exp"]);
    validation
}

impl AuthContext {
    /// Construct an `AuthContext` from an encoded JWT.
    ///
    /// Objectstore JWTs _must_ contain:
    /// - the `kid` header indicating which key was used to sign the token
    /// - the `exp` claim indicating when the token expires
    ///
    /// The `aud` claim is not required, but if set it must be `"objectstore"`. The `iss` claim
    /// is not required, but if set it must be `"relay"` or `"sentry"`.
    ///
    /// To verify the token, objectstore will look up a list of possible keys based on the `kid`
    /// header field and attempt verification. It will also ensure that the timestamp from the
    /// `exp` claim field has not passed.
    pub fn from_encoded_jwt(
        encoded_token: Option<&str>,
        config: &AuthZ,
    ) -> Result<AuthContext, AuthError> {
        let Some(encoded_token) = encoded_token else {
            tracing::debug!("No authorization token provided");
            return Err(AuthError::InvalidToken);
        };

        let Ok(jwt_header) = decode_header(encoded_token) else {
            tracing::debug!("Provided authorization token is not valid JWT");
            return Err(AuthError::InvalidToken);
        };

        let Some(key_id) = jwt_header.kid.as_ref() else {
            tracing::debug!("JWT header is missing `kid` field");
            return Err(AuthError::InvalidToken);
        };

        let Some(key_config) = config.keys.get(key_id) else {
            return Err(AuthError::InvalidToken);
        };

        let mut verified_claims: Option<TokenData<JwtClaims>> = None;
        for key in &key_config.key_versions {
            // TODO: Certain failure reasons (e.g. token expiration) should early-exit
            verified_claims = decode::<JwtClaims>(
                encoded_token,
                &DecodingKey::from_secret(key.expose_secret().as_bytes()),
                &jwt_validation_params(&jwt_header),
            )
            .ok();
            if verified_claims.is_some() {
                break;
            }
        }

        let Some(verified_claims) = verified_claims else {
            tracing::debug!("Failed to verify JWT with all configured keys");
            return Err(AuthError::InvalidSignature);
        };

        let usecase = verified_claims.claims.ext.os_usecase;
        let scope = verified_claims
            .claims
            .resources
            .split('/')
            .map(|s| s.to_string())
            .collect();

        // Taking the intersection here ensures the `AuthContext` does not have any permissions
        // that `key_config.max_permissions` doesn't have, even if the token tried to grant them.
        let permissions = verified_claims
            .claims
            .permissions
            .intersection(&key_config.max_permissions)
            .cloned()
            .collect();

        Ok(AuthContext {
            usecase,
            scope,
            permissions,
            enforce: config.enforce,
        })
    }

    fn fail_if_enforced(&self, perm: &Permission, path: &ObjectPath) -> Result<(), AuthError> {
        tracing::debug!(?self, ?perm, ?path, "Authorization failed");
        if self.enforce {
            return Err(AuthError::NotPermitted);
        }
        Ok(())
    }

    /// Ensures that an operation requiring `perm` and applying to `path` is authorized. If not,
    /// `Err(AuthError::NotPermitted)` is returned.
    ///
    /// The passed-in `perm` is checked against this `AuthContext`'s `permissions`. If it is not
    /// present, then the operation is not authorized.
    ///
    /// The passed-in `path` is checked against this `AuthContext`'s `scope` and `usecase`.
    /// - If `self.usecase` does not match `path.usecase`, the operation is not authorized
    /// - If `self.scope` is _more specific_ than `path.scope`, the operation is not authorized
    /// - If `self.scope` _diverges from_ `path.scope`, the operation is not authorized
    ///
    /// See below for examples of how `scope` is compared.
    ///
    /// ```text
    /// Allowed, because the scopes match exactly:
    ///     path.scope: state.wa / city.seattle
    ///     self.scope: state.wa / city.seattle
    ///
    /// Allowed, because `self.scope` is broader than `path.scope`:
    ///     path.scope: state.wa / city.seattle
    ///     self.scope: state.wa
    ///
    /// Not allowed, because `self.scope` is more specific than `path.scope`:
    ///     path.scope: state.wa / city.seattle
    ///     self.scope: state.wa / city.seattle / neighborhood.fremont
    ///
    /// Not allowed, because `self.scope` diverges from `path.scope`:
    ///     path.scope: state.wa / city.seattle
    ///     self.scope: state.mi
    /// ```
    pub fn assert_authorized(&self, perm: Permission, path: &ObjectPath) -> Result<(), AuthError> {
        if !self.permissions.contains(&perm) || self.usecase != path.usecase {
            self.fail_if_enforced(&perm, path)?;
        }

        // If our scope is more specific than the path's scope, we are definitely not authorized
        if self.scope.len() > path.scope.len() {
            self.fail_if_enforced(&perm, path)?;
            return Ok(());
        }

        // Otherwise, if our scope is equal to (or a prefix of) the path's scope, we are authorized
        for (auth_segment, obj_segment) in self.scope.iter().zip(path.scope.iter()) {
            // If any part of our scope differs from the path's scope, we are not authorized
            if auth_segment != obj_segment {
                self.fail_if_enforced(&perm, path)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthZVerificationKey, ConfigSecret};
    use secrecy::SecretBox;
    use std::collections::BTreeMap;

    const TEST_SIGNING_KID: &str = "test-key";
    const TEST_SIGNING_SECRET: &str = "fa24f0a3ab08f9ff0d4b2183595a045c";

    #[derive(Serialize, Deserialize)]
    struct TestJwtClaims {
        exp: u64,
        #[serde(flatten)]
        claims: JwtClaims,
    }

    fn max_permission() -> HashSet<Permission> {
        HashSet::from([
            Permission::ObjectRead,
            Permission::ObjectWrite,
            Permission::ObjectDelete,
        ])
    }

    fn test_config(max_permissions: HashSet<Permission>) -> AuthZ {
        let wrapped_key = SecretBox::new(Box::new(ConfigSecret::from(TEST_SIGNING_SECRET)));
        let key_config = AuthZVerificationKey {
            key_versions: vec![wrapped_key],
            max_permissions,
        };
        AuthZ {
            enforce: true,
            keys: BTreeMap::from([(TEST_SIGNING_KID.into(), key_config)]),
        }
    }

    fn sign_token(claims: &JwtClaims, signing_secret: &str) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};

        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(TEST_SIGNING_KID.into());
        header.typ = Some("JWT".into());

        let claims = TestJwtClaims {
            exp: get_current_timestamp(),
            claims: claims.clone(),
        };

        encode(
            &header,
            &claims,
            &EncodingKey::from_secret(signing_secret.as_bytes()),
        )
        .unwrap()
    }

    fn sample_claims(
        org: u32,
        proj: u32,
        usecase: &str,
        permissions: HashSet<Permission>,
    ) -> JwtClaims {
        JwtClaims {
            resources: format!("org.{org}/proj.{proj}"),
            permissions,
            ext: JwtExt {
                os_usecase: usecase.into(),
            },
        }
    }

    #[test]
    fn test_from_encoded_jwt_basic() -> Result<(), AuthError> {
        // Create a token with max permissions
        let claims = sample_claims(123, 456, "attachments", max_permission());
        let encoded_token = sign_token(&claims, TEST_SIGNING_SECRET);

        // Create test config with max permissions
        let test_config = test_config(max_permission());
        let auth_context =
            AuthContext::from_encoded_jwt(Some(encoded_token.as_str()), &test_config)?;

        // Ensure the key is correctly verified and deserialized
        let expected = AuthContext {
            usecase: "attachments".into(),
            scope: vec!["org.123".into(), "proj.456".into()],
            permissions: max_permission(),
            enforce: true,
        };
        assert_eq!(auth_context, expected);

        Ok(())
    }

    #[test]
    fn test_from_encoded_jwt_max_permissions_limit() -> Result<(), AuthError> {
        // Create a token with max permissions
        let claims = sample_claims(123, 456, "attachments", max_permission());
        let encoded_token = sign_token(&claims, TEST_SIGNING_SECRET);

        // Assign read-only permissions to the signing key in config
        let ro_permission = HashSet::from([Permission::ObjectRead]);
        let test_config = test_config(ro_permission.clone());
        let auth_context =
            AuthContext::from_encoded_jwt(Some(encoded_token.as_str()), &test_config)?;

        // Ensure the key is correctly verified and that the permissions are restricted
        let expected = AuthContext {
            usecase: "attachments".into(),
            scope: vec!["org.123".into(), "proj.456".into()],
            permissions: ro_permission,
            enforce: true,
        };
        assert_eq!(auth_context, expected);

        Ok(())
    }

    #[test]
    fn test_from_encoded_jwt_invalid_token_fails() -> Result<(), AuthError> {
        // Create a bogus token
        let encoded_token = "abcdef";

        // Create test config with max permissions
        let test_config = test_config(max_permission());
        let auth_context = AuthContext::from_encoded_jwt(Some(encoded_token), &test_config);

        // Ensure the token failed verification
        assert!(matches!(auth_context, Err(AuthError::InvalidToken)));

        Ok(())
    }

    #[test]
    fn test_from_encoded_jwt_unknown_key_fails() -> Result<(), AuthError> {
        // Create a token with max permissions
        let claims = sample_claims(123, 456, "attachments", max_permission());
        let encoded_token = sign_token(&claims, "unknown signing key");

        // Create test config with max permissions
        let test_config = test_config(max_permission());
        let auth_context =
            AuthContext::from_encoded_jwt(Some(encoded_token.as_str()), &test_config);

        // Ensure the token failed verification
        assert!(matches!(auth_context, Err(AuthError::InvalidSignature)));

        Ok(())
    }

    fn sample_auth_context(permissions: HashSet<Permission>) -> AuthContext {
        AuthContext {
            usecase: "attachments".into(),
            scope: vec!["org.123".into(), "proj.456".into()],
            enforce: true,
            permissions,
        }
    }

    fn sample_object_path(org: u32, proj: u32) -> ObjectPath {
        ObjectPath {
            usecase: "attachments".into(),
            scope: vec![format!("org.{}", org), format!("proj.{}", proj)],
            key: "abcde".into(),
        }
    }

    #[test]
    fn test_assert_authorized_basic() -> Result<(), AuthError> {
        let auth_context = sample_auth_context(max_permission());
        let object = sample_object_path(123, 456);

        auth_context.assert_authorized(Permission::ObjectRead, &object)?;

        Ok(())
    }

    #[test]
    fn test_assert_authorized_broader_scope_allowed() -> Result<(), AuthError> {
        let mut auth_context = sample_auth_context(max_permission());
        auth_context.scope.pop(); // Get rid of the most specific part of the scope
        let object = sample_object_path(123, 456);

        auth_context.assert_authorized(Permission::ObjectRead, &object)?;

        Ok(())
    }

    #[test]
    fn test_assert_authorized_scope_too_specific_fails() -> Result<(), AuthError> {
        let mut auth_context = sample_auth_context(max_permission());
        auth_context.scope.push("person.einstein".into());
        let object = sample_object_path(123, 456);

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_scope_mismatch() -> Result<(), AuthError> {
        let mut auth_context = sample_auth_context(max_permission());
        auth_context.scope = vec!["org.999".into(), "proj.456".into()];
        let object = sample_object_path(123, 456);

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_wrong_usecase_fails() -> Result<(), AuthError> {
        let mut auth_context = sample_auth_context(max_permission());
        auth_context.usecase = "debug-files".into();
        let object = sample_object_path(123, 456);

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_auth_context_missing_permission_fails() -> Result<(), AuthError> {
        let auth_context = sample_auth_context(HashSet::from([Permission::ObjectRead]));
        let object = sample_object_path(123, 456);

        let result = auth_context.assert_authorized(Permission::ObjectWrite, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_passes_if_enforcement_disabled() -> Result<(), AuthError> {
        // Auth context is read-only but we will try using write permissions
        let mut auth_context = sample_auth_context(HashSet::from([Permission::ObjectRead]));
        // Object's scope is not covered by the auth context
        let object = sample_object_path(999, 999);

        // Auth fails for two reasons, but because enforcement is off, it should not return an error
        auth_context.enforce = false;
        auth_context.assert_authorized(Permission::ObjectWrite, &object)?;

        Ok(())
    }
}
