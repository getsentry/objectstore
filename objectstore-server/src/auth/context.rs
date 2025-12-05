use std::collections::{BTreeMap, HashSet};

use jsonwebtoken::{Algorithm, DecodingKey, Header, TokenData, Validation, decode, decode_header};
use objectstore_service::ObjectPath;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::util::StringOrWildcard;
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

/// `AuthContext` encapsulates the verified content of things like authorization tokens.
///
/// [`AuthContext::assert_authorized`] can be used to check whether a request is authorized to
/// perform certain operations on a given resource.
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub struct AuthContext {
    /// The objectstore usecase that this request may act on.
    pub usecase: String,

    /// The scope elements that this request may act on. See also: [`ObjectPath::scope`].
    pub scopes: BTreeMap<String, StringOrWildcard>,

    /// The permissions that this request has been granted.
    pub permissions: HashSet<Permission>,

    /// If true, authorization checks are performed and logged but failures are suppressed.
    /// If false, authorization failures result in errors.
    pub enforce: bool,
}

/// Error type for different authorization failure scenarios.
#[derive(Error, Debug, PartialEq)]
pub enum AuthError {
    /// Indicates that something about the request prevented authorization verification from
    /// happening properly.
    #[error("bad request: {0}")]
    BadRequest(&'static str),

    /// Indicates that something about Objectstore prevented authorization verification from
    /// happening properly.
    #[error("internal error: {0}")]
    InternalError(String),

    /// Indicates that the provided authorization token is invalid (e.g. expired or malformed).
    #[error("failed to decode token: {0}")]
    ValidationFailure(#[from] jsonwebtoken::errors::Error),

    /// Indicates that an otherwise-valid token was unable to be verified with configured keys.
    #[error("failed to verify token")]
    VerificationFailure,

    /// Indicates that the requested operation is not authorized and auth enforcement is enabled.
    #[error("operation not allowed")]
    NotPermitted,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct JwtRes {
    #[serde(rename = "os:usecase")]
    usecase: String,

    #[serde(flatten)]
    scope: BTreeMap<String, StringOrWildcard>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct JwtClaims {
    res: JwtRes,
    permissions: HashSet<Permission>,
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
        let encoded_token =
            encoded_token.ok_or(AuthError::BadRequest("No authorization token provided"))?;

        let jwt_header = decode_header(encoded_token)?;
        let key_id = jwt_header
            .kid
            .as_ref()
            .ok_or(AuthError::BadRequest("JWT header is missing `kid` field"))?;

        let key_config = config
            .keys
            .get(key_id)
            .ok_or_else(|| AuthError::InternalError(format!("Key `{key_id}` not configured")))?;

        if jwt_header.alg != Algorithm::EdDSA {
            tracing::warn!(
                "JWT signed with unexpected algorithm `{:?}`",
                jwt_header.alg
            );
        }

        let mut verified_claims: Option<TokenData<JwtClaims>> = None;
        for key in &key_config.key_versions {
            let decoding_key = match jwt_header.alg {
                Algorithm::EdDSA => DecodingKey::from_ed_pem(key.expose_secret().as_bytes())?,
                _ => DecodingKey::from_secret(key.expose_secret().as_bytes()),
            };

            let decode_result = decode::<JwtClaims>(
                encoded_token,
                &decoding_key,
                &jwt_validation_params(&jwt_header),
            );

            // Handle retryable errors
            use jsonwebtoken::errors::ErrorKind;
            if decode_result
                .as_ref()
                .is_err_and(|err| err.kind() == &ErrorKind::InvalidSignature)
            {
                continue;
            }

            verified_claims = Some(decode_result?);
            break;
        }
        let verified_claims = verified_claims.ok_or(AuthError::VerificationFailure)?;

        let usecase = verified_claims.claims.res.usecase;
        let scope = verified_claims.claims.res.scope;

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
            scopes: scope,
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
    pub fn assert_authorized(&self, perm: Permission, path: &ObjectPath) -> Result<(), AuthError> {
        if !self.permissions.contains(&perm) || self.usecase != path.usecase {
            self.fail_if_enforced(&perm, path)?;
        }

        for element in &path.scope {
            let (key, value) = element
                .split_once('.')
                .ok_or(AuthError::InternalError(format!(
                    "Invalid scope element: {element}"
                )))?;
            let authorized = match self.scopes.get(key) {
                Some(StringOrWildcard::String(s)) => s == value,
                Some(StringOrWildcard::Wildcard) => true,
                None => false,
            };
            if !authorized {
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
    use serde_json::json;

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
            exp: get_current_timestamp() + 300,
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
        org: &str,
        proj: &str,
        usecase: &str,
        permissions: HashSet<Permission>,
    ) -> JwtClaims {
        serde_json::from_value(json!({
            "res": {
                "os:usecase": usecase,
                "org": org,
                "project": proj,
            },
            "permissions": permissions,
        }))
        .unwrap()
    }

    fn sample_auth_context(org: &str, proj: &str, permissions: HashSet<Permission>) -> AuthContext {
        AuthContext {
            usecase: "attachments".into(),
            permissions,
            enforce: true,
            scopes: serde_json::from_value(json!({"org": org, "project": proj})).unwrap(),
        }
    }

    #[test]
    fn test_from_encoded_jwt_basic() -> Result<(), AuthError> {
        // Create a token with max permissions
        let claims = sample_claims("123", "456", "attachments", max_permission());
        let encoded_token = sign_token(&claims, TEST_SIGNING_SECRET);

        // Create test config with max permissions
        let test_config = test_config(max_permission());
        let auth_context =
            AuthContext::from_encoded_jwt(Some(encoded_token.as_str()), &test_config)?;

        // Ensure the key is correctly verified and deserialized
        let expected = sample_auth_context("123", "456", max_permission());
        assert_eq!(auth_context, expected);

        Ok(())
    }

    #[test]
    fn test_from_encoded_jwt_max_permissions_limit() -> Result<(), AuthError> {
        // Create a token with max permissions
        let claims = sample_claims("123", "456", "attachments", max_permission());
        let encoded_token = sign_token(&claims, TEST_SIGNING_SECRET);

        // Assign read-only permissions to the signing key in config
        let ro_permission = HashSet::from([Permission::ObjectRead]);
        let test_config = test_config(ro_permission.clone());
        let auth_context =
            AuthContext::from_encoded_jwt(Some(encoded_token.as_str()), &test_config)?;

        // Ensure the key is correctly verified and that the permissions are restricted
        let expected = sample_auth_context("123", "456", ro_permission);
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
        assert!(matches!(auth_context, Err(AuthError::ValidationFailure(_))));

        Ok(())
    }

    #[test]
    fn test_from_encoded_jwt_unknown_key_fails() -> Result<(), AuthError> {
        // Create a token with max permissions
        let claims = sample_claims("123", "456", "attachments", max_permission());
        let encoded_token = sign_token(&claims, "unknown signing key");

        // Create test config with max permissions
        let test_config = test_config(max_permission());
        let auth_context =
            AuthContext::from_encoded_jwt(Some(encoded_token.as_str()), &test_config);

        // Ensure the token failed verification
        assert!(matches!(auth_context, Err(AuthError::VerificationFailure)));

        Ok(())
    }

    #[test]
    fn test_from_encoded_jwt_expired() -> Result<(), AuthError> {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};

        let claims = sample_claims("123", "456", "attachments", max_permission());

        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(TEST_SIGNING_KID.into());
        header.typ = Some("JWT".into());

        let claims = TestJwtClaims {
            exp: get_current_timestamp() - 100, // NB: expired
            claims: claims.clone(),
        };

        let encoded_token = encode(
            &header,
            &claims,
            &EncodingKey::from_secret(TEST_SIGNING_SECRET.as_bytes()),
        )
        .unwrap();

        // Create test config with max permissions
        let test_config = test_config(max_permission());
        let auth_context =
            AuthContext::from_encoded_jwt(Some(encoded_token.as_str()), &test_config);

        // Ensure the token failed verification
        let Err(AuthError::ValidationFailure(error)) = auth_context else {
            panic!("auth must fail");
        };
        assert_eq!(
            error.kind(),
            &jsonwebtoken::errors::ErrorKind::ExpiredSignature
        );

        Ok(())
    }

    fn sample_object_path(org: u32, proj: u32) -> ObjectPath {
        ObjectPath {
            usecase: "attachments".into(),
            scope: vec![format!("org.{org}"), format!("project.{proj}")],
            key: "abcde".into(),
        }
    }

    // Allowed:
    //   auth_context: org.123 / proj.123
    //         object: org.123 / proj.123
    #[test]
    fn test_assert_authorized_exact_scope_allowed() -> Result<(), AuthError> {
        let auth_context = sample_auth_context("123", "456", max_permission());
        let object = sample_object_path(123, 456);

        auth_context.assert_authorized(Permission::ObjectRead, &object)?;

        Ok(())
    }

    // Allowed:
    //   auth_context: org.123 / proj.*
    //         object: org.123 / proj.123
    #[test]
    fn test_assert_authorized_wildcard_project_allowed() -> Result<(), AuthError> {
        let auth_context = sample_auth_context("123", "*", max_permission());
        let object = sample_object_path(123, 456);

        auth_context.assert_authorized(Permission::ObjectRead, &object)?;

        Ok(())
    }

    // Allowed:
    //   auth_context: org.123 / proj.456
    //         object: org.123
    #[test]
    fn test_assert_authorized_org_only_path_allowed() -> Result<(), AuthError> {
        let auth_context = sample_auth_context("123", "456", max_permission());
        let mut object = sample_object_path(123, 999);
        object.scope.pop();

        auth_context.assert_authorized(Permission::ObjectRead, &object)?;

        Ok(())
    }

    // Not allowed:
    //   auth_context: org.123 / proj.456
    //         object: org.123 / proj.999
    //
    //   auth_context: org.123 / proj.456
    //         object: org.999 / proj.456
    #[test]
    fn test_assert_authorized_scope_mismatch_fails() -> Result<(), AuthError> {
        let auth_context = sample_auth_context("123", "456", max_permission());
        let object = sample_object_path(123, 999);

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        let auth_context = sample_auth_context("123", "456", max_permission());
        let object = sample_object_path(999, 456);

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_wrong_usecase_fails() -> Result<(), AuthError> {
        let mut auth_context = sample_auth_context("123", "456", max_permission());
        auth_context.usecase = "debug-files".into();
        let object = sample_object_path(123, 456);

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_auth_context_missing_permission_fails() -> Result<(), AuthError> {
        let auth_context =
            sample_auth_context("123", "456", HashSet::from([Permission::ObjectRead]));
        let object = sample_object_path(123, 456);

        let result = auth_context.assert_authorized(Permission::ObjectWrite, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_passes_if_enforcement_disabled() -> Result<(), AuthError> {
        // Auth context is read-only but we will try using write permissions
        let mut auth_context =
            sample_auth_context("123", "456", HashSet::from([Permission::ObjectRead]));
        // Object's scope is not covered by the auth context
        let object = sample_object_path(999, 999);

        // Auth fails for two reasons, but because enforcement is off, it should not return an error
        auth_context.enforce = false;
        auth_context.assert_authorized(Permission::ObjectWrite, &object)?;

        Ok(())
    }
}
