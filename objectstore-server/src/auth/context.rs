use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, SystemTime};

use http::Method;
use jsonwebtoken::{Algorithm, Header, TokenData, Validation, decode, decode_header};
use objectstore_service::id::ObjectContext;
use objectstore_types::auth::Permission;
use objectstore_types::presign::CanonicalRequest;
use serde::{Deserialize, Serialize};

use crate::auth::KeyId;
use crate::auth::error::AuthError;
use crate::auth::key_directory::PublicKeyDirectory;
use crate::auth::util::StringOrWildcard;

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

/// The verified authorization details obtained from a JWT.
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub struct ScopedContext {
    /// The objectstore usecase that this request may act on.
    ///
    /// See also: [`ObjectContext::usecase`].
    pub usecase: String,

    /// The scope elements that this request may act on.
    ///
    /// See also: [`ObjectContext::scopes`].
    pub scopes: BTreeMap<String, StringOrWildcard>,

    /// The permissions that this request has been granted.
    pub permissions: HashSet<Permission>,
}

/// Maximum duration for a pre-signed URL.
const MAX_PRESIGN_DURATION: Duration = Duration::from_secs(7 * 24 * 60 * 60); // 7 days

/// The pre-signing query parameters.
#[derive(Debug, Deserialize)]
pub struct PresignParams {
    /// Key ID identifying which signing key was used.
    #[serde(rename = "os_kid")]
    pub key_id: KeyId,
    /// Base64url-encoded Ed25519 signature.
    #[serde(rename = "os_sig")]
    pub signature: String,
    /// RFC 3339 timestamp of when the URL was signed.
    #[serde(rename = "os_timestamp", with = "humantime_serde")]
    pub timestamp: SystemTime,
    /// Validity duration in seconds from `timestamp`.
    #[serde(rename = "os_duration")]
    pub duration_secs: u64,
}

/// `AuthContext` encapsulates the verified authorization details of a request.
///
/// [`AuthContext::assert_authorized`] can be used to check whether a request is authorized to
/// perform certain operations on a given resource.
#[derive(Debug, PartialEq)]
pub enum AuthContext {
    /// Authorization is inactive; every operation is permitted.
    Disabled,
    /// A verified JWT; each operation is checked against these scopes and permissions.
    Scoped(ScopedContext),
    /// A valid signature already authorized this exact request.
    Preauthorized,
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
        key_directory: &PublicKeyDirectory,
    ) -> Result<AuthContext, AuthError> {
        let encoded_token =
            encoded_token.ok_or(AuthError::BadRequest("No authorization token provided"))?;

        let jwt_header = decode_header(encoded_token)?;
        let key_id = jwt_header
            .kid
            .as_ref()
            .ok_or(AuthError::BadRequest("JWT header is missing `kid` field"))?;

        let key_config = key_directory
            .keys
            .get(key_id)
            .ok_or(AuthError::UnknownKey)?;

        if jwt_header.alg != Algorithm::EdDSA {
            objectstore_log::warn!(
                algorithm = ?jwt_header.alg,
                "JWT signed with unexpected algorithm",
            );
            let kind = jsonwebtoken::errors::ErrorKind::InvalidAlgorithm;
            return Err(AuthError::ValidationFailure(kind.into()));
        }

        let mut verified_claims: Option<TokenData<JwtClaims>> = None;
        for key_version in &key_config.key_versions {
            let decode_result = decode::<JwtClaims>(
                encoded_token,
                &key_version.decoding_key,
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
            .copied()
            .collect();

        Ok(AuthContext::Scoped(ScopedContext {
            usecase,
            scopes: scope,
            permissions,
        }))
    }

    /// Construct an `AuthContext` from a pre-signed request.
    ///
    /// A pre-signed URL carries its signature and parameters in the query string (see
    /// [`objectstore_types::presign`]). This verifies the signature against the request's
    /// canonical form and enforces the maximum duration, returning [`AuthContext::Preauthorized`]
    /// on success.
    pub fn from_presigned_request(
        method: &Method,
        path: &str,
        raw_query: Option<&str>,
        params: &PresignParams,
        key_directory: &PublicKeyDirectory,
        now: SystemTime,
    ) -> Result<AuthContext, AuthError> {
        let key_config = key_directory
            .keys
            .get(&params.key_id)
            .ok_or(AuthError::UnknownKey)?;

        let duration = Duration::from_secs(params.duration_secs);
        if duration > MAX_PRESIGN_DURATION {
            return Err(AuthError::BadRequest(
                "presigned URL validity exceeds the maximum of 1 week",
            ));
        }

        let start = params
            .timestamp
            // Subtract 60 secs to account for possible clock skew.
            .checked_sub(Duration::from_secs(60))
            .ok_or(AuthError::VerificationFailure)?;
        let end = params
            .timestamp
            .checked_add(duration)
            .ok_or(AuthError::VerificationFailure)?;
        if now < start || now > end {
            return Err(AuthError::VerificationFailure);
        }

        let canonical = CanonicalRequest::new(method, path, raw_query);

        let verified = key_config.key_versions.iter().any(|key| {
            canonical
                .verify(key.verifying_key.as_bytes(), &params.signature)
                .is_ok()
        });
        if !verified {
            return Err(AuthError::VerificationFailure);
        }

        // Pre-signed URLs currently only support read operations (GET/HEAD).
        if !key_config.max_permissions.contains(&Permission::ObjectRead) {
            return Err(AuthError::NotPermitted);
        }

        Ok(AuthContext::Preauthorized)
    }

    /// Ensures that an operation requiring `perm` and applying to `path` is authorized. If not,
    /// `Err(AuthError::NotPermitted)` is returned.
    ///
    /// - [`AuthContext::Disabled`] permits every operation.
    /// - [`AuthContext::Scoped`] permits the operation if `perm` is within the granted permissions
    ///   and usecase and scopes match the granted ones.
    /// - [`AuthContext::Preauthorized`] always permits the operation — the signing key's
    ///   permissions were already verified when the pre-signed URL was validated.
    pub fn assert_authorized(
        &self,
        perm: Permission,
        context: &ObjectContext,
    ) -> Result<(), AuthError> {
        let scoped = match self {
            AuthContext::Disabled => return Ok(()),
            AuthContext::Preauthorized =>
            // Pre-signed URLs currently only support read operations (GET/HEAD).
            {
                return if perm == Permission::ObjectRead {
                    Ok(())
                } else {
                    Err(AuthError::NotPermitted)
                };
            }
            AuthContext::Scoped(scoped) => scoped,
        };

        if !scoped.permissions.contains(&perm) || scoped.usecase != context.usecase {
            return Err(AuthError::NotPermitted);
        }

        for scope in &context.scopes {
            let authorized = match scoped.scopes.get(scope.name()) {
                Some(StringOrWildcard::String(s)) => s == scope.value(),
                Some(StringOrWildcard::Wildcard) => true,
                None => false,
            };
            if !authorized {
                return Err(AuthError::NotPermitted);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{PublicKey, PublicKeyConfig};
    use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
    use ed25519_dalek::{SigningKey, VerifyingKey};
    use jsonwebtoken::DecodingKey;
    use objectstore_types::presign::{
        CanonicalRequest, PARAM_DURATION, PARAM_KID, PARAM_SIG, PARAM_TIMESTAMP,
    };
    use objectstore_types::scope::{Scope, Scopes};
    use serde_json::json;

    use objectstore_test::server::{TEST_EDDSA_KID, TEST_EDDSA_PRIVKEY, TEST_EDDSA_PUBKEY};

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

    fn test_key_config(max_permissions: HashSet<Permission>) -> PublicKeyDirectory {
        let public_key = PublicKeyConfig {
            key_versions: vec![PublicKey {
                decoding_key: DecodingKey::from_ed_pem(TEST_EDDSA_PUBKEY.as_bytes()).unwrap(),
                verifying_key: VerifyingKey::from_public_key_pem(TEST_EDDSA_PUBKEY).unwrap(),
            }],
            max_permissions,
        };
        PublicKeyDirectory {
            keys: BTreeMap::from([(TEST_EDDSA_KID.into(), public_key)]),
        }
    }

    fn sign_token(claims: &JwtClaims, signing_secret: &str, exp: Option<u64>) -> String {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};

        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(TEST_EDDSA_KID.into());
        header.typ = Some("JWT".into());

        let claims = TestJwtClaims {
            exp: exp.unwrap_or_else(|| get_current_timestamp() + 300),
            claims: claims.clone(),
        };

        let key = EncodingKey::from_ed_pem(signing_secret.as_bytes()).unwrap();
        encode(&header, &claims, &key).unwrap()
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
        AuthContext::Scoped(ScopedContext {
            usecase: "attachments".into(),
            permissions,
            scopes: serde_json::from_value(json!({"org": org, "project": proj})).unwrap(),
        })
    }

    #[test]
    fn test_from_encoded_jwt_basic() -> Result<(), AuthError> {
        // Create a token with max permissions
        let claims = sample_claims("123", "456", "attachments", max_permission());
        let encoded_token = sign_token(&claims, TEST_EDDSA_PRIVKEY, None);

        // Create test config with max permissions
        let test_config = test_key_config(max_permission());
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
        let encoded_token = sign_token(&claims, TEST_EDDSA_PRIVKEY, None);

        // Assign read-only permissions to the signing key in config
        let ro_permission = HashSet::from([Permission::ObjectRead]);
        let test_config = test_key_config(ro_permission.clone());
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
        let test_config = test_key_config(max_permission());
        let auth_context = AuthContext::from_encoded_jwt(Some(encoded_token), &test_config);

        // Ensure the token failed verification
        assert!(matches!(auth_context, Err(AuthError::ValidationFailure(_))));

        Ok(())
    }

    #[test]
    fn test_from_encoded_jwt_unknown_key_fails() -> Result<(), AuthError> {
        let claims = sample_claims("123", "456", "attachments", max_permission());
        let unknown_key = r#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIKwVoE4TmTfWoqH3HgLVsEcHs9PHNe+ar/Hp6e4To8pK
-----END PRIVATE KEY-----
"#;
        let encoded_token = sign_token(&claims, unknown_key, None);

        // Create test config with max permissions
        let test_config = test_key_config(max_permission());
        let auth_context =
            AuthContext::from_encoded_jwt(Some(encoded_token.as_str()), &test_config);

        // Ensure the token failed verification
        assert!(matches!(auth_context, Err(AuthError::VerificationFailure)));

        Ok(())
    }

    #[test]
    fn test_from_encoded_jwt_expired() -> Result<(), AuthError> {
        let claims = sample_claims("123", "456", "attachments", max_permission());
        let encoded_token = sign_token(
            &claims,
            TEST_EDDSA_PRIVKEY,
            Some(jsonwebtoken::get_current_timestamp() - 100),
        );

        // Create test config with max permissions
        let test_config = test_key_config(max_permission());
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

    fn sample_object_context(org: &str, project: &str) -> ObjectContext {
        ObjectContext {
            usecase: "attachments".into(),
            scopes: Scopes::from_iter([
                Scope::create("org", org).unwrap(),
                Scope::create("project", project).unwrap(),
            ]),
        }
    }

    // Allowed:
    //   auth_context: org.123 / proj.123
    //         object: org.123 / proj.123
    #[test]
    fn test_assert_authorized_exact_scope_allowed() -> Result<(), AuthError> {
        let auth_context = sample_auth_context("123", "456", max_permission());
        let object = sample_object_context("123", "456");

        auth_context.assert_authorized(Permission::ObjectRead, &object)?;

        Ok(())
    }

    // Allowed:
    //   auth_context: org.123 / proj.*
    //         object: org.123 / proj.123
    #[test]
    fn test_assert_authorized_wildcard_project_allowed() -> Result<(), AuthError> {
        let auth_context = sample_auth_context("123", "*", max_permission());
        let object = sample_object_context("123", "456");

        auth_context.assert_authorized(Permission::ObjectRead, &object)?;

        Ok(())
    }

    // Allowed:
    //   auth_context: org.123 / proj.456
    //         object: org.123
    #[test]
    fn test_assert_authorized_org_only_path_allowed() -> Result<(), AuthError> {
        let auth_context = sample_auth_context("123", "456", max_permission());
        let object = ObjectContext {
            usecase: "attachments".into(),
            scopes: Scopes::from_iter([Scope::create("org", "123").unwrap()]),
        };

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
        let object = sample_object_context("123", "999");

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        let auth_context = sample_auth_context("123", "456", max_permission());
        let object = sample_object_context("999", "456");

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_wrong_usecase_fails() -> Result<(), AuthError> {
        let AuthContext::Scoped(mut scoped) = sample_auth_context("123", "456", max_permission())
        else {
            panic!("expected a scoped auth context");
        };
        scoped.usecase = "debug-files".into();
        let auth_context = AuthContext::Scoped(scoped);
        let object = sample_object_context("123", "456");

        let result = auth_context.assert_authorized(Permission::ObjectRead, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_assert_authorized_auth_context_missing_permission_fails() -> Result<(), AuthError> {
        let auth_context =
            sample_auth_context("123", "456", HashSet::from([Permission::ObjectRead]));
        let object = sample_object_context("123", "456");

        let result = auth_context.assert_authorized(Permission::ObjectWrite, &object);
        assert_eq!(result, Err(AuthError::NotPermitted));

        Ok(())
    }

    #[test]
    fn test_auth_context_from_presigned() {
        let key_directory = test_key_config(HashSet::from([Permission::ObjectRead]));

        let path = "/v1/objects/test/org=1/key";
        let timestamp = humantime::format_rfc3339(SystemTime::now()).to_string();
        let base = format!(
            "{PARAM_KID}={TEST_EDDSA_KID}&{PARAM_TIMESTAMP}={timestamp}&{PARAM_DURATION}=3600"
        );

        let signing_key = SigningKey::from_pkcs8_pem(TEST_EDDSA_PRIVKEY).unwrap();
        let canonical = CanonicalRequest::new(&Method::GET, path, Some(&base));
        let signature = canonical.sign(signing_key.as_bytes());
        let query = format!("{base}&{PARAM_SIG}={signature}");

        let params = PresignParams {
            signature,
            key_id: TEST_EDDSA_KID.to_string(),
            timestamp: SystemTime::now(),
            duration_secs: 3600,
        };

        let context = AuthContext::from_presigned_request(
            &Method::GET,
            path,
            Some(&query),
            &params,
            &key_directory,
            SystemTime::now(),
        )
        .unwrap();

        assert_eq!(context, AuthContext::Preauthorized);
    }

    #[test]
    fn test_presigned_request_rejected_without_read_permission() {
        let key_directory = test_key_config(HashSet::from([
            Permission::ObjectWrite,
            Permission::ObjectDelete,
        ]));

        let path = "/v1/objects/test/org=1/key";
        let timestamp = humantime::format_rfc3339(SystemTime::now()).to_string();
        let base = format!(
            "{PARAM_KID}={TEST_EDDSA_KID}&{PARAM_TIMESTAMP}={timestamp}&{PARAM_DURATION}=3600"
        );

        let signing_key = SigningKey::from_pkcs8_pem(TEST_EDDSA_PRIVKEY).unwrap();
        let canonical = CanonicalRequest::new(&Method::GET, path, Some(&base));
        let signature = canonical.sign(signing_key.as_bytes());
        let query = format!("{base}&{PARAM_SIG}={signature}");

        let params = PresignParams {
            signature,
            key_id: TEST_EDDSA_KID.to_string(),
            timestamp: SystemTime::now(),
            duration_secs: 3600,
        };

        let result = AuthContext::from_presigned_request(
            &Method::GET,
            path,
            Some(&query),
            &params,
            &key_directory,
            SystemTime::now(),
        );

        assert_eq!(result, Err(AuthError::NotPermitted));
    }
}
