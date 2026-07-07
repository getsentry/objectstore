use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, SystemTime};

use http::{HeaderMap, HeaderName, Method};
use jsonwebtoken::{Algorithm, Header, TokenData, Validation, decode, decode_header};
use objectstore_service::id::ObjectContext;
use objectstore_types::auth::Permission;
use objectstore_types::presign::{
    CanonicalRequest, X_OS_EXPIRES, X_OS_KEY_ID, X_OS_SIG, X_OS_SIGNED_HEADERS, X_OS_TIMESTAMP,
};
use serde::{Deserialize, Serialize};

use crate::auth::error::AuthError;
use crate::auth::key_directory::PublicKeyDirectory;
use crate::auth::util::StringOrWildcard;

/// Maximum validity window for a pre-signed URL.
///
/// Requests whose `X-Os-Expires` claims a longer window are rejected so that a URL cannot be
/// minted to be effectively immortal.
const MAX_PRESIGN_VALIDITY: Duration = Duration::from_secs(7 * 24 * 60 * 60);

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

/// The verified authorization details carried by a scoped JWT.
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

/// `AuthContext` encapsulates the verified authorization details of a request.
///
/// [`AuthContext::assert_authorized`] can be used to check whether a request is authorized to
/// perform certain operations on a given resource.
#[derive(Debug, PartialEq)]
pub enum AuthContext {
    /// Authorization is inactive; every operation is permitted.
    Disabled,

    /// A valid pre-signed signature already authorized this exact request.
    ///
    /// The signature covers the request's method, path, query, and signed headers, so no scope
    /// check is required. The wrapped set is the signing key's `max_permissions`: the operation's
    /// permission must still be within it, so a restricted (e.g. read-only) key cannot be used to
    /// pre-sign a more privileged operation.
    Preauthorized(HashSet<Permission>),

    /// A verified JWT; each operation is checked against these scopes and permissions.
    Scoped(ScopedContext),
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
            .ok_or_else(|| AuthError::InternalError(format!("Key `{key_id}` not configured")))?;

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
    /// canonical form and enforces the validity window, returning [`AuthContext::Preauthorized`]
    /// on success.
    ///
    /// The caller is responsible for restricting the set of methods for which pre-signed URLs
    /// are accepted; `now` is the current time, injected for testability.
    pub fn from_presigned_request(
        method: &Method,
        path: &str,
        query: Option<&str>,
        headers: &HeaderMap,
        key_directory: &PublicKeyDirectory,
        now: SystemTime,
    ) -> Result<AuthContext, AuthError> {
        let query = query.unwrap_or_default();

        let signature = find_query_param(query, X_OS_SIG)
            .ok_or(AuthError::BadRequest("presigned URL missing X-Os-Sig"))?;

        let key_id = find_query_param(query, X_OS_KEY_ID)
            .ok_or(AuthError::BadRequest("presigned URL missing X-Os-Key-Id"))?;
        // An unknown key ID is treated as a verification failure (rather than leaking which
        // keys are configured), matching the failure mode of a bad signature.
        let key_config = key_directory
            .keys
            .get(key_id.as_ref())
            .ok_or(AuthError::VerificationFailure)?;

        // `X-Os-Signed-Headers` is an optional `;`-separated list of header names that the
        // signature covers.
        let signed_header_names = match find_query_param(query, X_OS_SIGNED_HEADERS) {
            Some(value) if !value.is_empty() => value
                .split(';')
                .map(|name| HeaderName::from_bytes(name.as_bytes()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| {
                    AuthError::BadRequest("presigned URL has an invalid signed header name")
                })?,
            _ => Vec::new(),
        };
        let signed_headers: Vec<&HeaderName> = signed_header_names.iter().collect();

        // Enforce the validity window before spending time on signature verification.
        let timestamp = find_query_param(query, X_OS_TIMESTAMP).ok_or(AuthError::BadRequest(
            "presigned URL missing X-Os-Timestamp",
        ))?;
        let timestamp = humantime::parse_rfc3339(timestamp.as_ref())
            .map_err(|_| AuthError::BadRequest("presigned URL has an invalid X-Os-Timestamp"))?;
        let expires: u64 = find_query_param(query, X_OS_EXPIRES)
            .ok_or(AuthError::BadRequest("presigned URL missing X-Os-Expires"))?
            .parse()
            .map_err(|_| AuthError::BadRequest("presigned URL has an invalid X-Os-Expires"))?;
        let expires = Duration::from_secs(expires);
        if expires > MAX_PRESIGN_VALIDITY {
            return Err(AuthError::BadRequest(
                "presigned URL validity exceeds the maximum of 1 week",
            ));
        }
        if now < timestamp || now > timestamp + expires {
            objectstore_log::debug!("presigned URL is outside its validity window");
            return Err(AuthError::VerificationFailure);
        }

        let canonical = CanonicalRequest::new(method, path, Some(query), headers, &signed_headers)
            .map_err(|_| AuthError::BadRequest("presigned URL has invalid signed headers"))?;

        // Any configured key version verifying the signature is sufficient (supports rotation).
        let verified = key_config.key_versions.iter().any(|key| {
            canonical
                .verify(&key.verifying_key, signature.as_ref())
                .is_ok()
        });
        if !verified {
            return Err(AuthError::VerificationFailure);
        }

        // The `Preauthorized` context carries the signing key's `max_permissions` so that
        // `assert_authorized` can still reject operations the key was never allowed to grant.
        Ok(AuthContext::Preauthorized(
            key_config.max_permissions.clone(),
        ))
    }

    /// Ensures that an operation requiring `perm` and applying to `path` is authorized. If not,
    /// `Err(AuthError::NotPermitted)` is returned.
    ///
    /// [`AuthContext::Disabled`] permits every operation. [`AuthContext::Preauthorized`] permits
    /// the operation as long as `perm` is within the signing key's `max_permissions` (the
    /// signature already binds the request's usecase and scopes). For [`AuthContext::Scoped`], the
    /// passed-in `perm` is checked against the granted permissions and the request's usecase and
    /// scopes are checked against the granted ones.
    pub fn assert_authorized(
        &self,
        perm: Permission,
        context: &ObjectContext,
    ) -> Result<(), AuthError> {
        let scoped = match self {
            AuthContext::Disabled => return Ok(()),
            AuthContext::Preauthorized(max_permissions) => {
                return if max_permissions.contains(&perm) {
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

/// Finds a query parameter by name in a raw query string, percent-decoding its value.
///
/// Returns the first match. Parameter names in the pre-signing scheme are never
/// percent-encoded, so the name is compared verbatim.
fn find_query_param<'a>(query: &'a str, name: &str) -> Option<Cow<'a, str>> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| percent_encoding::percent_decode_str(value).decode_utf8_lossy())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{PublicKey, PublicKeyConfig};
    use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
    use ed25519_dalek::{SigningKey, VerifyingKey};
    use jsonwebtoken::DecodingKey;
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

    // A `Preauthorized` context must still honor the signing key's `max_permissions`, so a
    // read-only key cannot authorize a delete even though the signature is valid.
    #[test]
    fn test_preauthorized_respects_max_permissions() {
        let context = AuthContext::Preauthorized(HashSet::from([Permission::ObjectRead]));
        let object = sample_object_context("123", "456");

        assert_eq!(
            context.assert_authorized(Permission::ObjectRead, &object),
            Ok(())
        );
        assert_eq!(
            context.assert_authorized(Permission::ObjectDelete, &object),
            Err(AuthError::NotPermitted)
        );
    }

    // Verifying a pre-signed request yields a `Preauthorized` context carrying the signing key's
    // `max_permissions`, which is what enforces the limit above end-to-end.
    #[test]
    fn test_from_presigned_request_carries_max_permissions() {
        use objectstore_types::presign::{
            CanonicalRequest, X_OS_EXPIRES, X_OS_KEY_ID, X_OS_SIG, X_OS_TIMESTAMP,
        };

        let read_only = HashSet::from([Permission::ObjectRead]);
        let key_directory = test_key_config(read_only.clone());

        let path = "/v1/objects/test/org=1/key";
        let timestamp = humantime::format_rfc3339(SystemTime::now()).to_string();
        let base = format!(
            "{X_OS_KEY_ID}={TEST_EDDSA_KID}&{X_OS_TIMESTAMP}={timestamp}&{X_OS_EXPIRES}=3600"
        );

        let signing_key = SigningKey::from_pkcs8_pem(TEST_EDDSA_PRIVKEY).unwrap();
        let canonical =
            CanonicalRequest::new(&Method::GET, path, Some(&base), &HeaderMap::new(), &[]).unwrap();
        let query = format!("{base}&{X_OS_SIG}={}", canonical.sign(&signing_key));

        let context = AuthContext::from_presigned_request(
            &Method::GET,
            path,
            Some(&query),
            &HeaderMap::new(),
            &key_directory,
            SystemTime::now(),
        )
        .unwrap();

        assert_eq!(context, AuthContext::Preauthorized(read_only));
    }
}
