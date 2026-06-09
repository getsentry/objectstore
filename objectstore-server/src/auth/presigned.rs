use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use axum::http::Method;
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation, decode, decode_header,
    encode,
};
use objectstore_service::id::ObjectId;
use objectstore_types::scope::Scope;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};

use crate::auth::AuthError;
use crate::config::{PresignedAuth, PresignedHmacKey};

const PRESIGNED_OPERATION_GET: &str = "GET";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PresignedScope {
    name: String,
    value: String,
}

impl From<&Scope> for PresignedScope {
    fn from(scope: &Scope) -> Self {
        Self {
            name: scope.name().to_owned(),
            value: scope.value().to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PresignedResource {
    #[serde(rename = "os:usecase")]
    usecase: String,

    #[serde(rename = "os:scopes")]
    scopes: Vec<PresignedScope>,

    #[serde(rename = "os:objectkey")]
    object_key: String,
}

impl From<&ObjectId> for PresignedResource {
    fn from(id: &ObjectId) -> Self {
        Self {
            usecase: id.usecase().to_owned(),
            scopes: id.iter_scopes().map(PresignedScope::from).collect(),
            object_key: id.key().to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PresignedClaims {
    exp: u64,
    operation: String,
    res: PresignedResource,
}

impl PresignedClaims {
    fn object_get(id: &ObjectId, expires_at: u64) -> Self {
        Self {
            exp: expires_at,
            operation: PRESIGNED_OPERATION_GET.to_owned(),
            res: id.into(),
        }
    }
}

fn read_secret_from_file(filename: &Path) -> anyhow::Result<String> {
    let secret = std::fs::read_to_string(filename)
        .with_context(|| format!("reading key from {filename:?}"))?;
    Ok(secret.trim_end_matches(['\r', '\n']).to_owned())
}

/// HMAC secret versions configured for a single presigned key ID.
#[derive(Debug)]
pub struct PresignedKeyConfig {
    secrets: Vec<String>,
}

impl PresignedKeyConfig {
    fn active_secret(&self) -> Option<&str> {
        self.secrets.first().map(String::as_str)
    }

    fn decoding_keys(&self) -> impl Iterator<Item = DecodingKey> + '_ {
        self.secrets
            .iter()
            .map(|secret| DecodingKey::from_secret(secret.as_bytes()))
    }
}

impl TryFrom<&PresignedHmacKey> for PresignedKeyConfig {
    type Error = anyhow::Error;

    fn try_from(key_config: &PresignedHmacKey) -> Result<Self, Self::Error> {
        let mut secrets = key_config
            .secrets
            .iter()
            .map(|secret| secret.expose_secret().as_str().to_owned())
            .collect::<Vec<_>>();

        secrets.extend(
            key_config
                .secret_files
                .iter()
                .map(|filename| {
                    read_secret_from_file(filename)
                        .inspect_err(|e| objectstore_log::error!("{:?}", e))
                })
                .collect::<anyhow::Result<Vec<String>>>()?,
        );

        Ok(Self { secrets })
    }
}

/// Directory of HMAC keys used for presigned URL signatures.
#[derive(Debug)]
pub struct PresignedKeyDirectory {
    signing_key_id: Option<String>,
    keys: BTreeMap<String, PresignedKeyConfig>,
}

impl PresignedKeyDirectory {
    /// Signs a presigned GET token for an object.
    pub fn sign_get(&self, id: &ObjectId, expires_at: u64) -> Result<String, AuthError> {
        let key_id = self.signing_key_id.as_deref().ok_or_else(|| {
            AuthError::InternalError("No presigned signing key configured".into())
        })?;
        let key = self.keys.get(key_id).ok_or_else(|| {
            AuthError::InternalError(format!("Presigned signing key `{key_id}` not configured"))
        })?;
        let secret = key.active_secret().ok_or_else(|| {
            AuthError::InternalError(format!("Presigned signing key `{key_id}` has no secrets"))
        })?;

        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(key_id.to_owned());
        header.typ = Some("JWT".into());

        Ok(encode(
            &header,
            &PresignedClaims::object_get(id, expires_at),
            &EncodingKey::from_secret(secret.as_bytes()),
        )?)
    }

    /// Verifies that a presigned token grants read access to exactly the passed object.
    pub fn verify_object_read(
        &self,
        encoded_token: &str,
        id: &ObjectId,
        method: &Method,
    ) -> Result<u64, AuthError> {
        if method != Method::GET && method != Method::HEAD {
            return Err(AuthError::NotPermitted);
        }

        let jwt_header = decode_header(encoded_token)?;
        let key_id = jwt_header
            .kid
            .as_ref()
            .ok_or(AuthError::BadRequest("JWT header is missing `kid` field"))?;

        if jwt_header.alg != Algorithm::HS256 {
            let kind = jsonwebtoken::errors::ErrorKind::InvalidAlgorithm;
            return Err(AuthError::ValidationFailure(kind.into()));
        }

        let key_config = self
            .keys
            .get(key_id)
            .ok_or(AuthError::VerificationFailure)?;

        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_required_spec_claims(&["exp"]);

        let mut verified_claims: Option<TokenData<PresignedClaims>> = None;
        for decoding_key in key_config.decoding_keys() {
            let decode_result =
                decode::<PresignedClaims>(encoded_token, &decoding_key, &validation);

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

        let claims = verified_claims
            .ok_or(AuthError::VerificationFailure)?
            .claims;
        let expected_resource = PresignedResource::from(id);
        if claims.operation != PRESIGNED_OPERATION_GET || claims.res != expected_resource {
            return Err(AuthError::NotPermitted);
        }

        Ok(claims.exp)
    }
}

impl TryFrom<&PresignedAuth> for PresignedKeyDirectory {
    type Error = anyhow::Error;

    fn try_from(presigned_config: &PresignedAuth) -> Result<Self, Self::Error> {
        Ok(Self {
            signing_key_id: presigned_config.signing_key_id.clone(),
            keys: presigned_config
                .keys
                .iter()
                .map(|(kid, key)| Ok((kid.clone(), key.try_into()?)))
                .collect::<Result<BTreeMap<String, PresignedKeyConfig>, anyhow::Error>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConfigSecret, PresignedAuth};
    use jsonwebtoken::get_current_timestamp;
    use secrecy::SecretBox;

    const KID: &str = "presigned-test";
    const SECRET: &str = "test-presigned-secret";
    const OLD_SECRET: &str = "old-presigned-secret";

    fn test_id(key: &str) -> ObjectId {
        ObjectId::from_parts(
            "attachments".to_owned(),
            objectstore_types::scope::Scopes::from_iter([
                Scope::create("org", "123").unwrap(),
                Scope::create("project", "456").unwrap(),
            ]),
            key.to_owned(),
        )
    }

    fn config_with_secrets(kid: &str, secrets: Vec<&str>) -> PresignedKeyDirectory {
        let config = PresignedAuth {
            signing_key_id: Some(kid.to_owned()),
            keys: BTreeMap::from([(
                kid.to_owned(),
                PresignedHmacKey {
                    secrets: secrets
                        .into_iter()
                        .map(|secret| SecretBox::new(Box::new(ConfigSecret::from(secret))))
                        .collect(),
                    secret_files: vec![],
                },
            )]),
        };
        PresignedKeyDirectory::try_from(&config).unwrap()
    }

    #[test]
    fn signs_and_verifies_get_for_exact_object() {
        let directory = config_with_secrets(KID, vec![SECRET]);
        let id = test_id("foo/bar");
        let expires_at = get_current_timestamp() + 60;

        let token = directory.sign_get(&id, expires_at).unwrap();
        let verified_exp = directory
            .verify_object_read(&token, &id, &Method::GET)
            .unwrap();

        assert_eq!(verified_exp, expires_at);
    }

    #[test]
    fn head_accepts_get_signature() {
        let directory = config_with_secrets(KID, vec![SECRET]);
        let id = test_id("foo/bar");
        let token = directory
            .sign_get(&id, get_current_timestamp() + 60)
            .unwrap();

        directory
            .verify_object_read(&token, &id, &Method::HEAD)
            .unwrap();
    }

    #[test]
    fn key_rotation_verifies_old_versions_and_mints_with_first_secret() {
        let old_directory = config_with_secrets(KID, vec![OLD_SECRET]);
        let rotated_directory = config_with_secrets(KID, vec![SECRET, OLD_SECRET]);
        let new_only_directory = config_with_secrets(KID, vec![SECRET]);
        let id = test_id("foo/bar");

        let old_token = old_directory
            .sign_get(&id, get_current_timestamp() + 60)
            .unwrap();
        rotated_directory
            .verify_object_read(&old_token, &id, &Method::GET)
            .unwrap();

        let new_token = rotated_directory
            .sign_get(&id, get_current_timestamp() + 60)
            .unwrap();
        new_only_directory
            .verify_object_read(&new_token, &id, &Method::GET)
            .unwrap();
    }

    #[test]
    fn wrong_kid_fails_verification() {
        let signing_directory = config_with_secrets("other-kid", vec![SECRET]);
        let verify_directory = config_with_secrets(KID, vec![SECRET]);
        let id = test_id("foo/bar");
        let token = signing_directory
            .sign_get(&id, get_current_timestamp() + 60)
            .unwrap();

        assert_eq!(
            verify_directory.verify_object_read(&token, &id, &Method::GET),
            Err(AuthError::VerificationFailure)
        );
    }

    #[test]
    fn tampered_signature_fails_verification() {
        let directory = config_with_secrets(KID, vec![SECRET]);
        let id = test_id("foo/bar");
        let mut token = directory
            .sign_get(&id, get_current_timestamp() + 60)
            .unwrap();
        token.push('x');

        assert!(matches!(
            directory.verify_object_read(&token, &id, &Method::GET),
            Err(AuthError::ValidationFailure(_)) | Err(AuthError::VerificationFailure)
        ));
    }

    #[test]
    fn wrong_object_fails_verification() {
        let directory = config_with_secrets(KID, vec![SECRET]);
        let token = directory
            .sign_get(&test_id("foo/bar"), get_current_timestamp() + 60)
            .unwrap();

        assert_eq!(
            directory.verify_object_read(&token, &test_id("other"), &Method::GET),
            Err(AuthError::NotPermitted)
        );
    }

    #[test]
    fn wrong_operation_fails_verification() {
        let directory = config_with_secrets(KID, vec![SECRET]);
        let id = test_id("foo/bar");
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(KID.to_owned());
        let token = encode(
            &header,
            &PresignedClaims {
                exp: get_current_timestamp() + 60,
                operation: "PUT".to_owned(),
                res: PresignedResource::from(&id),
            },
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();

        assert_eq!(
            directory.verify_object_read(&token, &id, &Method::GET),
            Err(AuthError::NotPermitted)
        );
    }

    #[test]
    fn expired_signature_fails_verification() {
        let directory = config_with_secrets(KID, vec![SECRET]);
        let id = test_id("foo/bar");
        let token = directory
            .sign_get(&id, get_current_timestamp() - 3600)
            .unwrap();

        let Err(AuthError::ValidationFailure(error)) =
            directory.verify_object_read(&token, &id, &Method::GET)
        else {
            panic!("presigned token must fail");
        };
        assert_eq!(
            error.kind(),
            &jsonwebtoken::errors::ErrorKind::ExpiredSignature
        );
    }

    #[test]
    fn file_secret_loads() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, SECRET.as_bytes()).unwrap();

        let config = PresignedAuth {
            signing_key_id: Some(KID.to_owned()),
            keys: BTreeMap::from([(
                KID.to_owned(),
                PresignedHmacKey {
                    secrets: vec![],
                    secret_files: vec![file.path().to_owned()],
                },
            )]),
        };
        let directory = PresignedKeyDirectory::try_from(&config).unwrap();
        let id = test_id("foo/bar");
        let token = directory
            .sign_get(&id, get_current_timestamp() + 60)
            .unwrap();

        directory
            .verify_object_read(&token, &id, &Method::GET)
            .unwrap();
    }
}
