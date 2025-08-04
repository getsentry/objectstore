use std::collections::BTreeSet;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use jsonwebtoken::errors::Result as JwtResult;
use jsonwebtoken::{DecodingKey, Validation, decode};
use objectstore_service::ObjectKey;
use objectstore_types::Scope;
use serde::Deserialize;

use crate::state::ServiceState;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    Read,
    Write,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Claim {
    pub usecase: String,
    pub scope: Scope,
    // TODO: a bitfield or something else would surely be more efficient
    pub permissions: BTreeSet<Permission>,
}

impl Claim {
    pub fn into_key(self, key: String) -> ObjectKey {
        ObjectKey {
            usecase: self.usecase,
            scope: self.scope,
            key,
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn ensure_permission(&self, permission: Permission) -> Result<(), Response> {
        if !self.permissions.contains(&permission) {
            return Err((
                StatusCode::FORBIDDEN,
                "no permission to perform this action",
            )
                .into_response());
        }
        Ok(())
    }
}

pub fn decode_auth_header(token: &str, secret: &[u8]) -> JwtResult<Claim> {
    let key = DecodingKey::from_secret(secret);
    let validation = Validation::default();

    let token = decode::<Claim>(token, &key, &validation)?;
    Ok(token.claims)
}

pub struct ExtractScope(pub Claim);

impl FromRequestParts<ServiceState> for ExtractScope {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get(header::AUTHORIZATION)
            .ok_or((StatusCode::BAD_REQUEST, "`Authorization` header is missing"))?;
        let token = auth_header
            .to_str()
            .map_err(|_err| (StatusCode::BAD_REQUEST, "malformed `Authorization` header"))?;

        let scope = decode_auth_header(token, state.config.jwt_secret.as_bytes())
            .map_err(|_err| (StatusCode::UNAUTHORIZED, "invalid `Authorization`"))?;
        Ok(ExtractScope(scope))
    }
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::{EncodingKey, Header, encode};

    use super::*;

    #[test]
    fn validates_token() {
        let claims = serde_json::json!({
            "exp": jsonwebtoken::get_current_timestamp() ,
            "usecase": "attachments",
            "scope": {
                "organization": 12345,
            },
            "permissions": ["read"],
        });

        let header = Header::default();
        let key = EncodingKey::from_secret(b"KEY");

        let token = encode(&header, &claims, &key).unwrap();

        assert!(decode_auth_header(&token, b"WRONG").is_err());
        let claims = decode_auth_header(&token, b"KEY").unwrap();

        assert_eq!(
            claims,
            Claim {
                usecase: "attachments".into(),
                scope: Scope {
                    organization: 12345,
                    project: None
                },
                permissions: [Permission::Read].into()
            }
        );
    }

    #[test]
    fn rejects_expired_token() {
        let claims = serde_json::json!({
            "exp": jsonwebtoken::get_current_timestamp() - 100,
            "usecase": "attachments",
            "scope": {
                "project": 23456,
            },
            "permissions": ["write"],
        });

        let header = Header::default();
        let key = EncodingKey::from_secret(b"KEY");

        let token = encode(&header, &claims, &key).unwrap();

        assert!(decode_auth_header(&token, b"KEY").is_err());
    }
}
