use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use jsonwebtoken::errors::Result as JwtResult;
use jsonwebtoken::{DecodingKey, Validation, decode};
use objectstore_service::{Scope, Usecase};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Claim {
    pub usecase: Usecase,
    pub scope: Scope,
}

pub fn decode_auth_header(token: &str) -> JwtResult<Claim> {
    let key = DecodingKey::from_secret(b"TODO");
    let validation = Validation::default();

    let token = decode::<Claim>(token, &key, &validation)?;
    Ok(token.claims)
}

pub struct ExtractScope(pub Claim);

impl<S> FromRequestParts<S> for ExtractScope
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get(header::AUTHORIZATION)
            .ok_or((StatusCode::BAD_REQUEST, "`Authorization` header is missing"))?;
        let token = auth_header
            .to_str()
            .map_err(|_err| (StatusCode::BAD_REQUEST, "malformed `Authorization` header"))?;

        let scope = decode_auth_header(token)
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
        });

        let header = Header::default();
        let key = EncodingKey::from_secret(b"TODO");

        let token = encode(&header, &claims, &key).unwrap();
        let claims = decode_auth_header(&token).unwrap();

        assert_eq!(
            claims,
            Claim {
                usecase: Usecase::Attachments,
                scope: Scope::Organization(12345)
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
        });

        let header = Header::default();
        let key = EncodingKey::from_secret(b"TODO");

        let token = encode(&header, &claims, &key).unwrap();

        assert!(decode_auth_header(&token).is_err());
    }
}
