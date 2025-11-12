use std::collections::HashSet;

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use jsonwebtoken::{DecodingKey, Validation, decode, decode_header};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};

use crate::config::AuthZ;
use crate::state::ServiceState;

#[inline]
fn log_auth_failure(enforce: bool, msg: &str) {
    if enforce {
        tracing::error!(msg);
    } else {
        tracing::warn!(msg);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Claims {
    scope: String,
    permissions: HashSet<Permission>,
}

#[derive(Debug, Clone)]
pub struct AuthContext {
    /// TODO should be ordered BTreeMap
    pub scope: String,

    /// TODO maybe make into bitmask
    pub permissions: HashSet<Permission>,
}

fn verify_token(encoded_token: Option<&str>, config: &AuthZ) -> Option<AuthContext> {
    let Some(encoded_token) = encoded_token else {
        log_auth_failure(
            config.enforce,
            "Authorization header required but not found",
        );
        return None;
    };

    let Ok(jwt_header) = decode_header(encoded_token) else {
        log_auth_failure(config.enforce, "Authorization header is not valid JWT");
        return None;
    };

    let Some(key_id) = jwt_header.kid.as_ref() else {
        log_auth_failure(config.enforce, "JWT header is missing `kid` field");
        return None;
    };

    let Some(key_config) = config.keys.get(key_id) else {
        log_auth_failure(
            config.enforce,
            &format!("Key {key_id} not configured in service"),
        );
        return None;
    };

    let mut verified_claims: Option<jsonwebtoken::TokenData<Claims>> = None;
    for key in &key_config.key_versions {
        verified_claims = match decode::<Claims>(
            encoded_token,
            &DecodingKey::from_secret(key.expose_secret().as_bytes()),
            &Validation::default(),
        ) {
            Ok(claims) => Some(claims),
            Err(_) => continue,
        }
    }
    let Some(verified_claims) = verified_claims else {
        log_auth_failure(
            config.enforce,
            "Failed to verify JWT with any configured keys",
        );
        return None;
    };

    Some(AuthContext {
        scope: verified_claims.claims.scope,
        permissions: verified_claims
            .claims
            .permissions
            .intersection(&key_config.max_permissions)
            .cloned()
            .collect(),
    })
}

pub async fn authorize(
    State(state): State<ServiceState>,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let encoded_token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok());

    let auth_context = verify_token(encoded_token, &state.config.auth);
    if state.config.auth.enforce && auth_context.is_none() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Provide auth context to endpoints
    request.extensions_mut().insert(auth_context);

    // Continue processing the request
    Ok(next.run(request).await)
}

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
