use axum::extract::FromRequestParts;
use axum::http::{StatusCode, header, request::Parts};
use objectstore_service::BackendStream;
use objectstore_service::{ObjectPath, StorageService};
use objectstore_types::Metadata;

use super::{AuthContext, AuthError, Permission};
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

/// Wrapper around [`objectstore_service::StorageService`] that ensures each storage operation is
/// authorized according to the request's authorization details. See also: [`AuthContext`].
///
/// When [`crate::config::AuthZ::enforce`] is false, authorization failures are logged but any
/// unauthorized operations are still allowed to proceed.
///
/// Objectstore API endpoints can use `AuthAwareService` simply by adding it to their handler
/// function's argument list like so:
/// ```no_run
/// # use axum::extract::Path;
/// # use axum::response::IntoResponse;
/// # use axum::http::StatusCode;
/// # use objectstore_server::{auth::AuthAwareService, error::ApiResult};
/// # use objectstore_service::ObjectPath;
/// async fn delete_object(
///     service: AuthAwareService,      // <- Constructed automatically from request parts
///     Path(path): Path<ObjectPath>,
/// ) -> ApiResult<impl IntoResponse> {
///     service.delete_object(&path).await?;
///
///     Ok(StatusCode::NO_CONTENT)
/// }
/// ```
pub struct AuthAwareService {
    service: StorageService,

    enforce: bool,

    auth_context: Option<AuthContext>,
}

impl AuthAwareService {
    fn assert_authorized(&self, perm: Permission, path: &ObjectPath) -> anyhow::Result<()> {
        let auth_result = self
            .auth_context
            .as_ref()
            .ok_or(AuthError::VerificationFailure)
            .and_then(|ac| ac.assert_authorized(perm, path));
        if self.enforce {
            return Ok(auth_result?);
        }
        Ok(())
    }

    /// Auth-aware wrapper around [`StorageService::put_object`].
    pub async fn put_object(
        &self,
        path: ObjectPath,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> anyhow::Result<ObjectPath> {
        self.assert_authorized(Permission::ObjectWrite, &path)?;

        self.service.put_object(path, metadata, stream).await
    }

    /// Auth-aware wrapper around [`StorageService::get_object`].
    pub async fn get_object(
        &self,
        path: &ObjectPath,
    ) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        self.assert_authorized(Permission::ObjectRead, path)?;

        self.service.get_object(path).await
    }

    /// Auth-aware wrapper around [`StorageService::delete_object`].
    pub async fn delete_object(&self, path: &ObjectPath) -> anyhow::Result<()> {
        self.assert_authorized(Permission::ObjectDelete, path)?;

        self.service.delete_object(path).await
    }
}

impl FromRequestParts<ServiceState> for AuthAwareService {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ServiceState,
    ) -> Result<Self, Self::Rejection> {
        let encoded_token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            // TODO: Handle case-insensitive bearer prefix
            .and_then(|v| v.strip_prefix(BEARER_PREFIX));

        let auth_context = AuthContext::from_encoded_jwt(encoded_token, &state.config.auth);
        if auth_context.is_err() && state.config.auth.enforce {
            tracing::debug!("Authorization failed when enforcement is enabled");
            return Err(StatusCode::UNAUTHORIZED);
        }

        Ok(AuthAwareService {
            service: state.authless_service.clone(),
            enforce: state.config.auth.enforce,
            auth_context: auth_context.ok(),
        })
    }
}
