use axum::extract::FromRequestParts;
use axum::http::{StatusCode, header, request::Parts};
use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::{PayloadStream, StorageService};
use objectstore_types::{Metadata, Permission};

use crate::auth::{AuthContext, AuthError};
use crate::state::ServiceState;

const BEARER_PREFIX: &str = "Bearer ";

/// Wrapper around [`StorageService`] that ensures each operation is authorized.
///
/// Authorization is performed according to the request's authorization details, see also
/// [`AuthContext`]. When [`crate::config::AuthZ::enforce`] is false, authorization failures are
/// logged but any unauthorized operations are still allowed to proceed.
///
/// Objectstore API endpoints can use `AuthAwareService` simply by adding it to their handler
/// function's argument list like so:
///
/// ```
/// use axum::http::StatusCode;
/// use objectstore_server::auth::AuthAwareService;
/// use objectstore_server::error::ApiResult;
///
/// async fn my_endpoint(service: AuthAwareService) -> Result<StatusCode, StatusCode> {
///     service.delete_object(todo!("pass some ID"))
///         .await
///         .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
///
///     Ok(StatusCode::NO_CONTENT)
/// }
/// ```
#[derive(Debug)]
pub struct AuthAwareService {
    service: StorageService,
    context: Option<AuthContext>,
    enforce: bool,
}

impl AuthAwareService {
    fn assert_authorized(&self, perm: Permission, context: &ObjectContext) -> anyhow::Result<()> {
        if self.enforce {
            let auth = self
                .context
                .as_ref()
                .ok_or(AuthError::VerificationFailure)?;
            auth.assert_authorized(perm, context)?;
        }

        Ok(())
    }

    /// Auth-aware wrapper around [`StorageService::insert_object`].
    pub async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: &Metadata,
        stream: PayloadStream,
    ) -> anyhow::Result<ObjectId> {
        self.assert_authorized(Permission::ObjectWrite, &context)?;
        self.service
            .insert_object(context, key, metadata, stream)
            .await
    }

    /// Auth-aware wrapper around [`StorageService::get_object`].
    pub async fn get_object(
        &self,
        id: &ObjectId,
    ) -> anyhow::Result<Option<(Metadata, PayloadStream)>> {
        self.assert_authorized(Permission::ObjectRead, id.context())?;
        self.service.get_object(id).await
    }

    /// Auth-aware wrapper around [`StorageService::delete_object`].
    pub async fn delete_object(&self, id: &ObjectId) -> anyhow::Result<()> {
        self.assert_authorized(Permission::ObjectDelete, id.context())?;
        self.service.delete_object(id).await
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

        let context = AuthContext::from_encoded_jwt(encoded_token, &state.config.auth);
        if context.is_err() && state.config.auth.enforce {
            tracing::debug!(
                "Authorization failed and enforcement is enabled: `{:?}`",
                context
            );
            return Err(StatusCode::UNAUTHORIZED);
        }

        Ok(AuthAwareService {
            service: state.service.clone(),
            enforce: state.config.auth.enforce,
            context: context.ok(),
        })
    }
}
