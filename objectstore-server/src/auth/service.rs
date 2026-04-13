use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::service::{DeleteResponse, GetResponse, InsertResponse, MetadataResponse};
use objectstore_service::{ClientStream, StorageService};
use objectstore_types::auth::Permission;
use objectstore_types::metadata::Metadata;

use crate::auth::{AuthContext, AuthError};
use crate::endpoints::common::ApiResult;

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
    /// Creates a new `AuthAwareService` using the given [`StorageService`], [`AuthContext`], and
    /// enforcement setting.
    ///
    /// If enforcement is enabled, an `AuthContext` must be provided and its checks must succeed
    /// for an operation to be permitted.
    ///
    /// If enforcement is disabled, an `AuthContext` is not required. If one is provided, its
    /// checks will be run but their results ignored. All operations will be permitted.
    pub fn new(
        service: StorageService,
        context: Option<AuthContext>,
        enforce: bool,
    ) -> ApiResult<Self> {
        if enforce && context.is_none() {
            let err = AuthError::InternalError("Missing auth context".into());
            err.log(None, None, enforce);
            Err(err.into())
        } else {
            Ok(Self {
                service,
                context,
                enforce,
            })
        }
    }

    fn check_auth_result(
        &self,
        auth_result: Result<(), AuthError>,
        perm: Permission,
        usecase: &str,
    ) -> ApiResult<()> {
        let auth_result =
            auth_result.inspect_err(|err| err.log(Some(perm), Some(usecase), self.enforce));

        match self.enforce {
            true => Ok(auth_result?),
            false => Ok(()),
        }
    }

    /// Checks whether the request is authorized for the given permission on the given context.
    ///
    /// Object-bound auth contexts, such as pre-signed URLs, are rejected here because they must
    /// not be widened into context-level authorization.
    pub fn check_context_permission(
        &self,
        perm: Permission,
        context: &ObjectContext,
    ) -> ApiResult<()> {
        let auth_result = match &self.context {
            Some(auth) => auth.assert_context_authorized(perm, context),
            None => Ok(()),
        };

        self.check_auth_result(auth_result, perm, &context.usecase)
    }

    /// Checks whether the request is authorized for the given permission on the given object.
    ///
    /// Object-bound auth contexts authorize only exact object matches here. Scope-bound auth
    /// contexts fall back to their usecase/scope grant.
    pub fn check_object_permission(&self, perm: Permission, id: &ObjectId) -> ApiResult<()> {
        let auth_result = match &self.context {
            Some(auth) => auth.assert_object_authorized(perm, id),
            None => Ok(()),
        };

        self.check_auth_result(auth_result, perm, id.usecase())
    }

    /// Auth-aware wrapper around [`StorageService::insert_object`].
    pub async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: Metadata,
        stream: ClientStream,
    ) -> ApiResult<InsertResponse> {
        match &key {
            Some(key) => {
                let id = ObjectId::new(context.clone(), key.clone());
                self.check_object_permission(Permission::ObjectWrite, &id)?;
            }
            None => self.check_context_permission(Permission::ObjectWrite, &context)?,
        }

        Ok(self
            .service
            .insert_object(context, key, metadata, stream)
            .await?)
    }

    /// Auth-aware wrapper around [`StorageService::get_metadata`].
    pub async fn get_metadata(&self, id: ObjectId) -> ApiResult<MetadataResponse> {
        self.check_object_permission(Permission::ObjectRead, &id)?;
        Ok(self.service.get_metadata(id).await?)
    }

    /// Auth-aware wrapper around [`StorageService::get_object`].
    pub async fn get_object(&self, id: ObjectId) -> ApiResult<GetResponse> {
        self.check_object_permission(Permission::ObjectRead, &id)?;
        Ok(self.service.get_object(id).await?)
    }

    /// Auth-aware wrapper around [`StorageService::delete_object`].
    pub async fn delete_object(&self, id: ObjectId) -> ApiResult<DeleteResponse> {
        self.check_object_permission(Permission::ObjectDelete, &id)?;
        Ok(self.service.delete_object(id).await?)
    }
}
