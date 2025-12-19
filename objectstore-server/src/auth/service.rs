use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::{
    DeleteResult, GetResult, InsertResult, InsertStream, PayloadStream, StorageService,
};
use objectstore_types::{Metadata, Permission};

use crate::auth::AuthContext;

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
}

impl AuthAwareService {
    /// Creates a new `AuthAwareService` using the given service and auth context.
    ///
    /// If no auth context is provided, authorization is disabled and all operations will be
    /// permitted.
    pub fn new(service: StorageService, context: Option<AuthContext>) -> Self {
        Self { service, context }
    }

    fn assert_authorized(&self, perm: Permission, context: &ObjectContext) -> anyhow::Result<()> {
        if let Some(auth) = &self.context {
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
    ) -> InsertResult {
        self.assert_authorized(Permission::ObjectWrite, &context)?;
        self.service
            .insert_object(context, key, metadata, stream)
            .await
    }

    /// Auth-aware wrapper around [`StorageService::get_object`].
    pub async fn get_object(&self, id: &ObjectId) -> GetResult {
        self.assert_authorized(Permission::ObjectRead, id.context())?;
        self.service.get_object(id).await
    }

    /// Auth-aware wrapper around [`StorageService::delete_object`].
    pub async fn delete_object(&self, id: &ObjectId) -> DeleteResult {
        self.assert_authorized(Permission::ObjectDelete, id.context())?;
        self.service.delete_object(id).await
    }
}
