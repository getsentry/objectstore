use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
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

    fn assert_authorized(&self, perm: Permission, context: &ObjectContext) -> ApiResult<()> {
        let auth_result = match &self.context {
            Some(auth) => auth.assert_authorized(perm, context),
            None => Ok(()),
        }
        .inspect_err(|err| err.log(Some(perm), Some(context.usecase.as_str()), self.enforce));

        match self.enforce {
            true => Ok(auth_result?),
            false => Ok(()),
        }
    }

    /// Checks whether the request is authorized for the given permission on the given context.
    ///
    /// Returns `Ok(())` if authorized, or otherwise an error indicating the reason.
    /// Equivalent to the internal `assert_authorized` check but exposed for callers
    /// that validate operations individually before delegating to a lower-level service.
    pub fn check_permission(&self, perm: Permission, context: &ObjectContext) -> ApiResult<()> {
        self.assert_authorized(perm, context)
    }

    /// Auth-aware wrapper around [`StorageService::insert_object`].
    pub async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: Metadata,
        stream: ClientStream,
    ) -> ApiResult<InsertResponse> {
        self.assert_authorized(Permission::ObjectWrite, &context)?;
        Ok(self
            .service
            .insert_object(context, key, metadata, stream)
            .await?)
    }

    /// Auth-aware wrapper around [`StorageService::get_metadata`].
    pub async fn get_metadata(&self, id: ObjectId) -> ApiResult<MetadataResponse> {
        self.assert_authorized(Permission::ObjectRead, id.context())?;
        Ok(self.service.get_metadata(id).await?)
    }

    /// Auth-aware wrapper around [`StorageService::get_object`].
    pub async fn get_object(&self, id: ObjectId) -> ApiResult<GetResponse> {
        self.assert_authorized(Permission::ObjectRead, id.context())?;
        Ok(self.service.get_object(id).await?)
    }

    /// Auth-aware wrapper around [`StorageService::delete_object`].
    pub async fn delete_object(&self, id: ObjectId) -> ApiResult<DeleteResponse> {
        self.assert_authorized(Permission::ObjectDelete, id.context())?;
        Ok(self.service.delete_object(id).await?)
    }

    // --- Multipart upload operations ---

    /// Auth-aware wrapper around [`StorageService::initiate_multipart`].
    pub async fn initiate_multipart(
        &self,
        id: ObjectId,
        metadata: Metadata,
    ) -> ApiResult<InitiateMultipartResponse> {
        self.assert_authorized(Permission::ObjectWrite, id.context())?;
        Ok(self.service.initiate_multipart(id, metadata).await?)
    }

    /// Auth-aware wrapper around [`StorageService::upload_part`].
    pub async fn upload_part(
        &self,
        id: ObjectId,
        upload_id: UploadId,
        part_number: PartNumber,
        content_length: u64,
        content_md5: Option<String>,
        body: ClientStream,
    ) -> ApiResult<UploadPartResponse> {
        self.assert_authorized(Permission::ObjectWrite, id.context())?;
        Ok(self
            .service
            .upload_part(
                id,
                upload_id,
                part_number,
                content_length,
                content_md5,
                body,
            )
            .await?)
    }

    /// Auth-aware wrapper around [`StorageService::list_parts`].
    pub async fn list_parts(
        &self,
        id: ObjectId,
        upload_id: UploadId,
        max_parts: Option<u32>,
        part_number_marker: Option<PartNumber>,
    ) -> ApiResult<ListPartsResponse> {
        self.assert_authorized(Permission::ObjectRead, id.context())?;
        Ok(self
            .service
            .list_parts(id, upload_id, max_parts, part_number_marker)
            .await?)
    }

    /// Auth-aware wrapper around [`StorageService::abort_multipart`].
    pub async fn abort_multipart(
        &self,
        id: ObjectId,
        upload_id: UploadId,
    ) -> ApiResult<AbortMultipartResponse> {
        self.assert_authorized(Permission::ObjectWrite, id.context())?;
        Ok(self.service.abort_multipart(id, upload_id).await?)
    }

    /// Auth-aware wrapper around [`StorageService::complete_multipart`].
    pub async fn complete_multipart(
        &self,
        id: ObjectId,
        upload_id: UploadId,
        parts: Vec<CompletedPart>,
    ) -> ApiResult<CompleteMultipartResponse> {
        self.assert_authorized(Permission::ObjectWrite, id.context())?;
        Ok(self
            .service
            .complete_multipart(id, upload_id, parts)
            .await?)
    }
}
