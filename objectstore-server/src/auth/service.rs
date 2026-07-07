use objectstore_service::id::{ObjectContext, ObjectId};
use objectstore_service::multipart::{
    AbortMultipartResponse, CompleteMultipartResponse, CompletedPart, InitiateMultipartResponse,
    ListPartsResponse, PartNumber, UploadId, UploadPartResponse,
};
use objectstore_service::service::{DeleteResponse, GetResponse, InsertResponse, MetadataResponse};
use std::sync::Arc;

use objectstore_service::{ClientStream, StorageService};
use objectstore_types::auth::Permission;
use objectstore_types::metadata::Metadata;
use objectstore_types::range::ByteRange;

use crate::auth::{AuthContext, PublicKeyDirectory};
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
    context: AuthContext,
    enforce: bool,
    key_directory: Arc<PublicKeyDirectory>,
}

impl AuthAwareService {
    /// Creates a new `AuthAwareService` using the given [`StorageService`], [`AuthContext`],
    /// enforcement setting, and key directory.
    ///
    /// The `context` is used to check whether each operation is authorized. `enforce` controls
    /// whether authorization failures will result in an error response or be ignored. The
    /// `key_directory` is used to resolve the signing key's permissions for pre-signed requests.
    ///
    /// With an [`AuthContext::Disabled`] context, all operations are permitted.
    pub fn new(
        service: StorageService,
        context: AuthContext,
        enforce: bool,
        key_directory: Arc<PublicKeyDirectory>,
    ) -> Self {
        Self {
            service,
            context,
            enforce,
            key_directory,
        }
    }

    /// Checks whether the request is authorized for the given permission on the given context.
    ///
    /// Returns `Ok(())` if authorized, or an error indicating the reason.
    pub fn check_permission(&self, perm: Permission, context: &ObjectContext) -> ApiResult<()> {
        if let Err(error) = self
            .context
            .assert_authorized(perm, context, &self.key_directory)
        {
            sentry::with_scope(
                |s| s.set_tag("perm", perm.to_string()),
                || error.log(!self.enforce),
            );

            if self.enforce {
                return Err(error.into());
            }
        }

        Ok(())
    }

    /// Auth-aware wrapper around [`StorageService::insert_object`].
    pub async fn insert_object(
        &self,
        context: ObjectContext,
        key: Option<String>,
        metadata: Metadata,
        stream: ClientStream,
    ) -> ApiResult<InsertResponse> {
        self.check_permission(Permission::ObjectWrite, &context)?;
        Ok(self
            .service
            .insert_object(context, key, metadata, stream)
            .await?)
    }

    /// Auth-aware wrapper around [`StorageService::get_metadata`].
    pub async fn get_metadata(&self, id: ObjectId) -> ApiResult<MetadataResponse> {
        self.check_permission(Permission::ObjectRead, id.context())?;
        Ok(self.service.get_metadata(id).await?)
    }

    /// Auth-aware wrapper around [`StorageService::get_object`].
    pub async fn get_object(
        &self,
        id: ObjectId,
        range: Option<ByteRange>,
    ) -> ApiResult<GetResponse> {
        self.check_permission(Permission::ObjectRead, id.context())?;
        Ok(self.service.get_object(id, range).await?)
    }

    /// Auth-aware wrapper around [`StorageService::delete_object`].
    pub async fn delete_object(&self, id: ObjectId) -> ApiResult<DeleteResponse> {
        self.check_permission(Permission::ObjectDelete, id.context())?;
        Ok(self.service.delete_object(id).await?)
    }

    // --- Multipart upload operations ---

    /// Auth-aware wrapper around [`StorageService::initiate_multipart`].
    pub async fn initiate_multipart(
        &self,
        id: ObjectId,
        metadata: Metadata,
    ) -> ApiResult<InitiateMultipartResponse> {
        self.check_permission(Permission::ObjectWrite, id.context())?;
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
        self.check_permission(Permission::ObjectWrite, id.context())?;
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
        self.check_permission(Permission::ObjectWrite, id.context())?;
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
        self.check_permission(Permission::ObjectWrite, id.context())?;
        Ok(self.service.abort_multipart(id, upload_id).await?)
    }

    /// Auth-aware wrapper around [`StorageService::complete_multipart`].
    pub async fn complete_multipart(
        &self,
        id: ObjectId,
        upload_id: UploadId,
        parts: Vec<CompletedPart>,
    ) -> ApiResult<CompleteMultipartResponse> {
        self.check_permission(Permission::ObjectWrite, id.context())?;
        Ok(self
            .service
            .complete_multipart(id, upload_id, parts)
            .await?)
    }
}
