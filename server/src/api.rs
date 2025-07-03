use anyhow::{Context, Result};
use api::storage::storage_server::{Storage, StorageServer};
use api::storage::{GetBlobRequest, GetBlobResponse, PutBlobRequest, PutBlobResponse};
use service::StorageService;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::config::Config;

#[derive(Debug)]
pub struct StorageServiceImpl {
    service: StorageService,
}

impl StorageServiceImpl {
    pub fn new(service: StorageService) -> Self {
        Self { service }
    }

    pub fn put_blob(&self, request: PutBlobRequest) -> Result<PutBlobResponse> {
        let scope = request.scope.context("scope is required")?;
        let key = request.key.unwrap_or_else(|| Uuid::new_v4().to_string());
        let key = format!("{}/{}/{}", scope.usecase, scope.scope, key);

        self.service.put_file(&key, &request.contents)?;

        Ok(PutBlobResponse { key })
    }

    pub fn get_blob(&self, request: GetBlobRequest) -> Result<GetBlobResponse> {
        let scope = request.scope.context("scope is required")?;
        let key = format!("{}/{}/{}", scope.usecase, scope.scope, request.key);

        let contents = self.service.get_file(&key)?.context("not found")?;
        Ok(GetBlobResponse { contents })
    }
}

#[tonic::async_trait]
impl Storage for StorageServiceImpl {
    async fn put_blob(
        &self,
        request: Request<PutBlobRequest>,
    ) -> Result<Response<PutBlobResponse>, Status> {
        self.put_blob(request.into_inner())
            .map(Response::new)
            .map_err(|e| Status::from_error(e.into_boxed_dyn_error()))
    }
    async fn get_blob(
        &self,
        request: Request<GetBlobRequest>,
    ) -> Result<Response<GetBlobResponse>, Status> {
        self.get_blob(request.into_inner())
            .map(Response::new)
            .map_err(|e| Status::from_error(e.into_boxed_dyn_error()))
    }
}

pub fn service(config: Config) -> Result<StorageServer<StorageServiceImpl>> {
    let service = StorageService::new(&config.path)?;
    let server = StorageServer::new(StorageServiceImpl::new(service));
    Ok(server)
}
