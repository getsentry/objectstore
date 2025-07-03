use anyhow::Result;
use api::storage::storage_server::{Storage, StorageServer};
use api::storage::{AllocateBlobRequest, AllocateBlobResponse};
use service::StorageService;
use tonic::{Request, Response, Status};

use crate::config::Config;

#[derive(Debug)]
pub struct StorageServiceImpl {
    service: StorageService,
}

impl StorageServiceImpl {
    pub fn new(service: StorageService) -> Self {
        Self { service }
    }

    pub fn put_blob(&self, request: AllocateBlobRequest) -> Result<AllocateBlobResponse> {
        // Here you would handle the blob upload logic.
        println!("Received a blob upload request: {:?}", request);

        let Some(storage_id) = request.id else {
            anyhow::bail!("storage id is required");
        };

        let key = str::from_utf8(&storage_id.id)?;
        self.service.put_file(key, b"")?;

        let response = AllocateBlobResponse {
            id: Some(storage_id),
            signed_put_url: "https://localhost:5000/mocked-upload".to_owned(),
        };

        Ok(response)
    }
}

#[tonic::async_trait]
impl Storage for StorageServiceImpl {
    async fn put_blob(
        &self,
        request: Request<AllocateBlobRequest>,
    ) -> Result<Response<AllocateBlobResponse>, Status> {
        self.put_blob(request.into_inner())
            .map(Response::new)
            .map_err(|e| Status::from_error(e.into_boxed_dyn_error()))
    }
}

pub fn service(config: Config) -> Result<StorageServer<StorageServiceImpl>> {
    let service = StorageService::new(&config.path)?;
    let server = StorageServer::new(StorageServiceImpl::new(service));
    Ok(server)
}
