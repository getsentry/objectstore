use std::pin::pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use api::storage::storage_server::{Storage, StorageServer};
use api::storage::{GetBlobRequest, GetBlobResponse, PutBlobRequest, PutBlobResponse};
use service::StorageService;
use tokio_stream::StreamExt as _;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::config::Config;

#[derive(Debug)]
pub struct StorageServiceImpl {
    service: Arc<StorageService>,
}

impl StorageServiceImpl {
    pub async fn put_blob(&self, request: PutBlobRequest) -> Result<PutBlobResponse> {
        let scope = request.scope.context("scope is required")?;
        let key = request.key.unwrap_or_else(|| Uuid::new_v4().to_string());
        let key = format!("{}/{}/{}", scope.usecase, scope.scope, key);

        self.service.put_file(&key, &request.contents).await?;

        Ok(PutBlobResponse { key })
    }

    pub async fn get_blob(&self, request: GetBlobRequest) -> Result<GetBlobResponse> {
        let scope = request.scope.context("scope is required")?;
        let key = format!("{}/{}/{}", scope.usecase, scope.scope, request.key);

        let stream = self
            .service
            .clone()
            .get_file(&key)
            .await?
            .context("not found")?;
        let mut stream = pin!(stream);

        let mut contents = vec![];
        while let Some(chunk) = stream.next().await {
            contents.extend_from_slice(&chunk?);
        }
        Ok(GetBlobResponse { contents })
    }
}

#[tonic::async_trait]
impl Storage for StorageServiceImpl {
    async fn put_blob(
        &self,
        request: Request<PutBlobRequest>,
    ) -> Result<Response<PutBlobResponse>, Status> {
        let res = self.put_blob(request.into_inner()).await;
        res.map(Response::new)
            .map_err(|e| Status::from_error(e.into_boxed_dyn_error()))
    }
    async fn get_blob(
        &self,
        request: Request<GetBlobRequest>,
    ) -> Result<Response<GetBlobResponse>, Status> {
        let res = self.get_blob(request.into_inner()).await;
        res.map(Response::new)
            .map_err(|e| Status::from_error(e.into_boxed_dyn_error()))
    }
}

pub async fn start_server(config: Arc<Config>, service: Arc<StorageService>) {
    let server = StorageServer::new(StorageServiceImpl { service });

    println!("gRPC server listening on {}", config.grpc_addr);
    Server::builder()
        .add_service(server)
        .serve_with_shutdown(
            config.grpc_addr,
            elegant_departure::tokio::depart().on_termination(),
        )
        .await
        .unwrap();
    println!("gRPC server shut down");
}
