//! The storage server component.
//!
//! This builds on top of the [`service`], and exposes the underlying
//! storage layer as both a `gRPC` service for use by the `client`, as well as
//! an `HTTP` layer which can serve files directly to *external clients*.

use proto_codegen::storage::{AllocateBlobRequest, AllocateBlobResponse, StorageId};
use tokio::signal::unix::SignalKind;
use tonic::{Request, Response, Status, transport::Server};

use proto_codegen::storage::storage_server::{Storage, StorageServer};

#[derive(Debug, Default)]
pub struct MockStorage {}

#[tonic::async_trait]
impl Storage for MockStorage {
    async fn put_blob(
        &self,
        request: Request<AllocateBlobRequest>,
    ) -> Result<Response<AllocateBlobResponse>, Status> {
        // Here you would handle the blob upload logic.
        println!("Received a blob upload request: {:?}", request);

        let storage_id = StorageId {
            id: "usecase/4711.0001".to_owned().into_bytes(),
        };

        let response = AllocateBlobResponse {
            id: Some(storage_id),
            signed_put_url: "https://localhost:5000/mocked-upload".to_owned(),
        };

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse().unwrap();
    let storage = MockStorage::default();

    println!("Server listening on {addr}");

    let shutdown = elegant_departure::tokio::depart()
        .on_termination()
        .on_sigint()
        .on_signal(SignalKind::hangup())
        .on_signal(SignalKind::quit());

    Server::builder()
        .add_service(StorageServer::new(storage))
        .serve_with_shutdown(addr, shutdown)
        .await?;

    Ok(())
}
