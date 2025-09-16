//! Contains a remote implementation using HTTP to interact with objectstore.

use std::collections::BTreeMap;

use futures::StreamExt;
use objectstore_client::{Client, ClientBuilder, GetResult};
use tokio::io::AsyncReadExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::workload::Payload;

/// A remote implementation using HTTP to interact with objectstore.
#[derive(Debug)]
pub struct HttpRemote {
    remote: String,
    builders: BTreeMap<String, ClientBuilder>,
}

impl HttpRemote {
    /// Creates a new `HttpRemote` instance with the given remote URL and a default client.
    pub fn new(remote: &str) -> Self {
        Self {
            remote: remote.to_owned(),
            builders: BTreeMap::new(),
        }
    }

    pub(crate) async fn write(
        &self,
        usecase: &str,
        organization_id: u64,
        payload: Payload,
    ) -> String {
        let client = self.client(usecase, organization_id);
        let stream = ReaderStream::new(payload).boxed();

        client
            .put_stream(stream)
            .compression(None)
            .send()
            .await
            .unwrap()
            .key
    }

    pub(crate) async fn read(
        &self,
        usecase: &str,
        organization_id: u64,
        key: &str,
        mut payload: Payload,
    ) {
        let client = self.client(usecase, organization_id);
        let GetResult { stream, .. } = client.get(key).send().await.unwrap().unwrap();
        let mut reader = StreamReader::new(stream);

        // TODO: both of these are currently buffering in-memory. we should use streaming here as well.
        let mut read_contents = Vec::new();
        reader.read_to_end(&mut read_contents).await.unwrap();
        let mut expected_payload = Vec::new();
        payload.read_to_end(&mut expected_payload).await.unwrap();

        if read_contents != expected_payload {
            eprintln!("contents of `{key}` do not match expectation");
        }
    }

    pub(crate) async fn delete(&self, usecase: &str, organization_id: u64, key: &str) {
        let client = self.client(usecase, organization_id);
        client.delete(key).await.unwrap();
    }

    /// Registers a new usecase that can be used by the workloads.
    pub fn register_usecase(&mut self, usecase: &str) {
        let builder = ClientBuilder::new(&self.remote, usecase).unwrap();
        self.builders.insert(usecase.to_owned(), builder);
    }

    fn client(&self, usecase: &str, organization_id: u64) -> Client {
        // NB: Reuse the organization ID as project ID to create unique projects. Right now, we do
        // not benefit from simulating multiple projects per org.
        self.builders[usecase].for_project(organization_id, organization_id)
    }
}
