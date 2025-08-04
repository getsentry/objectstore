//! Contains a remote implementation using HTTP to interact with objectstore.

use futures::StreamExt;
use objectstore_client::{Client, ClientBuilder, Compression, GetResult};
use tokio::io::AsyncReadExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::workload::{InternalId, Payload};

/// A remote implementation using HTTP to interact with objectstore.
#[derive(Debug)]
pub struct HttpRemote {
    /// The Storage Client used to talk to our service.
    pub client: Client,
}

impl HttpRemote {
    /// Creates a new `HttpRemote` instance with the given remote URL and a default client.
    ///
    /// The JWT secret is empty and can be changed with `with_secret`.
    pub fn new(remote: &str, jwt_secret: &str) -> Self {
        let client = ClientBuilder::new(remote, jwt_secret, "stresstest")
            .unwrap()
            .for_organization(12345);
        Self { client }
    }

    pub(crate) async fn write(&self, id: InternalId, payload: Payload) -> String {
        let stream = ReaderStream::new(payload).boxed();

        self.client
            .put(id.to_string().as_str())
            .compression(Compression::Uncompressible)
            .stream(stream)
            .send()
            .await
            .unwrap()
            .key
    }

    pub(crate) async fn read(&self, key: &str, mut payload: Payload) {
        let GetResult { stream, .. } = self.client.get(key, &[]).await.unwrap().unwrap();
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

    pub(crate) async fn delete(&self, key: String) {
        self.client.delete(&key).await.unwrap();
    }
}
