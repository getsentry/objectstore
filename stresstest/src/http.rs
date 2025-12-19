//! Contains a remote implementation using HTTP to interact with objectstore.

use anyhow::Context;
use futures::StreamExt;
use objectstore_client::{Client, GetResponse, Session, Usecase};
use tokio::io::AsyncReadExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::workload::Payload;

/// A remote implementation using HTTP to interact with objectstore.
#[derive(Debug)]
pub struct HttpRemote {
    client: Client,
}

impl HttpRemote {
    /// Creates a new `HttpRemote` instance with the given remote URL and a default client.
    pub fn new(remote: &str) -> Self {
        Self {
            client: Client::builder(remote)
                .configure_reqwest(|r| r.no_hickory_dns())
                .build()
                .unwrap(),
        }
    }

    pub(crate) async fn write(
        &self,
        usecase: &Usecase,
        organization_id: u64,
        payload: Payload,
    ) -> anyhow::Result<String> {
        let session = self.session(usecase, organization_id);
        let stream = ReaderStream::new(payload).boxed();

        session
            .put_stream(stream)
            .compression(None)
            .send()
            .await
            .map(|r| r.key)
            .context("error writing payload")
    }

    pub(crate) async fn read(
        &self,
        usecase: &Usecase,
        organization_id: u64,
        key: &str,
        mut payload: Payload,
    ) -> anyhow::Result<()> {
        let client = self.session(usecase, organization_id);
        let GetResponse { stream, .. } = client
            .get(key)
            .send()
            .await?
            .context("expected file to exist")?;
        let mut reader = StreamReader::new(stream);

        // TODO: both of these are currently buffering in-memory. we should use streaming here as well.
        let mut read_contents = Vec::new();
        reader.read_to_end(&mut read_contents).await?;
        let mut expected_payload = Vec::new();
        payload.read_to_end(&mut expected_payload).await?;

        if read_contents != expected_payload {
            anyhow::bail!("contents of `{key}` do not match expectation");
        }
        Ok(())
    }

    pub(crate) async fn delete(
        &self,
        usecase: &Usecase,
        organization_id: u64,
        key: &str,
    ) -> anyhow::Result<()> {
        self.session(usecase, organization_id)
            .delete(key)
            .send()
            .await?;

        Ok(())
    }

    fn session(&self, usecase: &Usecase, organization_id: u64) -> Session {
        // NB: Reuse the organization ID as project ID to create unique projects. Right now, we do
        // not benefit from simulating multiple projects per org.
        usecase
            .for_project(organization_id, organization_id)
            .session(&self.client)
            .unwrap()
    }
}
