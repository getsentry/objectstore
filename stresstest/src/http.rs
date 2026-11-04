//! Contains a remote implementation using HTTP to interact with objectstore.

use anyhow::Context;
use futures::StreamExt;
use objectstore_client::{Client, GetResponse, Session, TokenGenerator, Usecase};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::workload::Payload;

/// Size of the chunks in which [`contents_match`] compares contents.
///
/// This must be a multiple of 8, since [`Payload`] generates its contents in 8-byte words and
/// would otherwise produce different bytes depending on how it is read.
const COMPARE_CHUNK_SIZE: usize = 64 * 1024;
const _: () = assert!(COMPARE_CHUNK_SIZE.is_multiple_of(8));

/// A remote implementation using HTTP to interact with objectstore.
#[derive(Debug)]
pub struct HttpRemote {
    client: Client,
}

impl HttpRemote {
    /// Creates a new `HttpRemote` instance with the given remote URL and optional token generator.
    pub fn new(remote: &str, token: Option<TokenGenerator>) -> Self {
        Self {
            client: Client::builder(remote)
                .configure_reqwest(|r| r.no_hickory_dns())
                .token(token)
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
            .compress(None)
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
        payload: Payload,
    ) -> anyhow::Result<()> {
        let client = self.session(usecase, organization_id);
        let GetResponse { stream, .. } = client
            .get(key)
            .send()
            .await?
            .context("expected file to exist")?;
        let reader = StreamReader::new(stream);

        if !contents_match(reader, payload).await? {
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

    pub(crate) fn session(&self, usecase: &Usecase, organization_id: u64) -> Session {
        // NB: Reuse the organization ID as project ID to create unique projects. Right now, we do
        // not benefit from simulating multiple projects per org.
        usecase
            .for_project(organization_id, organization_id)
            .session(&self.client)
            .unwrap()
    }
}

/// Returns whether `actual` yields exactly the contents of `expected`.
///
/// Both sides are compared chunk by chunk, so memory usage stays bounded regardless of the
/// payload size.
async fn contents_match(
    mut actual: impl AsyncRead + Unpin,
    mut expected: Payload,
) -> std::io::Result<bool> {
    // Small payloads don't need full-size chunks. Keep at least 8 bytes for the trailing data check.
    let chunk_size = expected
        .len
        .next_multiple_of(8)
        .clamp(8, COMPARE_CHUNK_SIZE as u64) as usize;
    let mut expected_chunk = vec![0; chunk_size];
    let mut actual_chunk = vec![0; chunk_size];

    loop {
        // `Payload` always fills the buffer completely until it is exhausted.
        let len = expected.read(&mut expected_chunk).await?;
        if len == 0 {
            break;
        }

        match actual.read_exact(&mut actual_chunk[..len]).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }

        if actual_chunk[..len] != expected_chunk[..len] {
            return Ok(false);
        }
    }

    // `actual` must not contain any trailing data beyond the expected payload.
    Ok(actual.read(&mut actual_chunk[..1]).await? == 0)
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    use super::*;

    fn payload(len: u64) -> Payload {
        Payload {
            len,
            rng: SmallRng::seed_from_u64(42),
        }
    }

    async fn payload_bytes(len: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        payload(len).read_to_end(&mut bytes).await.unwrap();
        bytes
    }

    #[tokio::test]
    async fn contents_match_identical() {
        let len = 3 * COMPARE_CHUNK_SIZE as u64 + 13;
        let bytes = payload_bytes(len).await;
        assert!(
            contents_match(bytes.as_slice(), payload(len))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn contents_match_empty() {
        assert!(contents_match([].as_slice(), payload(0)).await.unwrap());
    }

    #[tokio::test]
    async fn contents_match_truncated() {
        let len = COMPARE_CHUNK_SIZE as u64 + 13;
        let bytes = payload_bytes(len).await;
        let truncated = &bytes[..bytes.len() - 1];
        assert!(!contents_match(truncated, payload(len)).await.unwrap());
    }

    #[tokio::test]
    async fn contents_match_trailing_data() {
        let len = COMPARE_CHUNK_SIZE as u64 + 13;
        let mut bytes = payload_bytes(len).await;
        bytes.push(0);
        assert!(
            !contents_match(bytes.as_slice(), payload(len))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn contents_match_differing() {
        let len = COMPARE_CHUNK_SIZE as u64 + 13;
        let mut bytes = payload_bytes(len).await;
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(
            !contents_match(bytes.as_slice(), payload(len))
                .await
                .unwrap()
        );
    }
}
