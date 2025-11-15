use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::pin::pin;

use futures_util::StreamExt;
use objectstore_types::Metadata;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::ObjectPath;
use crate::backend::common::{Backend, BackendStream};

#[derive(Debug)]
pub struct LocalFsBackend {
    path: PathBuf,
}

impl LocalFsBackend {
    pub fn new(path: &Path) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait::async_trait]
impl Backend for LocalFsBackend {
    fn name(&self) -> &'static str {
        "local-fs"
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn put_object(
        &self,
        path: &ObjectPath,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> anyhow::Result<()> {
        tracing::debug!("Writing to local_fs backend");
        let path = self.path.join(path.to_string());
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;

        let mut reader = pin!(StreamReader::new(stream));
        let mut writer = BufWriter::new(file);

        let metadata_json = serde_json::to_string(metadata)?;
        writer.write_all(metadata_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_data().await?;
        drop(file);

        Ok(())
    }

    // TODO: Return `Ok(None)` if object is found but past expiry
    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn get_object(
        &self,
        path: &ObjectPath,
    ) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        tracing::debug!("Reading from local_fs backend");
        let path = self.path.join(path.to_string());
        let file = match OpenOptions::new().read(true).open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                tracing::debug!("Object not found");
                return Ok(None);
            }
            err => err?,
        };

        let mut reader = BufReader::new(file);
        let mut metadata_line = String::new();
        reader.read_line(&mut metadata_line).await?;
        let metadata: Metadata = serde_json::from_str(metadata_line.trim_end())?;

        let stream = ReaderStream::new(reader);
        Ok(Some((metadata, stream.boxed())))
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn delete_object(&self, path: &ObjectPath) -> anyhow::Result<()> {
        tracing::debug!("Deleting from local_fs backend");
        let path = self.path.join(path.to_string());
        let result = tokio::fs::remove_file(path).await;
        if let Err(e) = &result
            && e.kind() == ErrorKind::NotFound
        {
            tracing::debug!("Object not found");
        }
        Ok(result?)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::BytesMut;
    use futures_util::TryStreamExt;
    use objectstore_types::{Compression, ExpirationPolicy};

    use super::*;

    fn make_stream(contents: &[u8]) -> BackendStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    #[tokio::test]
    async fn stores_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(tempdir.path());

        let key = ObjectPath {
            usecase: "testing".into(),
            scope: "testing".into(),
            key: "testing".into(),
        };
        let metadata = Metadata {
            is_redirect_tombstone: None,
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            compression: Some(Compression::Zstd),
            custom: [("foo".into(), "bar".into())].into(),
            size: None,
        };
        backend
            .put_object(&key, &metadata, make_stream(b"oh hai!"))
            .await
            .unwrap();

        let (read_metadata, stream) = backend.get_object(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(read_metadata, metadata);
        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }
}
