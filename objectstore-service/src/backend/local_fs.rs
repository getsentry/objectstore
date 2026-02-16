use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::pin::pin;

use futures_util::StreamExt;
use objectstore_types::metadata::Metadata;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::backend::common::{Backend, DeleteResponse, GetResponse, PutResponse};
use crate::id::ObjectId;
use crate::{PayloadStream, ServiceError, ServiceResult};

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

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn put_object(
        &self,
        id: &ObjectId,
        metadata: &Metadata,
        stream: PayloadStream,
    ) -> ServiceResult<PutResponse> {
        let path = self.path.join(id.as_storage_path().to_string());
        tracing::debug!(path=%path.display(), "Writing to local_fs backend");
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .await?;

        let mut reader = pin!(StreamReader::new(stream));
        let mut writer = BufWriter::new(file);

        let metadata_json =
            serde_json::to_string(metadata).map_err(|cause| ServiceError::Serde {
                context: "failed to serialize metadata".to_string(),
                cause,
            })?;
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
    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn get_object(&self, id: &ObjectId) -> ServiceResult<GetResponse> {
        tracing::debug!("Reading from local_fs backend");
        let path = self.path.join(id.as_storage_path().to_string());
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
        let metadata: Metadata =
            serde_json::from_str(metadata_line.trim_end()).map_err(|cause| {
                ServiceError::Serde {
                    context: "failed to deserialize metadata".to_string(),
                    cause,
                }
            })?;

        let stream = ReaderStream::new(reader);
        Ok(Some((metadata, stream.boxed())))
    }

    #[tracing::instrument(level = "trace", fields(?id), skip_all)]
    async fn delete_object(&self, id: &ObjectId) -> ServiceResult<DeleteResponse> {
        tracing::debug!("Deleting from local_fs backend");
        let path = self.path.join(id.as_storage_path().to_string());
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
    use std::time::{Duration, SystemTime};

    use bytes::BytesMut;
    use futures_util::TryStreamExt;
    use objectstore_types::metadata::{Compression, ExpirationPolicy};

    use crate::id::ObjectContext;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;

    fn make_stream(contents: &[u8]) -> PayloadStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    #[tokio::test]
    async fn stores_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(tempdir.path());

        let id = ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        });

        let metadata = Metadata {
            is_redirect_tombstone: None,
            content_type: "text/plain".into(),
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            time_created: Some(SystemTime::now()),
            time_expires: None,
            compression: Some(Compression::Zstd),
            origin: Some("203.0.113.42".into()),
            custom: [("foo".into(), "bar".into())].into(),
            size: None,
        };
        backend
            .put_object(&id, &metadata, make_stream(b"oh hai!"))
            .await
            .unwrap();

        let (read_metadata, stream) = backend.get_object(&id).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(read_metadata, metadata);
        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[tokio::test]
    async fn get_metadata_returns_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(tempdir.path());

        let id = ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        });

        let metadata = Metadata {
            content_type: "text/plain".into(),
            compression: Some(Compression::Zstd),
            origin: Some("203.0.113.42".into()),
            custom: [("foo".into(), "bar".into())].into(),
            ..Default::default()
        };
        backend
            .put_object(&id, &metadata, make_stream(b"oh hai!"))
            .await
            .unwrap();

        let read_metadata = backend.get_metadata(&id).await.unwrap().unwrap();
        assert_eq!(read_metadata, metadata);
    }

    #[tokio::test]
    async fn get_metadata_nonexistent() {
        let tempdir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(tempdir.path());

        let id = ObjectId::random(ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        });

        let result = backend.get_metadata(&id).await.unwrap();
        assert!(result.is_none());
    }
}
