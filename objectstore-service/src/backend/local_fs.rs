use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::pin::pin;

use bincode::error::DecodeError;
use futures_util::StreamExt;
use objectstore_types::Metadata;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::ObjectPath;

use super::{Backend, BackendStream};

const BC_CONFIG: bincode::config::Configuration = bincode::config::standard();

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
    async fn put_object(
        &self,
        path: &ObjectPath,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> anyhow::Result<()> {
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

        let metadata = bincode::serde::encode_to_vec(metadata, BC_CONFIG)?;
        writer.write_all(&metadata).await?;

        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_data().await?;
        drop(file);

        Ok(())
    }

    async fn get_object(
        &self,
        path: &ObjectPath,
    ) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        let path = self.path.join(path.to_string());
        let file = match OpenOptions::new().read(true).open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            err => err?,
        };

        let mut reader = BufReader::new(file);
        let mut metadata_buf = vec![];
        let metadata = loop {
            let reader_buf = reader.fill_buf().await?;
            let buf = if metadata_buf.is_empty() {
                reader_buf
            } else {
                metadata_buf.extend_from_slice(reader_buf);
                &metadata_buf
            };

            match bincode::serde::decode_from_slice(buf, BC_CONFIG) {
                Ok((metadata, read)) => {
                    let read = if metadata_buf.is_empty() {
                        read
                    } else {
                        let prev_consumed = metadata_buf.len() - reader_buf.len();
                        read - prev_consumed
                    };
                    reader.consume(read);
                    break metadata;
                }
                Err(DecodeError::UnexpectedEnd { .. }) => {
                    metadata_buf.extend_from_slice(reader_buf);
                    let len = reader_buf.len();
                    reader.consume(len);
                }
                Err(err) => Err(err)?,
            }
        };

        let stream = ReaderStream::new(reader);
        Ok(Some((metadata, stream.boxed())))
    }

    async fn delete_object(&self, path: &ObjectPath) -> anyhow::Result<()> {
        let path = self.path.join(path.to_string());
        Ok(tokio::fs::remove_file(path).await?)
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
            expiration_policy: ExpirationPolicy::TimeToIdle(Duration::from_secs(3600)),
            compression: Some(Compression::Zstd),
            custom: [("foo".into(), "bar".into())].into(),
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
