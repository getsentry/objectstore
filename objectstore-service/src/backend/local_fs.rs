use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::pin::pin;

use futures_util::{StreamExt, TryStreamExt};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::io::{ReaderStream, StreamReader};

use super::{Backend, BackendStream};

#[derive(Debug)]
pub struct LocalFs {
    path: PathBuf,
}

impl LocalFs {
    pub fn new(path: &Path) -> Self {
        Self { path: path.into() }
    }
}

#[async_trait::async_trait]
impl Backend for LocalFs {
    async fn put_file(&self, path: &str, stream: BackendStream) -> anyhow::Result<()> {
        let path = self.path.join(path);
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await?;

        let stream = stream.map_err(std::io::Error::other);
        let mut reader = pin!(StreamReader::new(stream));
        let mut writer = BufWriter::new(file);

        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_data().await?;
        drop(file);

        Ok(())
    }

    async fn get_file(&self, path: &str) -> anyhow::Result<Option<BackendStream>> {
        let path = self.path.join(path);
        let file = match OpenOptions::new().read(true).open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            err => err?,
        };

        let stream = ReaderStream::new(file).map_err(anyhow::Error::from);
        Ok(Some(stream.boxed()))
    }

    async fn delete_file(&self, path: &str) -> anyhow::Result<()> {
        let path = self.path.join(path);
        Ok(tokio::fs::remove_file(path).await?)
    }
}
