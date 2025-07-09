use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::pin::pin;

use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt as _;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt as _, BufWriter};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::backend::Backend;

pub struct LocalFs {
    root_path: PathBuf,
}

impl LocalFs {
    pub fn new(root_path: PathBuf) -> Self {
        Self { root_path }
    }
}

impl Backend for LocalFs {
    async fn put_file(
        &self,
        path: &str,
        stream: impl Stream<Item = io::Result<Bytes>>,
    ) -> anyhow::Result<()> {
        let path = self.root_path.join(path);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await?;

        let mut reader = pin!(StreamReader::new(stream));
        let mut writer = BufWriter::new(file);

        tokio::io::copy(&mut reader, &mut writer).await?;
        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_data().await?;
        drop(file);

        Ok(())
    }

    async fn get_file(
        &self,
        path: &str,
    ) -> anyhow::Result<Option<impl Stream<Item = io::Result<Bytes>> + 'static>> {
        let path = self.root_path.join(path);
        let file = match OpenOptions::new().read(true).open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            err => err?,
        };

        let stream = ReaderStream::new(file).map(|res| Ok(res?));
        Ok(Some(stream))
    }
}
