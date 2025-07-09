mod gcs;
mod local_fs;

use bytes::Bytes;
use futures_core::Stream;
use std::io;

pub trait Backend {
    async fn put_file(
        &self,
        path: &str,
        stream: impl Stream<Item = io::Result<Bytes>>,
    ) -> anyhow::Result<()>;
    async fn get_file(
        &self,
        path: &str,
    ) -> anyhow::Result<Option<impl Stream<Item = io::Result<Bytes>> + 'static>>;
}
