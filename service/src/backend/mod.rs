mod gcs;
mod local_fs;

use futures_core::stream::BoxStream;
pub use gcs::Gcs;
pub use local_fs::LocalFs;

use bytes::Bytes;
use std::io;

pub type BoxedBackend = Box<dyn Backend + Send + Sync + 'static>;

#[async_trait::async_trait]
pub trait Backend {
    async fn put_file(
        &self,
        path: &str,
        stream: BoxStream<'static, io::Result<Bytes>>,
    ) -> anyhow::Result<()>;
    async fn get_file(
        &self,
        path: &str,
    ) -> anyhow::Result<Option<BoxStream<'static, io::Result<Bytes>>>>;
}
