use std::fmt::Debug;
use std::io;

use bytes::Bytes;
use futures_util::stream::BoxStream;

mod gcs;
mod local_fs;
mod s3_compatible;
pub use gcs::gcs;
pub use local_fs::LocalFs;
use objectstore_types::Metadata;
pub use s3_compatible::S3Compatible;

pub type BoxedBackend = Box<dyn Backend>;
pub type BackendStream = BoxStream<'static, io::Result<Bytes>>;

#[async_trait::async_trait]
pub trait Backend: Debug + Send + Sync + 'static {
    async fn put_file(
        &self,
        path: &str,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> anyhow::Result<()>;
    async fn get_file(&self, path: &str) -> anyhow::Result<Option<(Metadata, BackendStream)>>;
    async fn delete_file(&self, path: &str) -> anyhow::Result<()>;
}
