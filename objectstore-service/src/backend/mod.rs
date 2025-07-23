mod gcs;
mod local_fs;
mod s3_compatible;

use std::fmt::Debug;

use futures_core::stream::BoxStream;
pub use gcs::gcs;
pub use local_fs::LocalFs;
pub use s3_compatible::S3Compatible;

use bytes::Bytes;

pub type BoxedBackend = Box<dyn Backend>;
pub type BackendStream = BoxStream<'static, anyhow::Result<Bytes>>;

#[async_trait::async_trait]
pub trait Backend: Debug + Send + Sync + 'static {
    async fn put_file(&self, path: &str, stream: BackendStream) -> anyhow::Result<()>;
    async fn get_file(&self, path: &str) -> anyhow::Result<Option<BackendStream>>;
    async fn delete_file(&self, path: &str) -> anyhow::Result<()>;
}
