use std::fmt::Debug;
use std::io;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use objectstore_types::Metadata;

mod bigtable;
mod gcs;
mod local_fs;
mod s3_compatible;

pub use bigtable::{BigTableBackend, BigTableConfig};
pub use gcs::gcs;
pub use local_fs::LocalFs;
pub use s3_compatible::S3Compatible;

use crate::ScopedKey;

pub type BoxedBackend = Box<dyn Backend>;
pub type BackendStream = BoxStream<'static, io::Result<Bytes>>;

#[async_trait::async_trait]
pub trait Backend: Debug + Send + Sync + 'static {
    async fn put_object(
        &self,
        key: &ScopedKey,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> anyhow::Result<()>;
    async fn get_object(
        &self,
        key: &ScopedKey,
    ) -> anyhow::Result<Option<(Metadata, BackendStream)>>;
    async fn delete_object(&self, key: &ScopedKey) -> anyhow::Result<()>;
}
