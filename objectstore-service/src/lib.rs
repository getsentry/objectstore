//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod backend;
mod datamodel;
mod metadata;

pub use metadata::*;
use objectstore_types::Metadata;

use std::mem;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use async_compression::tokio::bufread::ZstdDecoder;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;
use uuid::Uuid;
use watto::Pod;

use crate::backend::{BackendStream, BoxedBackend};
use crate::datamodel::{
    Compression, FILE_MAGIC, FILE_VERSION, File, FilePart, PART_MAGIC, PART_VERSION, Part,
};

/// High-level asynchronous service for storing and retrieving objects.
#[derive(Clone, Debug)]
pub struct StorageService(Arc<StorageServiceInner>);

#[derive(Debug)]
struct StorageServiceInner {
    backend: BoxedBackend,
}

/// Configuration to initialize a [`StorageService`].
#[derive(Debug)]
pub enum StorageConfig<'a> {
    /// Use a local filesystem as the storage backend.
    FileSystem {
        /// The path to the directory where files will be stored.
        path: &'a Path,
    },
    /// Use an S3-compatible storage backend.
    S3Compatible {
        /// Optional endpoint URL for the S3-compatible storage.
        endpoint: Option<&'a str>,
        /// The name of the bucket to use.
        bucket: &'a str,
    },
}

impl StorageService {
    /// Creates a new `StorageService` with the specified configuration.
    pub async fn new(config: StorageConfig<'_>) -> anyhow::Result<Self> {
        let backend = match config {
            StorageConfig::FileSystem { path } => Box::new(backend::LocalFs::new(path)),
            StorageConfig::S3Compatible { endpoint, bucket } => {
                if let Some(endpoint) = endpoint {
                    Box::new(backend::S3Compatible::without_token(endpoint, bucket))
                } else {
                    backend::gcs(bucket).await?
                }
            }
        };

        let inner = StorageServiceInner { backend };
        Ok(Self(Arc::new(inner)))
    }

    /// Stores or overwrites an object at the given key.
    pub async fn put_file(
        &self,
        key: &ObjectKey,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> anyhow::Result<()> {
        self.0.backend.put_file(&key.key, metadata, stream).await
    }

    /// Streams the contents of an object stored at the given key.
    pub async fn get_file(
        &self,
        key: &ObjectKey,
    ) -> anyhow::Result<Option<(Metadata, BackendStream)>> {
        self.0.backend.get_file(&key.key).await
    }

    /// Deletes an object stored at the given key, if it exists.
    pub async fn delete_file(&self, key: &ObjectKey) -> anyhow::Result<()> {
        self.0.backend.delete_file(&key.key).await
    }

    #[doc(hidden)]
    pub async fn _put_file(&self, key: &str, contents: &[u8]) -> anyhow::Result<()> {
        let file_part = self.put_part(contents).await?;
        self.assemble_file_from_parts(key, &[file_part]).await?;

        Ok(())
    }

    #[doc(hidden)]
    pub async fn _get_file(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<impl Stream<Item = anyhow::Result<Bytes>> + use<>>> {
        let file_path = format!("files/{key}.bin");

        let Some((_metadata, stream)) = self.0.backend.get_file(&file_path).await? else {
            return Ok(None);
        };

        let mut reader = BufReader::new(StreamReader::new(stream));

        let mut metadata_buf = vec![0; mem::size_of::<Part>()];
        reader.read_exact(&mut metadata_buf).await?;
        let file_metadata = File::ref_from_bytes(&metadata_buf).context("reading File metadata")?;

        let mut parts_buf =
            vec![0; mem::size_of::<FilePart>() * file_metadata.num_parts.get() as usize];
        reader.read_exact(&mut parts_buf).await?;
        let parts =
            FilePart::slice_from_bytes(&parts_buf).context("reading File Parts metadata")?;

        Ok(Some(self.clone().make_part_stream(parts.to_vec())))
    }

    fn make_part_stream(
        self,
        parts: Vec<FilePart>,
    ) -> impl Stream<Item = anyhow::Result<Bytes>> + use<> {
        async_stream::try_stream! {
            for part in parts {
                let part_contents = self
                    .get_part(Uuid::from_bytes(part.part_uuid))
                    .await?
                    .context("part not found")?;

                yield part_contents;
            }
        }
    }

    async fn assemble_file_from_parts(&self, key: &str, parts: &[FilePart]) -> anyhow::Result<()> {
        let file_size: u64 = parts.iter().map(|part| part.part_size.get() as u64).sum();
        let file_metadata = File {
            magic: FILE_MAGIC,
            version: FILE_VERSION.into(),
            num_parts: (parts.len() as u32).into(),
            file_size: file_size.into(),
        };

        let mut buffer = file_metadata.as_bytes().to_owned();
        buffer.extend_from_slice(parts.as_bytes());

        let file_path = format!("files/{key}.bin");
        let stream = tokio_stream::once(Ok(buffer.into()));
        self.0
            .backend
            .put_file(&file_path, &Default::default(), stream.boxed())
            .await?;

        Ok(())
    }

    async fn put_part(&self, contents: &[u8]) -> anyhow::Result<FilePart> {
        let part_size = contents.len() as u32;

        let compressed = zstd::bulk::compress(contents, 0)?;
        let compressed_size = compressed.len() as u32;

        let part_metadata = Part {
            magic: PART_MAGIC,
            version: PART_VERSION.into(),
            part_size: part_size.into(),
            compression_algorithm: Compression::Zstd as u8,
            _padding: [0; 3],
            compressed_size: compressed_size.into(),
        };

        let part_uuid = Uuid::new_v4();

        let mut buffer = part_metadata.as_bytes().to_owned();
        buffer.extend_from_slice(&compressed);

        let part_path = format!("parts/{part_uuid}.bin");
        let stream = tokio_stream::once(Ok(buffer.into()));
        self.0
            .backend
            .put_file(&part_path, &Default::default(), stream.boxed())
            .await?;

        Ok(FilePart {
            part_size: part_size.into(),
            part_uuid: part_uuid.into_bytes(),
        })
    }

    async fn get_part(&self, part_uuid: Uuid) -> anyhow::Result<Option<Bytes>> {
        let part_path = format!("parts/{part_uuid}.bin");

        let Some((_metadata, stream)) = self.0.backend.get_file(&part_path).await? else {
            return Ok(None);
        };

        let mut reader = BufReader::new(StreamReader::new(stream));

        let mut metadata_buf = vec![0; mem::size_of::<Part>()];
        reader.read_exact(&mut metadata_buf).await?;
        let part_metadata = Part::ref_from_bytes(&metadata_buf).context("reading Part metadata")?;

        // TODO: verify magic, version, etc…
        let mut buf = Vec::with_capacity(part_metadata.part_size.get() as usize);
        match part_metadata.compression_algorithm {
            1 /* Zstd */ =>
            {
                let mut reader = ZstdDecoder::new(reader);
                reader.read_to_end(&mut buf).await?;
            },
            _ => {
                reader.read_to_end(&mut buf).await?;
            }
        };

        Ok(Some(buf.into()))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use futures_util::TryStreamExt;
    use objectstore_types::Scope;

    use super::*;

    fn make_stream(contents: &[u8]) -> BackendStream {
        tokio_stream::once(Ok(contents.to_vec().into())).boxed()
    }

    #[tokio::test]
    async fn stores_parts() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::FileSystem {
            path: tempdir.path(),
        };
        let service = StorageService::new(config).await.unwrap();

        let file_part = service.put_part(b"oh hai!").await.unwrap();

        let read_part = service
            .get_part(Uuid::from_bytes(file_part.part_uuid))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_part.as_bytes(), b"oh hai!");
    }

    fn make_key(key: &str) -> ObjectKey {
        ObjectKey {
            usecase: "foo".into(),
            scope: Scope {
                organization: 1234,
                project: None,
            },
            key: key.into(),
        }
    }

    #[tokio::test]
    async fn stores_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::FileSystem {
            path: tempdir.path(),
        };
        let service = StorageService::new(config).await.unwrap();
        let key = make_key("the_file_key");

        service
            .put_file(&key, &Default::default(), make_stream(b"oh hai!"))
            .await
            .unwrap();

        let (_metadata, stream) = service.get_file(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[tokio::test]
    async fn assembles_file_from_parts() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = StorageConfig::FileSystem {
            path: tempdir.path(),
        };
        let service = StorageService::new(config).await.unwrap();

        let part1 = service.put_part(b"oh ").await.unwrap();
        let part2 = service.put_part(b"hai!").await.unwrap();

        service
            .assemble_file_from_parts("the_file_key", &[part1, part2])
            .await
            .unwrap();

        let stream = service._get_file("the_file_key").await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[ignore = "gcs credentials are not yet set up in CI"]
    #[tokio::test]
    async fn works_with_gcs() {
        let config = StorageConfig::S3Compatible {
            endpoint: None,
            bucket: "sbx-warp-benchmark-bucket",
        };
        let service = StorageService::new(config).await.unwrap();
        let key = make_key("the_file_key");

        service
            .put_file(&key, &Default::default(), make_stream(b"oh hai!"))
            .await
            .unwrap();

        let (_metadata, stream) = service.get_file(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }

    #[ignore = "seadweedfs is not yet set up in CI"]
    #[tokio::test]
    async fn works_with_seaweed() {
        let config = StorageConfig::S3Compatible {
            endpoint: Some("http://localhost:8333"),
            bucket: "whatever",
        };
        let service = StorageService::new(config).await.unwrap();
        let key = make_key("the_file_key");

        service
            .put_file(&key, &Default::default(), make_stream(b"oh hai!"))
            .await
            .unwrap();

        let (_metadata, stream) = service.get_file(&key).await.unwrap().unwrap();
        let file_contents: BytesMut = stream.try_collect().await.unwrap();

        assert_eq!(file_contents.as_ref(), b"oh hai!");
    }
}
