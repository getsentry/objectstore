//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.

mod datamodel;

use std::io::ErrorKind;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use async_compression::tokio::bufread::ZstdDecoder;
use bytes::Bytes;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, BufReader, BufWriter};
use tokio_stream::Stream;
use uuid::Uuid;

use watto::Pod;

use crate::datamodel::{
    Compression, FILE_MAGIC, FILE_VERSION, File, FilePart, PART_MAGIC, PART_VERSION, Part,
};

#[derive(Debug)]
pub struct StorageService {
    file_path: PathBuf,
    part_path: PathBuf,
}

impl StorageService {
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        let file_path = path.join("files");
        let part_path = path.join("parts");

        std::fs::create_dir_all(&file_path)?;
        std::fs::create_dir_all(&part_path)?;

        Ok(Self {
            file_path,
            part_path,
        })
    }

    pub async fn put_file(&self, key: &str, contents: &[u8]) -> anyhow::Result<()> {
        let file_part = self.put_part(contents).await?;
        self.assemble_file_from_parts(key, &[file_part]).await?;

        Ok(())
    }

    pub async fn get_file(
        self: Arc<Self>,
        key: &str,
    ) -> anyhow::Result<Option<impl Stream<Item = anyhow::Result<Bytes>> + use<>>> {
        let file_path = self.file_path.join(format!("{key}.bin"));
        let file_file = match OpenOptions::new().read(true).open(file_path).await {
            Ok(file_file) => file_file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            err => err?,
        };

        let mut reader = BufReader::new(file_file);

        let mut metadata_buf = vec![0; mem::size_of::<Part>()];
        reader.read_exact(&mut metadata_buf).await?;
        let file_metadata = File::ref_from_bytes(&metadata_buf).context("reading File metadata")?;

        let mut parts_buf =
            vec![0; mem::size_of::<FilePart>() * file_metadata.num_parts.get() as usize];
        reader.read_exact(&mut parts_buf).await?;
        let parts =
            FilePart::slice_from_bytes(&parts_buf).context("reading File Parts metadata")?;
        let parts = parts.to_vec();

        let stream = async_stream::try_stream! {
            for part in parts {
                let part_contents = self
                    .get_part(Uuid::from_bytes(part.part_uuid))
                    .await?
                    .context("part not found")?;

                yield part_contents;
            }
        };

        Ok(Some(stream))
    }

    async fn assemble_file_from_parts(&self, key: &str, parts: &[FilePart]) -> anyhow::Result<()> {
        let file_size: u64 = parts.iter().map(|part| part.part_size.get() as u64).sum();

        let file_metadata = File {
            magic: FILE_MAGIC,
            version: FILE_VERSION.into(),
            num_parts: (parts.len() as u32).into(),
            file_size: file_size.into(),
        };

        let file_path = self.file_path.join(format!("{key}.bin"));
        tokio::fs::create_dir_all(file_path.parent().unwrap()).await?;
        let file_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(file_path)
            .await?;

        let mut writer = BufWriter::new(file_file);

        writer.write_all(file_metadata.as_bytes()).await?;
        writer.write_all(parts.as_bytes()).await?;
        writer.flush().await?;

        let part_file = writer.into_inner();
        // part_file.sync_data().await?;
        drop(part_file);

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
        let part_path = self.part_path.join(format!("{part_uuid}.bin"));
        let part_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(part_path)
            .await?;

        let mut writer = BufWriter::new(part_file);

        writer.write_all(part_metadata.as_bytes()).await?;
        writer.write_all(&compressed).await?;
        writer.flush().await?;

        let part_file = writer.into_inner();
        // part_file.sync_data().await?;
        drop(part_file);

        Ok(FilePart {
            part_size: part_size.into(),
            part_uuid: part_uuid.into_bytes(),
        })
    }

    async fn get_part(&self, part_uuid: Uuid) -> anyhow::Result<Option<Bytes>> {
        let part_path = self.part_path.join(format!("{part_uuid}.bin"));
        let part_file = match OpenOptions::new().read(true).open(part_path).await {
            Ok(part_file) => part_file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            err => err?,
        };

        let mut reader = BufReader::new(part_file);

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
    use std::pin::pin;

    use tokio_stream::StreamExt as _;

    use super::*;

    async fn collect(s: impl Stream<Item = anyhow::Result<Bytes>>) -> anyhow::Result<Vec<u8>> {
        let mut output = vec![];
        let mut s = pin!(s);
        while let Some(chunk) = s.next().await {
            output.extend_from_slice(chunk?.as_bytes());
        }
        Ok(output)
    }

    #[tokio::test]
    async fn stores_parts() {
        let tempdir = tempfile::tempdir().unwrap();
        let service = StorageService::new(tempdir.path()).unwrap();

        let file_part = service.put_part(b"oh hai!").await.unwrap();

        let read_part = service
            .get_part(Uuid::from_bytes(file_part.part_uuid))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_part.as_bytes(), b"oh hai!");
    }

    #[tokio::test]
    async fn stores_files() {
        let tempdir = tempfile::tempdir().unwrap();
        let service = Arc::new(StorageService::new(tempdir.path()).unwrap());

        service.put_file("the_file_key", b"oh hai!").await.unwrap();

        let file_contents = service.get_file("the_file_key").await.unwrap().unwrap();
        let file_contents = collect(file_contents).await.unwrap();

        assert_eq!(file_contents, b"oh hai!");
    }

    #[tokio::test]
    async fn assembles_file_from_parts() {
        let tempdir = tempfile::tempdir().unwrap();
        let service = Arc::new(StorageService::new(tempdir.path()).unwrap());

        let part1 = service.put_part(b"oh ").await.unwrap();
        let part2 = service.put_part(b"hai!").await.unwrap();

        service
            .assemble_file_from_parts("the_file_key", &[part1, part2])
            .await
            .unwrap();

        let file_contents = service.get_file("the_file_key").await.unwrap().unwrap();
        let file_contents = collect(file_contents).await.unwrap();

        assert_eq!(file_contents, b"oh hai!");
    }
}
