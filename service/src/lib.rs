//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.

mod datamodel;

use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Seek as _, SeekFrom, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::Context;
use sqlx::PgPool;
use uuid::Uuid;

mod db;

pub use db::initialize_db;
use watto::Pod;

use crate::datamodel::{
    Compression, FILE_MAGIC, FILE_VERSION, File, FilePart, PART_MAGIC, PART_VERSION, Part,
};

pub struct StorageService {
    file_path: PathBuf,
    part_path: PathBuf,
}

impl StorageService {
    pub fn new(path: &Path) -> Self {
        Self {
            file_path: path.join("files"),
            part_path: path.join("parts"),
        }
    }

    pub fn assemble_file_from_parts(&self, key: &str, parts: &[FilePart]) -> anyhow::Result<()> {
        let file_size = parts.iter().map(|part| part.part_size.get() as u64).sum();

        let file_metadata = File {
            magic: FILE_MAGIC,
            version: FILE_VERSION.into(),
            num_parts: (parts.len() as u32).into(),
            file_size: file_size.into(),
        };

        let file_path = self.file_path.join(format!("{key}.bin"));
        let file_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(file_path)?;

        let mut writer = BufWriter::new(file_file);

        writer.write_all(file_metadata.as_bytes())?;
        writer.write_all(parts.as_bytes())?;

        let part_file = writer.into_inner()?;
        part_file.sync_data()?;
        drop(part_file);

        Ok(())
    }

    pub fn get_file(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        todo!()
    }

    pub fn put_part(&self, contents: &[u8]) -> anyhow::Result<FilePart> {
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
            .open(part_path)?;

        let mut writer = BufWriter::new(part_file);

        writer.write_all(part_metadata.as_bytes())?;
        writer.write_all(&compressed)?;

        let part_file = writer.into_inner()?;
        part_file.sync_data()?;
        drop(part_file);

        Ok(FilePart {
            part_size: part_size.into(),
            part_uuid: part_uuid.into_bytes(),
        })
    }

    pub fn get_part(&self, part_uuid: Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        let part_path = self.file_path.join(format!("{part_uuid}.bin"));
        let part_file = match OpenOptions::new().read(true).open(part_path) {
            Ok(part_file) => part_file,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            err => err?,
        };

        let mut reader = BufReader::new(part_file);

        let mut metadata_buf = vec![0; mem::size_of::<Part>()];
        reader.read_exact(&mut metadata_buf)?;
        let part_metadata = Part::ref_from_bytes(&metadata_buf).context("reading Part metadata")?;

        // TODO: verify magic, version, etc…
        let contents = match part_metadata.compression_algorithm {
            1 /* Zstd */ => zstd::decode_all(reader)?,
            _ => {
                let mut buf = Vec::with_capacity(part_metadata.part_size.get() as usize);
                reader.read_to_end(&mut buf)?;
                buf
            }
        };

        Ok(Some(contents))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_parts() {
        let tempdir = tempfile::tempdir().unwrap();

        let service = StorageService::new(tempdir.path());

        let file_part = service.put_part(b"oh hai!").unwrap();

        let read_part = service
            .get_part(Uuid::from_bytes(file_part.part_uuid))
            .unwrap()
            .unwrap();

        assert_eq!(read_part, b"oh hai!");
    }
}
