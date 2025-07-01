//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.

#![allow(unused)]

use std::fs::OpenOptions;
use std::io::{Read, Seek as _, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use uuid::Uuid;

use crate::datamodel::{BlobPart, StorageId};

mod datamodel;

struct StorageService {
    path: PathBuf,
    db: Connection,
    current_segment: Mutex<Option<Uuid>>,
}

impl StorageService {
    pub fn new(path: &Path) -> Self {
        let db_path = path.join("db.sqlite");
        let db = Connection::open(db_path).unwrap();
        //         db.execute(
        //             r#"
        // CREATE TABLE IF NOT EXISTS segments (
        //     id   INTEGER PRIMARY KEY,
        //     location INTEGER NOT NULL
        // )"#,
        //             (),
        //         )
        //         .unwrap();
        db.execute(
            r#"
CREATE TABLE IF NOT EXISTS blobs (
    id BLOB NOT NULL,
    compression INTEGER NOT NULL,
    part_size INTEGER NOT NULL,
    compressed_size INTEGER NOT NULL,

    segment BLOB NOT NULL,
    segment_offset INTEGER NOT NULL
)"#,
            (),
        )
        .unwrap();

        Self {
            path: path.into(),
            db,
            current_segment: Default::default(),
        }
    }

    pub fn put_blob(&self, id: StorageId, contents: &[u8]) {
        let mut segment = self.current_segment.lock().unwrap();
        let current_segment = segment.get_or_insert_with(Uuid::new_v4);

        let segment_path = self.path.join(format!("{current_segment}.bin"));
        let mut segment_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(segment_path)
            .unwrap();

        let part_size = contents.len();

        let segment_offset = segment_file.stream_position().unwrap();
        segment_file.write_all(contents).unwrap();
        segment_file.sync_all().unwrap();
        drop(segment_file);

        self.db
            .execute(
                "INSERT INTO blobs
            (id, compression, part_size, compressed_size, segment, segment_offset)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                (
                    id.id.as_bytes(),
                    0,
                    part_size,
                    part_size,
                    current_segment.as_bytes(),
                    segment_offset,
                ),
            )
            .unwrap();
    }

    pub fn get_blob(&self, id: StorageId) -> Arc<[u8]> {
        let mut stmt = self.db.prepare("SELECT compression, part_size, compressed_size, segment, segment_offset FROM blobs WHERE id = ?1").unwrap();
        let blob = stmt
            .query_one([id.id.as_bytes()], |row| {
                Ok(BlobPart {
                    compression: datamodel::Compression::None,
                    part_size: row.get(1)?,
                    compressed_size: row.get(2)?,
                    segment_id: Uuid::from_bytes(row.get(3)?),
                    segment_offset: row.get(4)?,
                })
            })
            .unwrap();

        let segment_path = self.path.join(format!("{}.bin", blob.segment_id));
        let mut segment_file = OpenOptions::new().read(true).open(segment_path).unwrap();

        segment_file
            .seek(SeekFrom::Start(blob.segment_offset as u64))
            .unwrap();

        let mut buf = vec![0; blob.part_size as usize];
        segment_file.read_exact(&mut buf).unwrap();

        drop(segment_file);

        // FIXME: this reallocates. why the hell can’t we read into a `Arc<[MaybeUninit]>`?
        buf.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_in_segments() {
        let tempdir = tempfile::tempdir().unwrap();
        let service = StorageService::new(tempdir.path());

        let blob: &[u8] = b"oh hai!";
        let id = StorageId { id: Uuid::new_v4() };
        service.put_blob(id.clone(), blob);

        assert_eq!(service.get_blob(id).as_ref(), blob);
    }
}
