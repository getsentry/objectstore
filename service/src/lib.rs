//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.

use std::fs::OpenOptions;
use std::io::{Read, Seek as _, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use sqlx::PgPool;
use uuid::Uuid;

mod db;

pub use db::initialize_db;

pub struct StorageService {
    db: PgPool,
    path: PathBuf,
    current_segment: Mutex<Option<Uuid>>,
}

impl StorageService {
    pub fn new(db: PgPool, path: &Path) -> Self {
        Self {
            path: path.into(),
            db,
            current_segment: Default::default(),
        }
    }

    pub async fn put_part(&self, contents: &[u8]) -> anyhow::Result<i64> {
        let part_size = contents.len();

        let segment = {
            let mut segment = self.current_segment.lock().unwrap();
            *segment.get_or_insert_with(Uuid::new_v4)
        };

        let segment_path = self.path.join(format!("{segment}.bin"));
        let mut segment_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(segment_path)?;

        let segment_offset = segment_file.stream_position()?;
        segment_file.write_all(contents)?;
        segment_file.sync_data()?;
        drop(segment_file);

        let id = sqlx::query!(
            r#"
        INSERT INTO parts
        (part_size, compression, compressed_size, segment_id, segment_offset)
        VALUES
        ($1, $2, $3, $4, $5)
        RETURNING id;"#,
            part_size as i32,
            0,
            part_size as i32,
            segment,
            segment_offset as i32,
        )
        .fetch_one(&self.db)
        .await?;

        Ok(id.id)
    }

    pub async fn get_part(&self, id: i64) -> anyhow::Result<Option<Arc<[u8]>>> {
        #[allow(unused)]
        struct Part {
            part_size: i32,
            compression: i16,
            compressed_size: i32,
            segment_id: Uuid,
            segment_offset: i32,
        }
        let Some(part) = sqlx::query_as!(
            Part,
            r#"
        SELECT
        part_size, "compression", compressed_size, segment_id, segment_offset
        FROM parts WHERE id = $1"#,
            id
        )
        .fetch_optional(&self.db)
        .await?
        else {
            return Ok(None);
        };

        let segment_path = self.path.join(format!("{}.bin", part.segment_id));
        let mut segment_file = OpenOptions::new().read(true).open(segment_path).unwrap();

        segment_file
            .seek(SeekFrom::Start(part.segment_offset as u64))
            .unwrap();

        let mut buf = vec![0; part.part_size as usize];
        segment_file.read_exact(&mut buf).unwrap();

        drop(segment_file);

        // FIXME: this reallocates. why the hell can’t we read into a `Arc<[MaybeUninit]>`?
        Ok(Some(buf.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sqlx::test]
    async fn stores_in_segments(db: PgPool) {
        let tempdir = tempfile::tempdir().unwrap();
        let service = StorageService::new(db, tempdir.path());

        let blob: &[u8] = b"oh hai!";
        let id = service.put_part(blob).await.unwrap();

        let got_blob = service.get_part(id).await.unwrap();
        assert_eq!(got_blob.unwrap().as_ref(), blob);
    }
}
