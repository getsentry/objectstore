//! The Service layer is providing the fundamental storage abstraction,
//! providing durable access to underlying blobs.
//!
//! It is designed as a library crate to be used by the `server`.

use std::fs::OpenOptions;
use std::io::{Read, Seek as _, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use sqlx::PgPool;
use uuid::Uuid;

mod db;

pub use db::initialize_db;

#[allow(unused)]
#[derive(Debug)]
struct Part {
    part_size: i32,
    compression: i16,
    compressed_size: i32,
    segment_id: Uuid,
    segment_offset: i32,
}

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

    pub async fn assemble_file_from_parts(&self, id: &str, parts: &[i64]) -> anyhow::Result<()> {
        sqlx::query!(
            r#"
        INSERT INTO blobs
        (id, parts)
        VALUES
        ($1, $2);"#,
            id,
            parts,
        )
        .execute(&self.db)
        .await?;

        Ok(())
    }

    async fn get_file_parts(&self, id: &str) -> anyhow::Result<Vec<Part>> {
        let parts = sqlx::query_as!(
            Part,
            r#"
            SELECT p.part_size, p."compression", p.compressed_size, p.segment_id, p.segment_offset
            FROM (SELECT unnest(parts) AS id FROM blobs WHERE id = $1) AS part_id
            JOIN parts AS p ON p.id = part_id.id;"#,
            id,
        )
        .fetch_all(&self.db)
        .await?;

        Ok(parts)
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

        let segment_offset = segment_file.seek(SeekFrom::End(0))?;
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

    pub async fn get_part(&self, id: i64) -> anyhow::Result<Option<Vec<u8>>> {
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

        let contents = self.get_part_from_storage(&part).await?;
        Ok(Some(contents))
    }

    async fn get_part_from_storage(&self, part: &Part) -> anyhow::Result<Vec<u8>> {
        let segment_path = self.path.join(format!("{}.bin", part.segment_id));
        let mut segment_file = OpenOptions::new().read(true).open(segment_path)?;

        segment_file.seek(SeekFrom::Start(part.segment_offset as u64))?;

        let mut buf = vec![0; part.part_size as usize];
        segment_file.read_exact(&mut buf)?;

        drop(segment_file);

        Ok(buf)
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
        assert_eq!(got_blob.unwrap(), blob);
    }

    #[sqlx::test]
    async fn stores_blob_as_parts(db: PgPool) {
        let tempdir = tempfile::tempdir().unwrap();
        let service = StorageService::new(db, tempdir.path());

        let part1 = service.put_part(b"oh ").await.unwrap();
        let part2 = service.put_part(b"hai!").await.unwrap();
        service
            .assemble_file_from_parts("some/file/id", &[part1, part2])
            .await
            .unwrap();

        let parts = service.get_file_parts("some/file/id").await.unwrap();

        let mut contents = vec![];
        for part in parts {
            let part = service.get_part_from_storage(&part).await.unwrap();
            contents.extend_from_slice(&part);
        }

        assert_eq!(contents, b"oh hai!");
    }
}
