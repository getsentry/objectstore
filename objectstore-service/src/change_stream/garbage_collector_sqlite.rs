use crate::change_stream::ChangeStream;
use async_trait::async_trait;
use objectstore_types::time::Timestamp;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use std::str::FromStr;
use tokio_util::task::TaskTracker;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[allow(dead_code)]
pub struct SqliteGarbageCollectorConfig {
    pub path: String,
}

impl Default for SqliteGarbageCollectorConfig {
    fn default() -> Self {
        Self {
            path: "./objectstore.db".to_owned(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct SqliteGarbageCollectorStream {
    pool: SqlitePool,
    task_tracker: TaskTracker,
}

#[allow(dead_code)]
impl SqliteGarbageCollectorStream {
    pub async fn new(config: &SqliteGarbageCollectorConfig) -> Result<Self, sqlx::Error> {
        let opts = SqliteConnectOptions::from_str(&config.path)?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(opts).await?;

        sqlx::migrate!("./../migrations/sqlite").run(&pool).await?;

        Ok(Self {
            pool,
            task_tracker: TaskTracker::new(),
        })
    }
}

#[async_trait]
impl ChangeStream for SqliteGarbageCollectorStream {
    fn write(&self, id: &crate::id::ObjectId, _size: u64, expires_at: Option<Timestamp>) {
        let pool = self.pool.clone();
        let id = id.clone();

        self.task_tracker.spawn(async move {
            let _ = async {
                sqlx::query("INSERT INTO garbage_collector (object_id, expires_at) VALUES (?, ?) ON CONFLICT (object_id) DO UPDATE SET expires_at = excluded.expires_at")
                    .bind(id.as_storage_path().to_string())
                    .bind(expires_at.map(|t| i64::try_from(t.as_secs()).ok()))
                    .execute(&pool)
                    .await?;

                Ok::<(), sqlx::Error>(())
            }
            .await;
        });
    }

    fn update(&self, id: &crate::id::ObjectId, expires_at: Option<Timestamp>) {
        let pool = self.pool.clone();
        let id = id.clone();

        self.task_tracker.spawn(async move {
            let _ = async {
                sqlx::query("UPDATE garbage_collector SET expires_at = ? WHERE object_id = ?")
                    .bind(expires_at.map(|t| i64::try_from(t.as_secs()).ok()))
                    .bind(id.as_storage_path().to_string())
                    .execute(&pool)
                    .await?;

                Ok::<(), sqlx::Error>(())
            }
            .await;
        });
    }

    fn delete(&self, id: &crate::id::ObjectId) {
        let pool = self.pool.clone();
        let id = id.clone();

        self.task_tracker.spawn(async move {
            let _ = async {
                sqlx::query("DELETE FROM garbage_collector WHERE object_id = ?")
                    .bind(id.as_storage_path().to_string())
                    .execute(&pool)
                    .await?;

                Ok::<(), sqlx::Error>(())
            }
            .await;
        });
    }

    async fn join(&self, _timeout: std::time::Duration) {
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use crate::id::ObjectContext;

    use super::*;
    use objectstore_types::scope::Scopes;
    use objectstore_types::time::Timestamp;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Builds a config backed by a throwaway database directory.
    ///
    /// The directory guard is returned first so binding it before the stream keeps the files
    /// alive for as long as any connection can still touch them.
    fn create_test_config() -> (TempDir, SqliteGarbageCollectorConfig) {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SqliteGarbageCollectorConfig {
            path: tempdir.path().join("gc.db").to_str().unwrap().to_owned(),
        };
        (tempdir, config)
    }

    /// Opens a stream against a private, freshly migrated database.
    async fn create_test_stream() -> (TempDir, SqliteGarbageCollectorStream) {
        let (tempdir, config) = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();
        (tempdir, stream)
    }

    fn create_test_id(s: &str) -> crate::id::ObjectId {
        crate::id::ObjectId::new(
            ObjectContext {
                usecase: "test".to_string(),
                scopes: Scopes::empty(),
            },
            s.to_string(),
        )
    }

    /// Total rows in the backing table; every test owns a private database.
    async fn row_count(stream: &SqliteGarbageCollectorStream) -> i64 {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM garbage_collector")
            .fetch_one(&stream.pool)
            .await
            .unwrap();
        count.0
    }

    /// Stored expiry for `id`, looked up by the same storage path the write path binds.
    ///
    /// `None` means no row exists, `Some(None)` means a row with a NULL expiry, and
    /// `Some(Some(secs))` is the stored deadline.
    async fn stored_expiry(
        stream: &SqliteGarbageCollectorStream,
        id: &crate::id::ObjectId,
    ) -> Option<Option<i64>> {
        let row: Option<(Option<i64>,)> =
            sqlx::query_as("SELECT expires_at FROM garbage_collector WHERE object_id = ?")
                .bind(id.as_storage_path().to_string())
                .fetch_optional(&stream.pool)
                .await
                .unwrap();
        row.map(|(value,)| value)
    }

    #[tokio::test]
    async fn new_creates_stream_and_runs_migrations() {
        let (_tempdir, config) = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        // Opening a connection is not enough: the migration must have created the table.
        let table: (String,) = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'garbage_collector'",
        )
        .fetch_one(&stream.pool)
        .await
        .unwrap();
        assert_eq!(table.0, "garbage_collector");
    }

    #[tokio::test]
    async fn write_enqueues_tracked_task() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("test-object-1");

        stream.write(&id, 1024, None);

        // No await has yielded the runtime yet, so the spawned insert cannot have run or
        // been reaped: the write is queued but not yet drained.
        assert_eq!(stream.task_tracker.len(), 1);

        stream.join(Duration::from_secs(5)).await;
        assert_eq!(stream.task_tracker.len(), 0);
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));
    }

    #[tokio::test]
    async fn multiple_writes_track_each_task() {
        let (_tempdir, stream) = create_test_stream().await;

        for i in 0..5 {
            let id = create_test_id(&format!("test-object-{i}"));
            stream.write(&id, 1024 * (i + 1) as u64, None);
        }

        // Each write spawns exactly one tracked task; none can have completed yet.
        assert_eq!(stream.task_tracker.len(), 5);

        stream.join(Duration::from_secs(5)).await;
        assert_eq!(stream.task_tracker.len(), 0);
        assert_eq!(row_count(&stream).await, 5);
    }

    #[tokio::test]
    async fn write_stores_expiry_seconds_and_created_at() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("expiring");

        stream.write(
            &id,
            2048,
            Some(Timestamp::from_unix_secs(1_234_567_890).unwrap()),
        );
        stream.join(Duration::from_secs(5)).await;

        // The stored deadline is the exact whole-second value the timestamp carried.
        assert_eq!(stored_expiry(&stream, &id).await, Some(Some(1_234_567_890)));

        // `created_at` comes from a column default the write path never supplies.
        let stamped: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM garbage_collector WHERE object_id = ? AND created_at IS NOT NULL",
        )
        .bind(id.as_storage_path().to_string())
        .fetch_one(&stream.pool)
        .await
        .unwrap();
        assert_eq!(stamped.0, 1);
    }

    #[tokio::test]
    async fn write_with_no_expiry_stores_null() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("no-expiry");

        stream.write(&id, 1024, None);
        stream.join(Duration::from_secs(5)).await;

        // The row exists but carries a NULL expiry, distinct from an absent row.
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));
    }

    #[tokio::test]
    async fn write_treats_epoch_expiry_as_zero_not_null() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("epoch");

        stream.write(&id, 1024, Some(Timestamp::UNIX_EPOCH));
        stream.join(Duration::from_secs(5)).await;

        // An expiry of 0 must round-trip as 0 rather than collapsing into NULL.
        assert_eq!(stored_expiry(&stream, &id).await, Some(Some(0)));
    }

    #[tokio::test]
    async fn write_upserts_existing_row() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("upsert");

        stream.write(&id, 1024, Some(Timestamp::from_unix_secs(1_000).unwrap()));
        stream.join(Duration::from_secs(5)).await;

        stream.write(&id, 2048, Some(Timestamp::from_unix_secs(2_000).unwrap()));
        stream.join(Duration::from_secs(5)).await;

        // Re-writing the same object updates in place instead of duplicating the row.
        assert_eq!(row_count(&stream).await, 1);
        assert_eq!(stored_expiry(&stream, &id).await, Some(Some(2_000)));
    }

    #[tokio::test]
    async fn write_upsert_can_clear_expiry() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("upsert-clear");

        stream.write(&id, 1024, Some(Timestamp::from_unix_secs(1_000).unwrap()));
        stream.join(Duration::from_secs(5)).await;

        stream.write(&id, 1024, None);
        stream.join(Duration::from_secs(5)).await;

        assert_eq!(row_count(&stream).await, 1);
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));
    }

    #[tokio::test]
    async fn writes_with_distinct_usecases_do_not_collide() {
        let (_tempdir, stream) = create_test_stream().await;
        let first = crate::id::ObjectId::from_parts(
            "attachments".into(),
            Scopes::empty(),
            "shared-key".into(),
        );
        let second =
            crate::id::ObjectId::from_parts("replays".into(), Scopes::empty(), "shared-key".into());

        stream.write(
            &first,
            1024,
            Some(Timestamp::from_unix_secs(1_000).unwrap()),
        );
        stream.write(
            &second,
            1024,
            Some(Timestamp::from_unix_secs(2_000).unwrap()),
        );
        stream.join(Duration::from_secs(5)).await;

        // The row key is the full storage path, so the usecase partitions the table.
        assert_eq!(row_count(&stream).await, 2);
        assert_eq!(stored_expiry(&stream, &first).await, Some(Some(1_000)));
        assert_eq!(stored_expiry(&stream, &second).await, Some(Some(2_000)));
    }

    #[tokio::test]
    async fn update_changes_expiry() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("test-update-object");

        stream.write(
            &id,
            1024,
            Some(Timestamp::from_unix_secs(1_000_000).unwrap()),
        );
        stream.join(Duration::from_secs(5)).await;

        stream.update(&id, Some(Timestamp::from_unix_secs(2_000_000).unwrap()));
        stream.join(Duration::from_secs(5)).await;

        assert_eq!(stored_expiry(&stream, &id).await, Some(Some(2_000_000)));
    }

    #[tokio::test]
    async fn update_clears_expiry() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("clear-expiry");

        stream.write(
            &id,
            1024,
            Some(Timestamp::from_unix_secs(1_000_000).unwrap()),
        );
        stream.join(Duration::from_secs(5)).await;

        stream.update(&id, None);
        stream.join(Duration::from_secs(5)).await;

        assert_eq!(row_count(&stream).await, 1);
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));
    }

    #[tokio::test]
    async fn update_does_not_create_missing_row() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("never-written");

        stream.update(&id, Some(Timestamp::from_unix_secs(1_000_000).unwrap()));
        stream.join(Duration::from_secs(5)).await;

        // `update` is a plain UPDATE, not an upsert.
        assert_eq!(row_count(&stream).await, 0);
        assert_eq!(stored_expiry(&stream, &id).await, None);
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("test-delete-object");

        stream.write(&id, 1024, None);
        stream.join(Duration::from_secs(5)).await;
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));

        stream.delete(&id);
        stream.join(Duration::from_secs(5)).await;
        assert_eq!(stored_expiry(&stream, &id).await, None);
        assert_eq!(row_count(&stream).await, 0);
    }

    #[tokio::test]
    async fn delete_leaves_other_rows_intact() {
        let (_tempdir, stream) = create_test_stream().await;
        let kept = create_test_id("kept");
        let removed = create_test_id("removed");

        stream.write(&kept, 1024, Some(Timestamp::from_unix_secs(1_000).unwrap()));
        stream.write(
            &removed,
            1024,
            Some(Timestamp::from_unix_secs(2_000).unwrap()),
        );
        stream.join(Duration::from_secs(5)).await;

        stream.delete(&removed);
        stream.join(Duration::from_secs(5)).await;

        assert_eq!(row_count(&stream).await, 1);
        assert_eq!(stored_expiry(&stream, &removed).await, None);
        assert_eq!(stored_expiry(&stream, &kept).await, Some(Some(1_000)));
    }

    #[tokio::test]
    async fn delete_of_missing_row_is_a_no_op() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("never-written");

        stream.delete(&id);
        stream.join(Duration::from_secs(5)).await;

        assert_eq!(row_count(&stream).await, 0);
    }

    #[tokio::test]
    async fn mixed_operations() {
        let (_tempdir, stream) = create_test_stream().await;
        let id1 = create_test_id("mixed-1");
        let id2 = create_test_id("mixed-2");
        let id3 = create_test_id("mixed-3");

        // Each phase flushes so consecutive operations on the same row cannot interleave
        // nondeterministically on the backing table.
        stream.write(&id1, 1024, None);
        stream.write(&id2, 2048, None);
        stream.write(&id3, 4096, None);
        stream.join(Duration::from_secs(5)).await;
        assert_eq!(row_count(&stream).await, 3);

        stream.update(&id2, Some(Timestamp::from_unix_secs(9_999).unwrap()));
        stream.delete(&id1);
        stream.join(Duration::from_secs(5)).await;

        // Only id2 and id3 remain, each holding exactly the value its operation set.
        assert_eq!(row_count(&stream).await, 2);
        assert_eq!(stored_expiry(&stream, &id1).await, None);
        assert_eq!(stored_expiry(&stream, &id2).await, Some(Some(9_999)));
        assert_eq!(stored_expiry(&stream, &id3).await, Some(None));
    }

    #[tokio::test]
    async fn join_waits_for_pending_writes() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("flush");

        stream.write(&id, 1024, None);

        // Tracked but not yet run: nothing has yielded the runtime since the write.
        assert_eq!(stream.task_tracker.len(), 1);

        stream.join(Duration::from_secs(5)).await;

        // `join` only returns once every tracked task has been reaped and its row landed.
        assert_eq!(stream.task_tracker.len(), 0);
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));
    }

    #[tokio::test]
    async fn writes_after_a_flush_still_persist() {
        let (_tempdir, stream) = create_test_stream().await;
        let first = create_test_id("cycle-1");
        let second = create_test_id("cycle-2");

        stream.write(&first, 1024, None);
        stream.join(Duration::from_secs(5)).await;

        stream.write(&second, 1024, None);
        stream.join(Duration::from_secs(5)).await;

        // A flush closes the tracker for waiting but leaves it usable for further work.
        assert_eq!(stream.task_tracker.len(), 0);
        assert_eq!(row_count(&stream).await, 2);
        assert_eq!(stored_expiry(&stream, &first).await, Some(None));
        assert_eq!(stored_expiry(&stream, &second).await, Some(None));
    }

    #[tokio::test]
    async fn concurrent_writes_all_land() {
        let (_tempdir, stream) = create_test_stream().await;
        let stream = std::sync::Arc::new(stream);

        let mut handles = Vec::new();
        for i in 0..10 {
            let stream = std::sync::Arc::clone(&stream);
            handles.push(tokio::spawn(async move {
                let id = create_test_id(&format!("concurrent-{i}"));
                stream.write(&id, 1024 * (i + 1) as u64, None);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        stream.join(Duration::from_secs(5)).await;

        // Ten racing writers leave ten rows and a fully drained tracker.
        assert_eq!(stream.task_tracker.len(), 0);
        assert_eq!(row_count(&stream).await, 10);
    }
}
