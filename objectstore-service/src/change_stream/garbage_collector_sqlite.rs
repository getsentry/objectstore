use crate::change_stream::{ChangeStream, ChangeStreamError, ExpiryIndex};
use crate::id::ObjectId;
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
    async fn begin_write(
        &self,
        id: &ObjectId,
        expires_at: Option<Timestamp>,
    ) -> Result<(), ChangeStreamError> {
        let pool = self.pool.clone();
        let id = id.clone();
        let query_result = sqlx::query("INSERT INTO garbage_collector (object_id, expires_at) VALUES (?, ?) ON CONFLICT (object_id) DO UPDATE SET expires_at = excluded.expires_at")
            .bind(id.as_storage_path().to_string())
            .bind(expires_at.map(|t| i64::try_from(t.as_secs()).ok()))
            .execute(&pool)
            .await;

        if query_result.is_err() {
            let error = query_result.err();
            match error {
                Some(err) => return Err(ChangeStreamError::Failure(Box::new(err))),
                None => {
                    return Err(ChangeStreamError::Failure(
                        "failed to insert row into garbage_collector".into(),
                    ));
                }
            }
        }

        Ok(())
    }

    async fn begin_update(
        &self,
        id: &ObjectId,
        expires_at: Option<Timestamp>,
    ) -> Result<(), ChangeStreamError> {
        let pool = self.pool.clone();
        let id = id.clone();

        let query_result =
            sqlx::query("UPDATE garbage_collector SET expires_at = ? WHERE object_id = ?")
                .bind(expires_at.map(|t| i64::try_from(t.as_secs()).ok()))
                .bind(id.as_storage_path().to_string())
                .execute(&pool)
                .await;

        if query_result.is_err() {
            let error = query_result.err();
            match error {
                Some(err) => return Err(ChangeStreamError::Failure(Box::new(err))),
                None => {
                    return Err(ChangeStreamError::Failure(
                        "failed to update row in garbage_collector".into(),
                    ));
                }
            }
        } else {
            Ok(())
        }
    }

    async fn commit_delete(&self, id: &ObjectId) {
        let pool = self.pool.clone();
        let id = id.clone();

        let _ = sqlx::query("DELETE FROM garbage_collector WHERE object_id = ?")
            .bind(id.as_storage_path().to_string())
            .execute(&pool)
            .await;
    }

    async fn join(&self, timeout: std::time::Duration) {
        self.task_tracker.close();
        let _ = tokio::time::timeout(timeout, self.task_tracker.wait()).await;
    }
}

#[async_trait]
impl ExpiryIndex for SqliteGarbageCollectorStream {
    async fn list_expired(
        &self,
        now: Timestamp,
        limit: u64,
    ) -> Result<Vec<ObjectId>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = self.pool.clone();
        let query_result: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar::<_, String>(
            "SELECT object_id FROM garbage_collector WHERE expires_at IS NOT NULL AND expires_at <= ? ORDER BY expires_at LIMIT ?",
        )
        .bind(i64::try_from(now.as_secs()).ok())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&pool)
        .await;

        match query_result {
            Ok(rows) => {
                let mut ids = Vec::new();
                for row in rows {
                    let id = row.clone();
                    ids.push(ObjectId::from_storage_path(&id).unwrap());
                }
                Ok(ids)
            }
            Err(error) => Err(Box::new(error)),
        }
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

    /// Stored expiry for `id`, looked up by the same storage path `begin_write` binds.
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

    /// Removes the backing table so the next `begin_*` call cannot reach it.
    async fn drop_table(stream: &SqliteGarbageCollectorStream) {
        sqlx::query("DROP TABLE garbage_collector")
            .execute(&stream.pool)
            .await
            .unwrap();
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
    async fn begin_write_with_no_expiry_stores_null() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("no-expiry");

        stream.begin_write(&id, None).await.unwrap();

        // `begin_write` is awaited on the request path, so the row is visible on return.
        assert_eq!(row_count(&stream).await, 1);
        // The row exists but carries a NULL expiry, distinct from an absent row.
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));
    }

    #[tokio::test]
    async fn begin_write_stores_expiry_seconds_and_created_at() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("expiring");

        stream
            .begin_write(&id, Some(Timestamp::from_unix_secs(1_234_567_890).unwrap()))
            .await
            .unwrap();

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
    async fn begin_write_treats_epoch_expiry_as_zero_not_null() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("epoch");

        stream
            .begin_write(&id, Some(Timestamp::UNIX_EPOCH))
            .await
            .unwrap();

        // An expiry of 0 must round-trip as 0 rather than collapsing into NULL.
        assert_eq!(stored_expiry(&stream, &id).await, Some(Some(0)));
    }

    #[tokio::test]
    async fn begin_write_upserts_existing_row() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("upsert");

        stream
            .begin_write(&id, Some(Timestamp::from_unix_secs(1_000).unwrap()))
            .await
            .unwrap();
        stream
            .begin_write(&id, Some(Timestamp::from_unix_secs(2_000).unwrap()))
            .await
            .unwrap();

        // Re-writing the same object updates in place instead of duplicating the row.
        assert_eq!(row_count(&stream).await, 1);
        assert_eq!(stored_expiry(&stream, &id).await, Some(Some(2_000)));
    }

    #[tokio::test]
    async fn begin_write_upsert_can_clear_expiry() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("upsert-clear");

        stream
            .begin_write(&id, Some(Timestamp::from_unix_secs(1_000).unwrap()))
            .await
            .unwrap();
        stream.begin_write(&id, None).await.unwrap();

        assert_eq!(row_count(&stream).await, 1);
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));
    }

    #[tokio::test]
    async fn begin_writes_with_distinct_usecases_do_not_collide() {
        let (_tempdir, stream) = create_test_stream().await;
        let first = crate::id::ObjectId::from_parts(
            "attachments".into(),
            Scopes::empty(),
            "shared-key".into(),
        );
        let second =
            crate::id::ObjectId::from_parts("replays".into(), Scopes::empty(), "shared-key".into());

        stream
            .begin_write(&first, Some(Timestamp::from_unix_secs(1_000).unwrap()))
            .await
            .unwrap();
        stream
            .begin_write(&second, Some(Timestamp::from_unix_secs(2_000).unwrap()))
            .await
            .unwrap();

        // The row key is the full storage path, so the usecase partitions the table.
        assert_eq!(row_count(&stream).await, 2);
        assert_eq!(stored_expiry(&stream, &first).await, Some(Some(1_000)));
        assert_eq!(stored_expiry(&stream, &second).await, Some(Some(2_000)));
    }

    #[tokio::test]
    async fn begin_write_reports_failure_when_table_is_missing() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("missing-table");

        drop_table(&stream).await;

        // A failed `begin_*` is surfaced so the request can abort before touching storage.
        let error = stream.begin_write(&id, None).await.unwrap_err();
        assert!(matches!(error, ChangeStreamError::Failure(_)));
        assert_eq!(error.to_string(), "change stream failed");
    }

    #[tokio::test]
    async fn begin_update_changes_expiry() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("test-update-object");

        stream
            .begin_write(&id, Some(Timestamp::from_unix_secs(1_000_000).unwrap()))
            .await
            .unwrap();
        stream
            .begin_update(&id, Some(Timestamp::from_unix_secs(2_000_000).unwrap()))
            .await
            .unwrap();

        assert_eq!(stored_expiry(&stream, &id).await, Some(Some(2_000_000)));
    }

    #[tokio::test]
    async fn begin_update_clears_expiry() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("clear-expiry");

        stream
            .begin_write(&id, Some(Timestamp::from_unix_secs(1_000_000).unwrap()))
            .await
            .unwrap();
        stream.begin_update(&id, None).await.unwrap();

        assert_eq!(row_count(&stream).await, 1);
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));
    }

    #[tokio::test]
    async fn begin_update_does_not_insert_missing_row() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("never-written");

        // `begin_update` is a plain UPDATE, not an upsert: a missing row is a no-op, not an
        // error.
        stream
            .begin_update(&id, Some(Timestamp::from_unix_secs(1_000_000).unwrap()))
            .await
            .unwrap();

        assert_eq!(row_count(&stream).await, 0);
        assert_eq!(stored_expiry(&stream, &id).await, None);
    }

    #[tokio::test]
    async fn begin_update_reports_failure_when_table_is_missing() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("missing-table-update");

        drop_table(&stream).await;

        let error = stream.begin_update(&id, None).await.unwrap_err();
        assert!(matches!(error, ChangeStreamError::Failure(_)));
        assert_eq!(error.to_string(), "change stream failed");
    }

    #[tokio::test]
    async fn commit_delete_removes_row() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("test-delete-object");

        stream.begin_write(&id, None).await.unwrap();
        assert_eq!(stored_expiry(&stream, &id).await, Some(None));

        stream.commit_delete(&id).await;

        assert_eq!(stored_expiry(&stream, &id).await, None);
        assert_eq!(row_count(&stream).await, 0);
    }

    #[tokio::test]
    async fn commit_delete_leaves_other_rows_intact() {
        let (_tempdir, stream) = create_test_stream().await;
        let kept = create_test_id("kept");
        let removed = create_test_id("removed");

        stream
            .begin_write(&kept, Some(Timestamp::from_unix_secs(1_000).unwrap()))
            .await
            .unwrap();
        stream
            .begin_write(&removed, Some(Timestamp::from_unix_secs(2_000).unwrap()))
            .await
            .unwrap();

        stream.commit_delete(&removed).await;

        assert_eq!(row_count(&stream).await, 1);
        assert_eq!(stored_expiry(&stream, &removed).await, None);
        assert_eq!(stored_expiry(&stream, &kept).await, Some(Some(1_000)));
    }

    #[tokio::test]
    async fn commit_delete_of_missing_row_is_a_no_op() {
        let (_tempdir, stream) = create_test_stream().await;
        let id = create_test_id("never-written");

        stream.commit_delete(&id).await;

        assert_eq!(row_count(&stream).await, 0);
    }

    #[tokio::test]
    async fn mixed_operations() {
        let (_tempdir, stream) = create_test_stream().await;
        let id1 = create_test_id("mixed-1");
        let id2 = create_test_id("mixed-2");
        let id3 = create_test_id("mixed-3");

        // Every call is awaited before the next, so operations on the same row cannot
        // interleave nondeterministically on the backing table.
        stream.begin_write(&id1, None).await.unwrap();
        stream.begin_write(&id2, None).await.unwrap();
        stream.begin_write(&id3, None).await.unwrap();
        assert_eq!(row_count(&stream).await, 3);

        stream
            .begin_update(&id2, Some(Timestamp::from_unix_secs(9_999).unwrap()))
            .await
            .unwrap();
        stream.commit_delete(&id1).await;

        // Only id2 and id3 remain, each holding exactly the value its operation set.
        assert_eq!(row_count(&stream).await, 2);
        assert_eq!(stored_expiry(&stream, &id1).await, None);
        assert_eq!(stored_expiry(&stream, &id2).await, Some(Some(9_999)));
        assert_eq!(stored_expiry(&stream, &id3).await, Some(None));
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
                stream.begin_write(&id, None).await.unwrap();
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        stream.join(Duration::from_secs(5)).await;

        // Ten racing writers leave ten rows.
        assert_eq!(row_count(&stream).await, 10);
    }

    #[tokio::test]
    async fn join_is_repeatable_and_preserves_rows() {
        let (_tempdir, stream) = create_test_stream().await;
        let first = create_test_id("cycle-1");
        let second = create_test_id("cycle-2");

        stream.begin_write(&first, None).await.unwrap();
        stream.join(Duration::from_secs(5)).await;

        // `join` may run on every shutdown drain without breaking subsequent work.
        stream.begin_write(&second, None).await.unwrap();
        stream.join(Duration::from_secs(5)).await;

        assert_eq!(row_count(&stream).await, 2);
        assert_eq!(stored_expiry(&stream, &first).await, Some(None));
        assert_eq!(stored_expiry(&stream, &second).await, Some(None));
    }

    #[tokio::test]
    async fn list_expired_returns_only_past_deadlines() {
        let (_tempdir, stream) = create_test_stream().await;
        let past = create_test_id("past");
        let future = create_test_id("future");
        let no_expiry = create_test_id("no-expiry");

        stream
            .begin_write(&past, Some(Timestamp::from_unix_secs(1_000).unwrap()))
            .await
            .unwrap();
        stream
            .begin_write(&future, Some(Timestamp::from_unix_secs(9_000).unwrap()))
            .await
            .unwrap();
        stream.begin_write(&no_expiry, None).await.unwrap();

        // Only the past deadline is listed: the future deadline and the NULL expiry
        // are both excluded, and the returned id round-trips through the storage path.
        assert_eq!(
            stream
                .list_expired(Timestamp::from_unix_secs(5_000).unwrap(), 10)
                .await
                .unwrap(),
            vec![past.clone()]
        );
    }

    #[tokio::test]
    async fn list_expired_includes_deadline_equal_to_now() {
        let (_tempdir, stream) = create_test_stream().await;
        let due = create_test_id("due");
        let later = create_test_id("later");

        stream
            .begin_write(&due, Some(Timestamp::from_unix_secs(2_000).unwrap()))
            .await
            .unwrap();
        stream
            .begin_write(&later, Some(Timestamp::from_unix_secs(2_001).unwrap()))
            .await
            .unwrap();

        // The contract is inclusive: a deadline exactly equal to `now` is listed.
        assert_eq!(
            stream
                .list_expired(Timestamp::from_unix_secs(2_000).unwrap(), 10)
                .await
                .unwrap(),
            vec![due.clone()]
        );
    }

    #[tokio::test]
    async fn list_expired_orders_by_oldest_deadline_and_respects_limit() {
        let (_tempdir, stream) = create_test_stream().await;
        let newest = create_test_id("newest");
        let oldest = create_test_id("oldest");
        let middle = create_test_id("middle");

        // Seeded scrambled relative to deadline order on purpose.
        stream
            .begin_write(&newest, Some(Timestamp::from_unix_secs(3_000).unwrap()))
            .await
            .unwrap();
        stream
            .begin_write(&oldest, Some(Timestamp::from_unix_secs(1_000).unwrap()))
            .await
            .unwrap();
        stream
            .begin_write(&middle, Some(Timestamp::from_unix_secs(2_000).unwrap()))
            .await
            .unwrap();

        // Oldest deadline first, capped at `limit`...
        assert_eq!(
            stream
                .list_expired(Timestamp::from_unix_secs(5_000).unwrap(), 2)
                .await
                .unwrap(),
            vec![oldest.clone(), middle.clone()]
        );
        // ...and the full set once the limit allows it.
        assert_eq!(
            stream
                .list_expired(Timestamp::from_unix_secs(5_000).unwrap(), 10)
                .await
                .unwrap(),
            vec![oldest.clone(), middle.clone(), newest.clone()]
        );
    }

    #[tokio::test]
    async fn list_expired_tracks_begin_update() {
        let (_tempdir, stream) = create_test_stream().await;
        let moved = create_test_id("moved");

        stream
            .begin_write(&moved, Some(Timestamp::from_unix_secs(1_000).unwrap()))
            .await
            .unwrap();
        assert_eq!(
            stream
                .list_expired(Timestamp::from_unix_secs(5_000).unwrap(), 10)
                .await
                .unwrap(),
            vec![moved.clone()]
        );

        // Extending the deadline past `now` drops the row out of the listing.
        stream
            .begin_update(&moved, Some(Timestamp::from_unix_secs(9_000).unwrap()))
            .await
            .unwrap();
        assert_eq!(
            stream
                .list_expired(Timestamp::from_unix_secs(5_000).unwrap(), 10)
                .await
                .unwrap(),
            Vec::<ObjectId>::new()
        );

        // Shortening it back below `now` makes the row listed again.
        stream
            .begin_update(&moved, Some(Timestamp::from_unix_secs(2_000).unwrap()))
            .await
            .unwrap();
        assert_eq!(
            stream
                .list_expired(Timestamp::from_unix_secs(5_000).unwrap(), 10)
                .await
                .unwrap(),
            vec![moved.clone()]
        );

        // Clearing the expiry leaves a NULL row, which is never listed.
        stream.begin_update(&moved, None).await.unwrap();
        assert_eq!(
            stream
                .list_expired(Timestamp::from_unix_secs(5_000).unwrap(), 10)
                .await
                .unwrap(),
            Vec::<ObjectId>::new()
        );
    }
}
