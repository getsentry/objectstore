use std::str::FromStr;

use async_trait::async_trait;
use objectstore_log::Level;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};

use crate::change_stream::ChangeStreamError;
use crate::garbage_collector::GcStorage;
use crate::id::ObjectId;

/// Configuration for [`SqliteGarbageCollector`].
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[allow(dead_code)]
pub struct SqliteGarbageCollectorConfig {
    /// Path to the SQLite database file, created if it does not exist.
    pub path: String,
}

impl Default for SqliteGarbageCollectorConfig {
    fn default() -> Self {
        Self {
            path: "./objectstore.db".to_owned(),
        }
    }
}

/// [`GcStorage`] backed by a SQLite database.
#[derive(Debug, Clone)]
pub struct SqliteGarbageCollector {
    pool: SqlitePool,
}

#[allow(dead_code)]
impl SqliteGarbageCollector {
    /// Opens (creating if needed) the database at [`config.path`](SqliteGarbageCollectorConfig::path)
    /// in WAL mode and runs the schema migrations.
    pub async fn new(config: &SqliteGarbageCollectorConfig) -> Result<Self, sqlx::Error> {
        let opts = SqliteConnectOptions::from_str(&config.path)?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(opts).await?;

        sqlx::migrate!("./../migrations/sqlite").run(&pool).await?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl GcStorage for SqliteGarbageCollector {
    async fn upsert(
        &self,
        id: &ObjectId,
        expires_at: Option<objectstore_types::time::Timestamp>,
    ) -> Result<(), ChangeStreamError> {
        let _ = sqlx::query(
            r#"INSERT INTO garbage_collector (object_id, expires_at)
                VALUES (?, ?)
                ON CONFLICT (object_id)
                DO UPDATE
                SET expires_at = excluded.expires_at
                WHERE garbage_collector.expires_at IS NOT NULL
                    AND garbage_collector.expires_at < excluded.expires_at"#,
        )
        .bind(id.as_storage_path().to_string())
        .bind(expires_at.map(|t| i64::try_from(t.as_secs()).ok()))
        .execute(&self.pool)
        .await
        .map_err(|e| ChangeStreamError::Failure(Box::new(e)))?;

        Ok(())
    }

    async fn remove(&self, id: &ObjectId) {
        let _ = sqlx::query("DELETE FROM garbage_collector WHERE object_id = ?")
            .bind(id.as_storage_path().to_string())
            .execute(&self.pool)
            .await;
    }

    async fn list_expired(
        &self,
        now: objectstore_types::time::Timestamp,
        limit: u64,
    ) -> Result<Vec<ObjectId>, ChangeStreamError> {
        let query_result: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar::<_, String>(
            r#"SELECT
                object_id
            FROM
                garbage_collector
            WHERE
                expires_at IS NOT NULL
                AND expires_at <= ?
            ORDER BY
                expires_at
            LIMIT ?"#,
        )
        .bind(i64::try_from(now.as_secs()).ok())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await;

        match query_result {
            Ok(rows) => {
                let mut ids = Vec::new();
                for row in rows {
                    let id = row.clone();
                    if let Some(object_id) = ObjectId::from_storage_path(&id) {
                        ids.push(object_id);
                    } else {
                        objectstore_log::event!(
                            Level::WARN,
                            object_id = id,
                            "cannot resolve object id from storage path"
                        );
                    }
                }
                Ok(ids)
            }
            Err(error) => Err(ChangeStreamError::Failure(Box::new(error))),
        }
    }
}

#[cfg(test)]
mod tests {
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::id::ObjectContext;

    /// The latest representable [`Timestamp`](objectstore_types::time::Timestamp), used as
    /// "infinitely far in the future" for expiration checks.
    const MAX_SECS: u64 = 253_402_300_799;

    fn ts(secs: u64) -> objectstore_types::time::Timestamp {
        objectstore_types::time::Timestamp::from_unix_secs(secs).unwrap()
    }

    fn id_for(key: &str) -> ObjectId {
        ObjectId::new(
            ObjectContext {
                usecase: "testing".parse().unwrap(),
                scopes: Scopes::from_iter([
                    Scope::create("organization", "17").unwrap(),
                    Scope::create("project", "42").unwrap(),
                ]),
            },
            key.to_owned(),
        )
    }

    fn config_in(dir: &tempfile::TempDir) -> SqliteGarbageCollectorConfig {
        SqliteGarbageCollectorConfig {
            path: dir
                .path()
                .join("objectstore.db")
                .to_string_lossy()
                .into_owned(),
        }
    }

    async fn collector_in(dir: &tempfile::TempDir) -> SqliteGarbageCollector {
        SqliteGarbageCollector::new(&config_in(dir)).await.unwrap()
    }

    /// Creates a collector backed by a database in a fresh temporary directory. The directory is
    /// returned so that it outlives the collector.
    async fn make_collector() -> (tempfile::TempDir, SqliteGarbageCollector) {
        let dir = tempfile::tempdir().unwrap();
        let collector = collector_in(&dir).await;
        (dir, collector)
    }

    #[tokio::test]
    async fn new_runs_migrations_on_empty_database() {
        let (_dir, collector) = make_collector().await;

        assert!(
            collector
                .list_expired(ts(MAX_SECS), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn upsert_and_list_expired_roundtrip_the_full_id() {
        let (_dir, collector) = make_collector().await;
        let id = id_for("key");

        collector.upsert(&id, Some(ts(100))).await.unwrap();

        // Equality covers usecase, scopes, and key, so this also proves the storage path
        // roundtrips through `from_storage_path`.
        assert_eq!(collector.list_expired(ts(100), 10).await.unwrap(), vec![id]);
    }

    #[tokio::test]
    async fn list_expired_excludes_deadlines_after_now() {
        let (_dir, collector) = make_collector().await;
        let id = id_for("key");

        collector.upsert(&id, Some(ts(200))).await.unwrap();

        assert!(
            collector
                .list_expired(ts(199), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn list_expired_ignores_objects_without_expiration() {
        let (_dir, collector) = make_collector().await;

        collector.upsert(&id_for("key"), None).await.unwrap();

        assert!(
            collector
                .list_expired(ts(MAX_SECS), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn upsert_extends_an_earlier_deadline() {
        let (_dir, collector) = make_collector().await;
        let id = id_for("key");

        collector.upsert(&id, Some(ts(100))).await.unwrap();
        collector.upsert(&id, Some(ts(300))).await.unwrap();

        assert!(
            collector
                .list_expired(ts(100), 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(collector.list_expired(ts(300), 10).await.unwrap(), vec![id]);
    }

    #[tokio::test]
    async fn upsert_does_not_shorten_an_existing_deadline() {
        let (_dir, collector) = make_collector().await;
        let id = id_for("key");

        collector.upsert(&id, Some(ts(300))).await.unwrap();
        collector.upsert(&id, Some(ts(100))).await.unwrap();

        assert_eq!(collector.list_expired(ts(300), 10).await.unwrap(), vec![id]);
        assert!(
            collector
                .list_expired(ts(200), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn upsert_infinite_duration_does_not_update_expiry() {
        let (_dir, collector) = make_collector().await;
        let id = id_for("key");

        collector.upsert(&id, None).await.unwrap();
        collector.upsert(&id, Some(ts(100))).await.unwrap();

        assert!(
            collector
                .list_expired(ts(100), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn upserting_the_same_id_twice_does_not_duplicate_it() {
        let (_dir, collector) = make_collector().await;
        let id = id_for("key");

        collector.upsert(&id, Some(ts(100))).await.unwrap();
        collector.upsert(&id, Some(ts(100))).await.unwrap();

        assert_eq!(collector.list_expired(ts(100), 10).await.unwrap(), vec![id]);
    }

    #[tokio::test]
    async fn upserting_the_same_id_twice_does_not_duplicate_it_infinite_expiry() {
        let (_dir, collector) = make_collector().await;
        let id = id_for("key");

        collector.upsert(&id, None).await.unwrap();
        collector.upsert(&id, None).await.unwrap();

        assert!(
            collector
                .list_expired(ts(100), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn remove_forgets_the_object() {
        let (_dir, collector) = make_collector().await;
        let id = id_for("key");

        collector.upsert(&id, Some(ts(100))).await.unwrap();
        collector.remove(&id).await;

        assert!(
            collector
                .list_expired(ts(MAX_SECS), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn remove_of_unknown_object_is_a_noop() {
        let (_dir, collector) = make_collector().await;

        collector.remove(&id_for("missing")).await;

        assert!(
            collector
                .list_expired(ts(MAX_SECS), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn list_expired_orders_by_earliest_deadline_first() {
        let (_dir, collector) = make_collector().await;
        let (late, early, middle) = (id_for("late"), id_for("early"), id_for("middle"));

        collector.upsert(&late, Some(ts(300))).await.unwrap();
        collector.upsert(&early, Some(ts(100))).await.unwrap();
        collector.upsert(&middle, Some(ts(200))).await.unwrap();

        assert_eq!(
            collector.list_expired(ts(300), 10).await.unwrap(),
            vec![early, middle, late]
        );
    }

    #[tokio::test]
    async fn list_expired_respects_the_limit() {
        let (_dir, collector) = make_collector().await;
        let (first, second, third) = (id_for("first"), id_for("second"), id_for("third"));

        collector.upsert(&first, Some(ts(100))).await.unwrap();
        collector.upsert(&second, Some(ts(200))).await.unwrap();
        collector.upsert(&third, Some(ts(300))).await.unwrap();

        assert_eq!(
            collector.list_expired(ts(300), 2).await.unwrap(),
            vec![first, second]
        );
        assert!(collector.list_expired(ts(300), 0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_expired_with_limit_above_row_count_returns_everything() {
        let (_dir, collector) = make_collector().await;
        let (first, second) = (id_for("first"), id_for("second"));

        collector.upsert(&first, Some(ts(100))).await.unwrap();
        collector.upsert(&second, Some(ts(200))).await.unwrap();

        assert_eq!(
            collector.list_expired(ts(300), 1_000).await.unwrap(),
            vec![first, second]
        );
    }

    #[tokio::test]
    async fn reopening_an_existing_database_keeps_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let id = id_for("key");

        {
            let collector = collector_in(&dir).await;
            collector.upsert(&id, Some(ts(100))).await.unwrap();
        }

        // Re-running the migrations must be a no-op, and the row must still be there.
        let collector = collector_in(&dir).await;
        assert_eq!(collector.list_expired(ts(100), 10).await.unwrap(), vec![id]);
    }
}
