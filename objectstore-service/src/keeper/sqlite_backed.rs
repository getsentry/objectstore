use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::migrate::MigrateDatabase;
use sqlx::pool::PoolOptions;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqliteSynchronous,
};
use sqlx::{ConnectOptions, FromRow, Pool, Sqlite};

use objectstore_types::metadata::ExpirationPolicy;

use crate::error::{Error, Result};
use crate::id::ObjectId;

/// A row from the `ttl_keeper` table.
#[derive(Debug, FromRow)]
pub struct TableRow {
    /// The storage path key for the object.
    pub object_id: String,
    /// Encoded expiration policy: 0 = Manual, 1 = TimeToLive, 2 = TimeToIdle.
    pub expiration_policy: i32,
    /// The expiration duration in seconds, if applicable.
    pub duration: Option<i64>,
    /// Unix timestamp (seconds) when the row was created.
    pub created_at: i64,
    /// Unix timestamp (seconds) when the object expires, if applicable.
    pub expires_at: Option<i64>,
}

/// SQLite-backed keeper that persists object retention state in a `ttl_keeper` table.
#[derive(Debug)]
pub struct SqliteBackedKeeper {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

/// Creates a pair of read/write SQLite connection pools for the given URL.
pub async fn create_sqlite_pool(url: &str) -> Result<(Pool<Sqlite>, Pool<Sqlite>)> {
    if !Sqlite::database_exists(url).await? {
        Sqlite::create_database(url).await?
    }

    let read_pool = PoolOptions::<Sqlite>::new()
        .max_connections(64)
        .connect_with(
            SqliteConnectOptions::from_str(url)?
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .read_only(true)
                .disable_statement_logging(),
        )
        .await?;

    let write_pool = PoolOptions::<Sqlite>::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(url)?
                .journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .auto_vacuum(SqliteAutoVacuum::Incremental)
                .disable_statement_logging(),
        )
        .await?;

    Ok((read_pool, write_pool))
}

impl SqliteBackedKeeper {
    /// Creates a new [`SqliteBackedKeeper`], running migrations against the given URL.
    pub async fn new(connection_url: &str) -> Result<Self> {
        let (read_pool, write_pool) = create_sqlite_pool(connection_url).await?;

        sqlx::migrate!("./../migrations/sqlite")
            .run(&write_pool)
            .await?;

        Ok(Self {
            read_pool,
            write_pool,
        })
    }
}

#[async_trait::async_trait]
impl super::Keeper for SqliteBackedKeeper {
    async fn keep(&self, id: &ObjectId, expiration_policy: ExpirationPolicy) -> Result<()> {
        if expiration_policy.is_manual() {
            return Ok(());
        }

        let expiration_duration: Option<i64> = expiration_policy.expires_in().and_then(|x| {
            x.as_secs()
                .try_into()
                .map_err(|_| Error::generic("expiration duration exceeds i64::MAX"))
                .ok()
        });

        let current_time: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::generic("system time before UNIX_EPOCH"))?
            .as_secs()
            .try_into()
            .map_err(|_| Error::generic("current time exceeds i64::MAX"))?;

        let expires_at = expiration_duration.map(|duration| current_time + duration);

        let mut atomic = self.write_pool.begin().await?;
        let _ = sqlx::query(
            "
            INSERT INTO ttl_keeper (object_id, expiration_policy, duration, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            ",
        )
        .bind(id.as_storage_path().to_string())
        .bind(match expiration_policy {
            ExpirationPolicy::Manual => 0,
            ExpirationPolicy::TimeToLive(_) => 1,
            ExpirationPolicy::TimeToIdle(_) => 2,
        })
        .bind(expiration_duration)
        .bind(current_time)
        .bind(expires_at)
        .execute(&mut *atomic)
        .await;

        atomic.commit().await?;

        Ok(())
    }

    async fn remove(&self, id: &ObjectId) -> Result<()> {
        let mut atomic = self.write_pool.begin().await?;
        let _ = sqlx::query(
            "
            DELETE FROM ttl_keeper
            WHERE object_id = ?
            ",
        )
        .bind(id.as_storage_path().to_string())
        .execute(&mut *atomic)
        .await;

        atomic.commit().await?;

        Ok(())
    }

    async fn mark_accessed(&self, id: &ObjectId) -> Result<()> {
        // Check what expiration policy is set for this object.
        let expiration_policy: Option<TableRow> = sqlx::query_as(
            "
            SELECT
                object_id,
                expiration_policy,
                duration,
                created_at,
                expires_at
            FROM ttl_keeper
            WHERE object_id = ?
            ",
        )
        .bind(id.as_storage_path().to_string())
        .fetch_optional(&self.read_pool)
        .await?;

        if let Some(expiration_policy) = expiration_policy {
            // We only check if the object's policy is TimeToIdle.
            // If it is, we need to extend the object's time to live.
            // For other policies, we do nothing.
            if expiration_policy.expiration_policy == 2 {
                // `expiration_policy.duration` should not be `None`.
                // If it is, we should not be here.
                let duration = match expiration_policy.duration {
                    Some(duration) => duration,
                    None => {
                        return Err(Error::generic(
                            "unwanted state for time to idle: duration is None",
                        ));
                    }
                };

                // If `expiration_policy.expires_at` is None, we should set it to `current_time + duration`.
                let current_time: i64 = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| Error::generic("system time before UNIX_EPOCH"))?
                    .as_secs()
                    .try_into()
                    .map_err(|_| Error::generic("current time exceeds i64::MAX"))?;

                // Update the object's expiration time.
                let mut atomic = self.write_pool.begin().await?;
                let _ = sqlx::query(
                    "
                    UPDATE ttl_keeper
                    SET expires_at = ?
                    WHERE object_id = ?
                    ",
                )
                .bind(current_time + duration)
                .bind(id.as_storage_path().to_string())
                .execute(&mut *atomic)
                .await;

                atomic.commit().await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::sqlite::SqlitePool;

    use objectstore_types::metadata::ExpirationPolicy;
    use objectstore_types::scope::{Scope, Scopes};

    use crate::id::{ObjectContext, ObjectId};
    use crate::keeper::Keeper;

    use super::*;

    fn make_id() -> ObjectId {
        let context = ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        };
        ObjectId::random(context)
    }

    struct TestKeeper {
        keeper: SqliteBackedKeeper,
        pool: SqlitePool,
    }

    impl TestKeeper {
        async fn new() -> Self {
            let pool = PoolOptions::<Sqlite>::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .expect("failed to create in-memory pool");

            sqlx::migrate!("./../migrations/sqlite")
                .run(&pool)
                .await
                .expect("failed to run migrations");

            let keeper = SqliteBackedKeeper {
                read_pool: pool.clone(),
                write_pool: pool.clone(),
            };

            Self { keeper, pool }
        }

        async fn fetch_row(&self, id: &ObjectId) -> Option<TableRow> {
            sqlx::query_as(
                "
                SELECT object_id, expiration_policy, duration, created_at, expires_at
                FROM ttl_keeper
                WHERE object_id = ?
                ",
            )
            .bind(id.as_storage_path().to_string())
            .fetch_optional(&self.pool)
            .await
            .expect("failed to fetch row")
        }
    }

    #[tokio::test]
    async fn keep_manual_is_noop() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper.keep(&id, ExpirationPolicy::Manual).await.unwrap();
        assert!(tk.fetch_row(&id).await.is_none());
    }

    #[tokio::test]
    async fn keep_ttl_inserts_row() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        tk.keeper
            .keep(&id, ExpirationPolicy::TimeToLive(Duration::from_secs(60)))
            .await
            .unwrap();

        let row = tk.fetch_row(&id).await.expect("row should exist");
        assert_eq!(row.expiration_policy, 1);
        assert_eq!(row.duration, Some(60));
        assert_eq!(row.expires_at, Some(row.created_at + 60));
        assert!(row.created_at >= before && row.created_at <= before + 2);
    }

    #[tokio::test]
    async fn keep_tti_inserts_row() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper
            .keep(&id, ExpirationPolicy::TimeToIdle(Duration::from_secs(60)))
            .await
            .unwrap();

        let row = tk.fetch_row(&id).await.expect("row should exist");
        assert_eq!(row.expiration_policy, 2);
        assert_eq!(row.duration, Some(60));
        assert_eq!(row.expires_at, Some(row.created_at + 60));
    }

    #[tokio::test]
    async fn remove_deletes_existing_row() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper
            .keep(&id, ExpirationPolicy::TimeToLive(Duration::from_secs(60)))
            .await
            .unwrap();
        assert!(tk.fetch_row(&id).await.is_some());

        tk.keeper.remove(&id).await.unwrap();
        assert!(tk.fetch_row(&id).await.is_none());
    }

    #[tokio::test]
    async fn remove_nonexistent_is_ok() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper.remove(&id).await.unwrap();
    }

    #[tokio::test]
    async fn mark_accessed_manual_is_noop() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper.keep(&id, ExpirationPolicy::Manual).await.unwrap();
        tk.keeper.mark_accessed(&id).await.unwrap();
        assert!(tk.fetch_row(&id).await.is_none());
    }

    #[tokio::test]
    async fn mark_accessed_ttl_is_noop() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper
            .keep(&id, ExpirationPolicy::TimeToLive(Duration::from_secs(60)))
            .await
            .unwrap();
        let before = tk.fetch_row(&id).await.unwrap();

        tk.keeper.mark_accessed(&id).await.unwrap();
        let after = tk.fetch_row(&id).await.unwrap();

        assert_eq!(before.expires_at, after.expires_at);
        assert_eq!(before.created_at, after.created_at);
        assert_eq!(before.duration, after.duration);
    }

    #[tokio::test]
    async fn mark_accessed_tti_without_expires_at_sets_it() {
        let tk = TestKeeper::new().await;
        let id = make_id();
        let id_str = id.as_storage_path().to_string();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Insert a TTI row directly with expires_at = NULL.
        sqlx::query(
            "
            INSERT INTO ttl_keeper (object_id, expiration_policy, duration, created_at, expires_at)
            VALUES (?, 2, 60, ?, NULL)
            ",
        )
        .bind(&id_str)
        .bind(now)
        .execute(&tk.pool)
        .await
        .unwrap();

        tk.keeper.mark_accessed(&id).await.unwrap();

        let row = tk.fetch_row(&id).await.unwrap();
        assert_eq!(row.expires_at, Some(now + 60));
    }

    #[tokio::test]
    async fn mark_accessed_tti_with_expires_at_bumps_it() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper
            .keep(&id, ExpirationPolicy::TimeToIdle(Duration::from_secs(60)))
            .await
            .unwrap();
        let original = tk.fetch_row(&id).await.unwrap();

        // Small delay so the timestamp changes.
        tokio::time::sleep(Duration::from_millis(50)).await;

        tk.keeper.mark_accessed(&id).await.unwrap();
        let updated = tk.fetch_row(&id).await.unwrap();

        // expires_at is always recomputed from current time + duration.
        assert!(updated.expires_at.unwrap() >= original.expires_at.unwrap());
        assert_eq!(updated.created_at, original.created_at);
    }

    #[tokio::test]
    async fn mark_accessed_nonexistent_is_ok() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper.mark_accessed(&id).await.unwrap();
    }
}
