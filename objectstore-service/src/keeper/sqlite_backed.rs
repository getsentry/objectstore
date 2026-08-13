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
    pub time_created: i64,
    /// Unix timestamp (seconds) when the object expires, if applicable.
    pub time_expires: Option<i64>,
}

/// SQLite-backed keeper that persists object retention state in a `ttl_keeper` table.
#[derive(Debug)]
pub struct SqliteBackedKeeper {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

const SQLITE_POLICY_MANUAL: i32 = 0;
const SQLITE_POLICY_TIME_TO_LIVE: i32 = 1;
const SQLITE_POLICY_TIME_TO_IDLE: i32 = 2;

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

        // XXX(aldy505): Since we convert the `expiration_policy.expires_in()` into `i64`
        // using `Duration.as_secs()`, there is a very small possibility of having it as a negative
        // number. When that happen, what should we do? Do we mark it as a manual expiration?
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

        let time_expires = expiration_duration.map(|duration| current_time + duration);

        let mut atomic = self.write_pool.begin().await?;
        sqlx::query(
            "
            INSERT INTO ttl_keeper (object_id, expiration_policy, duration, time_created, time_expires)
            VALUES (?, ?, ?, ?, ?)
            ",
        )
        .bind(id.as_storage_path().to_string())
        .bind(match expiration_policy {
            ExpirationPolicy::Manual => SQLITE_POLICY_MANUAL,
            ExpirationPolicy::TimeToLive(_) => SQLITE_POLICY_TIME_TO_LIVE,
            ExpirationPolicy::TimeToIdle(_) => SQLITE_POLICY_TIME_TO_IDLE,
        })
        .bind(expiration_duration)
        .bind(current_time)
        .bind(time_expires)
        .execute(&mut *atomic)
        .await?;

        atomic.commit().await?;

        Ok(())
    }

    async fn remove(&self, id: &ObjectId) -> Result<()> {
        let mut atomic = self.write_pool.begin().await?;
        sqlx::query(
            "
            DELETE FROM ttl_keeper
            WHERE object_id = ?
            ",
        )
        .bind(id.as_storage_path().to_string())
        .execute(&mut *atomic)
        .await?;

        atomic.commit().await?;

        Ok(())
    }

    async fn update(&self, id: &ObjectId, expiration_policy: ExpirationPolicy) -> Result<()> {
        // Check what expiration policy is set for this object.
        let keeper_row: Option<TableRow> = sqlx::query_as(
            "
            SELECT
                object_id,
                expiration_policy,
                duration,
                time_created,
                time_expires
            FROM ttl_keeper
            WHERE object_id = ?
            ",
        )
        .bind(id.as_storage_path().to_string())
        .fetch_optional(&self.read_pool)
        .await?;

        // The current time in UNIX seconds
        let current_time: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::generic("system time before UNIX_EPOCH"))?
            .as_secs()
            .try_into()
            .map_err(|_| Error::generic("current time exceeds i64::MAX"))?;

        if let Some(keeper_row) = keeper_row {
            // Check whether the object is expired. If it is, we should not update it.
            if (keeper_row.expiration_policy == SQLITE_POLICY_TIME_TO_LIVE
                || keeper_row.expiration_policy == SQLITE_POLICY_TIME_TO_IDLE)
                && let Some(time_expires) = keeper_row.time_expires
                && current_time >= time_expires
            {
                // The object is expired, we should not update it.
                return Ok(());
            }

            // Then, we update the expiration policy, duration, and the new time_expires if applicable.

            let expiration_duration: Option<i64> = expiration_policy.expires_in().and_then(|x| {
                x.as_secs()
                    .try_into()
                    .map_err(|_| Error::generic("expiration duration exceeds i64::MAX"))
                    .ok()
            });

            // For TTL, the expiration is fixed at creation time and must not change on
            // access. For TTI, the expiration is reset to now + duration on each access.
            let time_expires = match expiration_policy {
                ExpirationPolicy::Manual => None,
                ExpirationPolicy::TimeToLive(_) => {
                    expiration_duration.map(|duration| keeper_row.time_created + duration)
                }
                ExpirationPolicy::TimeToIdle(_) => {
                    expiration_duration.map(|duration| current_time + duration)
                }
            };

            // Update the object's expiration time.
            let mut atomic = self.write_pool.begin().await?;
            sqlx::query(
                "
                    UPDATE ttl_keeper
                    SET
                        expiration_policy = ?,
                        duration = ?,
                        time_expires = ?
                    WHERE object_id = ?
                    ",
            )
            .bind(match expiration_policy {
                ExpirationPolicy::Manual => SQLITE_POLICY_MANUAL,
                ExpirationPolicy::TimeToLive(_) => SQLITE_POLICY_TIME_TO_LIVE,
                ExpirationPolicy::TimeToIdle(_) => SQLITE_POLICY_TIME_TO_IDLE,
            })
            .bind(expiration_duration)
            .bind(time_expires)
            .bind(id.as_storage_path().to_string())
            .execute(&mut *atomic)
            .await?;

            atomic.commit().await?;
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
                SELECT object_id, expiration_policy, duration, time_created, time_expires
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
        assert_eq!(row.time_expires, Some(row.time_created + 60));
        assert!(row.time_created >= before && row.time_created <= before + 2);
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
        assert_eq!(row.time_expires, Some(row.time_created + 60));
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
    async fn update_manual_is_noop() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper.keep(&id, ExpirationPolicy::Manual).await.unwrap();
        tk.keeper
            .update(&id, ExpirationPolicy::Manual)
            .await
            .unwrap();
        assert!(tk.fetch_row(&id).await.is_none());
    }

    #[tokio::test]
    async fn update_ttl_is_noop() {
        let tk = TestKeeper::new().await;
        let id = make_id();
        let expiration_policy = ExpirationPolicy::TimeToLive(Duration::from_secs(60));
        tk.keeper.keep(&id, expiration_policy).await.unwrap();
        let before = tk.fetch_row(&id).await.unwrap();

        tk.keeper.update(&id, expiration_policy).await.unwrap();
        let after = tk.fetch_row(&id).await.unwrap();

        assert_eq!(before.time_expires, after.time_expires);
        assert_eq!(before.time_created, after.time_created);
        assert_eq!(before.duration, after.duration);
    }

    #[tokio::test]
    async fn update_tti_without_time_expires_sets_it() {
        let tk = TestKeeper::new().await;
        let id = make_id();
        let id_str = id.as_storage_path().to_string();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Insert a TTI row directly with time_expires = NULL.
        sqlx::query(
            "
            INSERT INTO ttl_keeper (object_id, expiration_policy, duration, time_created, time_expires)
            VALUES (?, 2, 60, ?, NULL)
            ",
        )
        .bind(&id_str)
        .bind(now)
        .execute(&tk.pool)
        .await
        .unwrap();

        tk.keeper
            .update(&id, ExpirationPolicy::TimeToIdle(Duration::from_secs(60)))
            .await
            .unwrap();

        let row = tk.fetch_row(&id).await.unwrap();
        assert_eq!(row.time_expires, Some(now + 60));
    }

    #[tokio::test]
    async fn update_tti_with_time_expires_bumps_it() {
        let tk = TestKeeper::new().await;
        let id = make_id();
        let expiration_policy = ExpirationPolicy::TimeToIdle(Duration::from_secs(60));

        tk.keeper.keep(&id, expiration_policy).await.unwrap();
        let original = tk.fetch_row(&id).await.unwrap();

        // Small delay so the timestamp changes.
        tokio::time::sleep(Duration::from_millis(50)).await;

        tk.keeper.update(&id, expiration_policy).await.unwrap();
        let updated = tk.fetch_row(&id).await.unwrap();

        // time_expires is always recomputed from current time + duration.
        assert!(updated.time_expires.unwrap() >= original.time_expires.unwrap());
        assert_eq!(updated.time_created, original.time_created);
    }

    #[tokio::test]
    async fn update_nonexistent_is_ok() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper
            .update(&id, ExpirationPolicy::Manual)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn keep_duplicate_returns_error() {
        let tk = TestKeeper::new().await;
        let id = make_id();

        tk.keeper
            .keep(&id, ExpirationPolicy::TimeToLive(Duration::from_secs(60)))
            .await
            .unwrap();

        let err = tk
            .keeper
            .keep(&id, ExpirationPolicy::TimeToLive(Duration::from_secs(60)))
            .await
            .unwrap_err();

        assert!(
            matches!(err, Error::Sqlx(_)),
            "expected Error::Sqlx, got: {err:?}"
        );
    }
}
