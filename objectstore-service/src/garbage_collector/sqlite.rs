use std::str::FromStr;

use async_trait::async_trait;
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

    async fn remove(&self, id: &ObjectId) {
        let pool = self.pool.clone();
        let id = id.clone();

        let _ = sqlx::query("DELETE FROM garbage_collector WHERE object_id = ?")
            .bind(id.as_storage_path().to_string())
            .execute(&pool)
            .await;
    }

    async fn list_expired(
        &self,
        now: objectstore_types::time::Timestamp,
        limit: u64,
    ) -> Result<Vec<ObjectId>, ChangeStreamError> {
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
            Err(error) => Err(ChangeStreamError::Failure(Box::new(error))),
        }
    }
}
