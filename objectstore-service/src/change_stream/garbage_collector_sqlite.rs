use crate::change_stream::ChangeStream;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{Connection, SqlitePool};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
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

pub struct SqliteGarbageCollectorStream {
    pool: SqlitePool,
    active_tasks: Arc<AtomicUsize>,
}

impl SqliteGarbageCollectorStream {
    pub async fn new(config: &SqliteGarbageCollectorConfig) -> Result<Self, sqlx::Error> {
        let pool = SqlitePool::connect(&format!("sqlite://{}", &config.path)).await?;

        sqlx::migrate!("./../migrations/sqlite").run(&pool).await?;

        Ok(Self {
            pool,
            active_tasks: Arc::new(AtomicUsize::new(0)),
        })
    }
}

impl fmt::Debug for SqliteGarbageCollectorStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteGarbageCollector").finish()
    }
}

#[async_trait]
impl ChangeStream for SqliteGarbageCollectorStream {
    fn write(
        &self,
        id: &crate::id::ObjectId,
        size: u64,
        expires_at: Option<objectstore_types::time::Timestamp>,
    ) {
        let pool = self.pool.clone();
        let id = id.clone();

        // Increment counter
        let active_tasks = Arc::clone(&self.active_tasks);
        active_tasks.fetch_add(1, Ordering::SeqCst);

        tokio::spawn(async move {
            let result = async {
                let connection = pool.acquire().await?;
                let tx_db = connection.begin().await?;

                sqlx::query("INSERT INTO garbage_collector (object_id, expires_at) VALUES (?, ?)")
                    .bind(id.as_storage_path())
                    .bind(expires_at.map(|t| t.as_secs()))
                    .execute(&tx_db)
                    .await?;

                tx_db.commit().await?;

                Ok::<(), sqlx::Error>(())
            }
            .await;

            // Decrement counter when done
            active_tasks.fetch_sub(1, Ordering::SeqCst);
        });
    }

    fn update(
        &self,
        id: &crate::id::ObjectId,
        expires_at: Option<objectstore_types::time::Timestamp>,
    ) {
        let pool = self.pool.clone();
        let id = id.clone();

        // Increment counter
        let active_tasks = Arc::clone(&self.active_tasks);
        active_tasks.fetch_add(1, Ordering::SeqCst);

        tokio::spawn(async move {
            let result = async {
                let connection = pool.acquire().await?;
                let tx_db = connection.begin().await?;

                sqlx::query("UPDATE garbage_collector SET expires_at = ? WHERE object_id = ?")
                    .bind(expires_at.map(|t| t.as_secs()))
                    .bind(id.as_storage_path())
                    .execute(&tx_db)
                    .await?;

                tx_db.commit().await?;

                Ok::<(), sqlx::Error>(())
            }
            .await;

            active_tasks.fetch_sub(1, Ordering::SeqCst);
        });
    }

    fn delete(&self, id: &crate::id::ObjectId) {
        let pool = self.pool.clone();
        let id = id.clone();

        // Increment counter
        let active_tasks = Arc::clone(&self.active_tasks);
        active_tasks.fetch_add(1, Ordering::SeqCst);

        tokio::spawn(async move {
            let result = async {
                let connection = pool.acquire().await?;
                let tx_db = connection.begin().await?;

                sqlx::query("DELETE FROM garbage_collector WHERE object_id = ?")
                    .bind(id.as_storage_path())
                    .execute(&tx_db)
                    .await?;

                tx_db.commit().await?;

                Ok::<(), sqlx::Error>(())
            }
            .await;

            active_tasks.fetch_sub(1, Ordering::SeqCst);
        });
    }

    async fn join(&self, timeout: std::time::Duration) {
        let start = std::time::Instant::now();

        loop {
            if self.active_tasks.load(Ordering::SeqCst) == 0 {
                tracing::info!("All garbage collector tasks completed");
                return;
            }

            if start.elapsed() > timeout {
                let remaining = self.active_tasks.load(Ordering::SeqCst);
                tracing::warn!(
                    "Timeout waiting for garbage collector: {} tasks still pending",
                    remaining
                );
                return;
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    fn create_test_config() -> SqliteGarbageCollectorConfig {
        let temp_file = NamedTempFile::new().unwrap();
        SqliteGarbageCollectorConfig {
            path: temp_file.path().to_str().unwrap().to_string(),
        }
    }

    fn create_test_id(s: &str) -> crate::id::ObjectId {
        crate::id::ObjectId::new(crate::id::ObjectContext::default(), s.to_string())
    }

    #[tokio::test]
    async fn test_new_creates_stream() {
        let config = create_test_config();
        let result = SqliteGarbageCollectorStream::new(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_write_increments_task_counter() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        let id = create_test_id("test-object-1");
        assert_eq!(stream.active_tasks.load(Ordering::SeqCst), 0);

        stream.write(&id, 1024, None);

        // Counter should increment immediately
        assert_eq!(stream.active_tasks.load(Ordering::SeqCst), 1);

        // Wait for task to complete
        stream.join(Duration::from_secs(5)).await;
        assert_eq!(stream.active_tasks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_multiple_writes() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        for i in 0..5 {
            let id = create_test_id(&format!("test-object-{}", i));
            stream.write(&id, 1024 * (i + 1) as u64, None);
        }

        // All 5 tasks should be active
        assert_eq!(stream.active_tasks.load(Ordering::SeqCst), 5);

        // Wait for all to complete
        stream.join(Duration::from_secs(5)).await;
        assert_eq!(stream.active_tasks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_write_with_expiration() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        let id = create_test_id("test-object-expiring");
        let expires_at = Some(objectstore_types::time::Timestamp::from_secs(1234567890));

        stream.write(&id, 2048, expires_at);

        stream.join(Duration::from_secs(5)).await;
        assert_eq!(stream.active_tasks.load(Ordering::SeqCst), 0);

        // Verify the record was inserted
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM garbage_collector WHERE object_id = ?")
                .bind(id.as_storage_path())
                .fetch_one(&stream.pool)
                .await
                .unwrap();

        assert_eq!(count.0, 1);
    }

    #[tokio::test]
    async fn test_update() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        let id = create_test_id("test-update-object");
        let initial_expires = Some(objectstore_types::time::Timestamp::from_secs(1000000));
        let updated_expires = Some(objectstore_types::time::Timestamp::from_secs(2000000));

        // First write the record
        stream.write(&id, 1024, initial_expires);
        stream.join(Duration::from_secs(5)).await;

        // Update it
        stream.update(&id, updated_expires);
        stream.join(Duration::from_secs(5)).await;

        // Verify the update
        let result: (i64,) =
            sqlx::query_as("SELECT expires_at FROM garbage_collector WHERE object_id = ?")
                .bind(id.as_storage_path())
                .fetch_one(&stream.pool)
                .await
                .unwrap();

        assert_eq!(result.0, 2000000);
    }

    #[tokio::test]
    async fn test_delete() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        let id = create_test_id("test-delete-object");

        // Write the record
        stream.write(&id, 1024, None);
        stream.join(Duration::from_secs(5)).await;

        // Verify it exists
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM garbage_collector WHERE object_id = ?")
                .bind(id.as_storage_path())
                .fetch_one(&stream.pool)
                .await
                .unwrap();
        assert_eq!(count.0, 1);

        // Delete it
        stream.delete(&id);
        stream.join(Duration::from_secs(5)).await;

        // Verify it's gone
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM garbage_collector WHERE object_id = ?")
                .bind(id.as_storage_path())
                .fetch_one(&stream.pool)
                .await
                .unwrap();
        assert_eq!(count.0, 0);
    }

    #[tokio::test]
    async fn test_mixed_operations() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        let id1 = create_test_id("mixed-1");
        let id2 = create_test_id("mixed-2");
        let id3 = create_test_id("mixed-3");

        // Write 3 records
        stream.write(&id1, 1024, None);
        stream.write(&id2, 2048, None);
        stream.write(&id3, 4096, None);

        // Update one
        stream.update(
            &id2,
            Some(objectstore_types::time::Timestamp::from_secs(9999)),
        );

        // Delete one
        stream.delete(&id1);

        stream.join(Duration::from_secs(5)).await;

        // Verify state
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM garbage_collector")
            .fetch_one(&stream.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 2); // Only id2 and id3 remain
    }

    #[tokio::test]
    async fn test_join_timeout() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        let id = create_test_id("timeout-test");
        stream.write(&id, 1024, None);

        // Very short timeout (should still complete since task is fast)
        let start = std::time::Instant::now();
        stream.join(Duration::from_millis(1)).await;
        let elapsed = start.elapsed();

        // Should timeout quickly
        assert!(elapsed < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let config = create_test_config();
        let stream = std::sync::Arc::new(SqliteGarbageCollectorStream::new(&config).await.unwrap());

        let mut handles = vec![];

        // Spawn 10 concurrent tasks doing writes
        for i in 0..10 {
            let stream_clone = std::sync::Arc::clone(&stream);
            let handle = tokio::spawn(async move {
                let id = create_test_id(&format!("concurrent-{}", i));
                stream_clone.write(&id, 1024 * (i + 1) as u64, None);
            });
            handles.push(handle);
        }

        // Wait for all spawns to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // All writes should still be processing
        let active = stream.active_tasks.load(Ordering::SeqCst);
        assert!(active > 0);

        // Wait for all to complete
        stream.join(Duration::from_secs(5)).await;
        assert_eq!(stream.active_tasks.load(Ordering::SeqCst), 0);

        // Verify all records were inserted
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM garbage_collector")
            .fetch_one(&stream.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 10);
    }

    #[tokio::test]
    async fn test_join_waits_for_completion() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        let id = create_test_id("wait-test");
        stream.write(&id, 1024, None);

        // join() should wait until all tasks complete
        let start = std::time::Instant::now();
        stream.join(Duration::from_secs(10)).await;
        let elapsed = start.elapsed();

        // Should complete almost immediately (task is fast)
        assert!(elapsed < Duration::from_secs(1));
        assert_eq!(stream.active_tasks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_debug_impl() {
        let config = create_test_config();
        let stream = SqliteGarbageCollectorStream::new(&config).await.unwrap();

        let debug_str = format!("{:?}", stream);
        assert!(debug_str.contains("SqliteGarbageCollector"));
    }
}
