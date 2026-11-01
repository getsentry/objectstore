//! A [`ChangeStream`] that keeps the garbage collector's expiration index in sync.
//!
//! The garbage collector deletes objects once their deadline has passed, so it needs to
//! know about every object the backends write and every object they delete explicitly.
//! [`GcChangeStream`] forwards the relevant lifecycle events to a [`GcStorage`].

use std::sync::Arc;
use std::time::Duration;

use objectstore_types::time::Timestamp;
use tonic::async_trait;

use crate::change_stream::{ChangeStream, ChangeStreamError};
use crate::garbage_collector::GcStorage;
use crate::id::ObjectId;

/// Mirrors object lifetimes into [`GcStorage`] for the garbage collector.
///
/// Writes and updates are announced through [`begin_write`](ChangeStream::begin_write) and
/// [`begin_update`](ChangeStream::begin_update); both record `expires_at` because the
/// collector only needs the current deadline, and [`GcStorage::upsert`] already implements
/// the required "a shorter deadline never replaces a longer one" semantics. Explicit
/// deletions are handled in [`commit_delete`](ChangeStream::commit_delete).
///
/// The `commit_*` hooks for writes and updates are intentionally left as no-ops: the
/// deadline is recorded before the storage operation runs, so an object cannot be written
/// without a corresponding index entry. Recording it earlier only means a failed write may
/// leave a deadline for an object that never existed, which the collector can safely ignore
/// (and which a later, longer write overwrites).
///
/// All storage calls are awaited inline, so [`join`](ChangeStream::join) returns
/// immediately; there is no buffered work to drain.
#[derive(Debug)]
pub struct GcChangeStream {
    /// Expiration index that receives the forwarded changes.
    pub storage: Arc<dyn GcStorage>,
}

#[async_trait]
impl ChangeStream for GcChangeStream {
    async fn begin_write(
        &self,
        id: &ObjectId,
        expires_at: Option<Timestamp>,
    ) -> Result<(), ChangeStreamError> {
        self.storage.upsert(id, expires_at).await
    }

    async fn begin_update(
        &self,
        id: &ObjectId,
        expires_at: Option<Timestamp>,
    ) -> Result<(), ChangeStreamError> {
        self.storage.upsert(id, expires_at).await
    }

    async fn commit_delete(&self, id: &ObjectId) {
        self.storage.remove(id).await
    }

    async fn join(&self, _timeout: Duration) {}
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::id::ObjectContext;

    fn ts(secs: u64) -> Timestamp {
        Timestamp::from_unix_secs(secs).unwrap()
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

    /// A [`GcStorage`] that records the calls it receives instead of persisting anything.
    #[derive(Debug, Default)]
    struct RecordingStorage {
        calls: Mutex<Vec<Call>>,
        fail_upsert: bool,
    }

    #[derive(Debug, PartialEq)]
    enum Call {
        Upsert(ObjectId, Option<Timestamp>),
        Remove(ObjectId),
    }

    impl RecordingStorage {
        fn calls(&self) -> Vec<Call> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }
    }

    #[async_trait]
    impl GcStorage for RecordingStorage {
        async fn upsert(
            &self,
            id: &ObjectId,
            expires_at: Option<Timestamp>,
        ) -> Result<(), ChangeStreamError> {
            if self.fail_upsert {
                return Err(ChangeStreamError::Unavailable("storage down".into()));
            }
            self.calls
                .lock()
                .unwrap()
                .push(Call::Upsert(id.clone(), expires_at));
            Ok(())
        }

        async fn remove(&self, id: &ObjectId) {
            self.calls.lock().unwrap().push(Call::Remove(id.clone()));
        }

        async fn list_expired(
            &self,
            _now: Timestamp,
            _limit: u64,
        ) -> Result<Vec<ObjectId>, ChangeStreamError> {
            Ok(Vec::new())
        }
    }

    fn stream(storage: Arc<RecordingStorage>) -> GcChangeStream {
        GcChangeStream {
            storage: storage as Arc<dyn GcStorage>,
        }
    }

    #[tokio::test]
    async fn begin_write_records_expiration() {
        let storage = Arc::new(RecordingStorage::default());
        let id = id_for("written");

        stream(storage.clone())
            .begin_write(&id, Some(ts(100)))
            .await
            .unwrap();

        assert_eq!(storage.calls(), vec![Call::Upsert(id, Some(ts(100)))]);
    }

    #[tokio::test]
    async fn begin_update_records_expiration() {
        let storage = Arc::new(RecordingStorage::default());
        let id = id_for("updated");

        stream(storage.clone())
            .begin_update(&id, Some(ts(200)))
            .await
            .unwrap();

        assert_eq!(storage.calls(), vec![Call::Upsert(id, Some(ts(200)))]);
    }

    #[tokio::test]
    async fn begin_hooks_forward_cleared_expiration() {
        let storage = Arc::new(RecordingStorage::default());
        let id = id_for("permanent");

        stream(storage.clone())
            .begin_update(&id, None)
            .await
            .unwrap();

        assert_eq!(storage.calls(), vec![Call::Upsert(id, None)]);
    }

    #[tokio::test]
    async fn commits_do_not_touch_storage() {
        let storage = Arc::new(RecordingStorage::default());
        let stream = stream(storage.clone());
        let id = id_for("committed");

        stream.commit_write(&id, 42, Some(ts(100))).await;
        stream.commit_update(&id, Some(ts(200))).await;

        assert!(storage.calls().is_empty());
    }

    #[tokio::test]
    async fn commit_delete_removes_object() {
        let storage = Arc::new(RecordingStorage::default());
        let id = id_for("deleted");

        stream(storage.clone()).commit_delete(&id).await;

        assert_eq!(storage.calls(), vec![Call::Remove(id)]);
    }

    #[tokio::test]
    async fn begin_hooks_propagate_storage_failures() {
        let storage = Arc::new(RecordingStorage {
            fail_upsert: true,
            ..Default::default()
        });
        let stream = stream(storage.clone());
        let id = id_for("failing");

        let write = stream.begin_write(&id, Some(ts(100))).await;
        let update = stream.begin_update(&id, Some(ts(100))).await;

        // A failing `begin_*` must fail the request before storage is touched.
        assert!(matches!(write, Err(ChangeStreamError::Unavailable(_))));
        assert!(matches!(update, Err(ChangeStreamError::Unavailable(_))));
        assert!(storage.calls().is_empty());
    }

    #[tokio::test]
    async fn join_returns_without_waiting_for_timeout() {
        let stream = stream(Arc::new(RecordingStorage::default()));

        // `join` has nothing buffered, so it must return well before the timeout it is given.
        tokio::time::timeout(Duration::from_secs(1), stream.join(Duration::from_secs(60)))
            .await
            .expect("join should return immediately");
    }
}
