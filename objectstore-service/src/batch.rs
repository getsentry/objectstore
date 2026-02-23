//! Batch operation types and concurrent executor.
//!
//! [`BatchExecutor`] processes a stream of [`Operation`]s concurrently within
//! a bounded window, derived from the current service capacity at construction
//! time.

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use objectstore_types::metadata::Metadata;

use crate::PayloadStream;
use crate::error::{Error, Result};
use crate::id::{ObjectContext, ObjectId, ObjectKey};
use crate::service::{DeleteResponse, GetResponse, InsertResponse, StorageService};

/// An insert operation: stores an object at the given key.
#[derive(Debug)]
pub struct Insert {
    /// The key to store the object under.
    pub key: ObjectKey,
    /// Metadata for the object.
    pub metadata: Metadata,
    /// The object payload. Batch inserts are fully buffered (≤1 MiB).
    pub payload: Bytes,
}

/// A get operation: retrieves an existing object by key.
#[derive(Debug)]
pub struct Get {
    /// The key of the object to retrieve.
    pub key: ObjectKey,
}

/// A delete operation: removes an object by key.
#[derive(Debug)]
pub struct Delete {
    /// The key of the object to delete.
    pub key: ObjectKey,
}

/// A single operation in a batch request.
#[derive(Debug)]
pub enum Operation {
    /// Insert a new object.
    Insert(Insert),
    /// Get an existing object.
    Get(Get),
    /// Delete an object.
    Delete(Delete),
}

impl Operation {
    /// Returns the key for this operation.
    pub fn key(&self) -> &ObjectKey {
        match self {
            Operation::Insert(op) => &op.key,
            Operation::Get(op) => &op.key,
            Operation::Delete(op) => &op.key,
        }
    }

    /// Returns the kind name for this operation.
    pub fn kind(&self) -> &'static str {
        match self {
            Operation::Insert(_) => "insert",
            Operation::Get(_) => "get",
            Operation::Delete(_) => "delete",
        }
    }
}

/// The outcome of a single batch operation.
#[derive(Debug)]
pub struct Outcome {
    /// The fully-qualified object identifier, always populated for correlation.
    pub id: ObjectId,
    /// The operation result.
    pub result: OperationResult,
}

/// The result of a single batch operation.
pub enum OperationResult {
    /// Result of an insert operation.
    Inserted(Result<InsertResponse>),
    /// Result of a get operation.
    Got(Result<GetResponse>),
    /// Result of a delete operation.
    Deleted(Result<DeleteResponse>),
}

impl std::fmt::Debug for OperationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationResult::Inserted(r) => f.debug_tuple("Inserted").field(r).finish(),
            OperationResult::Got(Ok(Some(_))) => {
                f.debug_tuple("Got").field(&"Ok(Some(<stream>))").finish()
            }
            OperationResult::Got(Ok(None)) => f.debug_tuple("Got").field(&"Ok(None)").finish(),
            OperationResult::Got(Err(e)) => f.debug_tuple("Got").field(&Err::<(), _>(e)).finish(),
            OperationResult::Deleted(r) => f.debug_tuple("Deleted").field(r).finish(),
        }
    }
}

/// Executes batch operations with bounded concurrency.
///
/// The concurrency window is derived from the service's available permits at
/// construction time: `ceil(available * 0.10)`, clamped to `[1, 50]`.
///
/// Returns [`Error::AtCapacity`] from [`new`](BatchExecutor::new) when no
/// permits are available at construction time, allowing the caller to reject
/// the entire batch immediately.
///
/// Internally uses [`buffer_unordered`](StreamExt::buffer_unordered) to drive
/// up to `window` operations concurrently. The input stream is pulled lazily —
/// at most `window` operations are in-flight at once, bounding memory to
/// roughly `window × max_operation_size`.
#[derive(Debug)]
pub struct BatchExecutor {
    window: usize,
}

impl BatchExecutor {
    /// Creates a new executor from the service's current permit availability.
    ///
    /// Returns [`Error::AtCapacity`] if no permits are available.
    pub fn new(service: &StorageService) -> Result<Self> {
        let available = service.available_permits();
        if available == 0 {
            return Err(Error::AtCapacity);
        }
        let window = ((available as f64 * 0.10).ceil() as usize).clamp(1, 50);
        Ok(Self { window })
    }

    /// Returns the concurrency window computed at construction.
    pub fn window(&self) -> usize {
        self.window
    }

    /// Executes operations with bounded concurrency.
    ///
    /// Pulls from `operations` at its own pace — at most `window` operations
    /// are in-flight simultaneously. Results are yielded in completion order.
    pub fn execute(
        self,
        service: StorageService,
        context: ObjectContext,
        operations: impl Stream<Item = Operation> + Send + 'static,
    ) -> impl Stream<Item = Outcome> + Send + 'static {
        let window = self.window;
        operations
            .map(move |op| {
                let service = service.clone();
                let context = context.clone();
                async move { execute_single(service, context, op).await }
            })
            .buffer_unordered(window)
    }
}

async fn execute_single(service: StorageService, context: ObjectContext, op: Operation) -> Outcome {
    match op {
        Operation::Get(get) => {
            let id = ObjectId::new(context, get.key);
            let result = service.get_object(id.clone()).await;
            Outcome {
                id,
                result: OperationResult::Got(result),
            }
        }
        Operation::Insert(insert) => {
            let key = insert.key;
            let id = ObjectId::new(context.clone(), key.clone());
            let stream: PayloadStream =
                futures_util::stream::once(futures_util::future::ready(Ok(insert.payload))).boxed();
            let result = service
                .insert_object(context, Some(key), insert.metadata, stream)
                .await;
            Outcome {
                id,
                result: OperationResult::Inserted(result),
            }
        }
        Operation::Delete(delete) => {
            let id = ObjectId::new(context, delete.key);
            let result = service.delete_object(id.clone()).await;
            Outcome {
                id,
                result: OperationResult::Deleted(result),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use futures_util::StreamExt;
    use objectstore_types::metadata::Metadata;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::error::Error;

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_service_with_limit(limit: usize) -> StorageService {
        let hv = InMemoryBackend::new("in-memory-hv");
        let lt = InMemoryBackend::new("in-memory-lt");
        StorageService::from_backends(Box::new(hv), Box::new(lt)).with_concurrency_limit(limit)
    }

    fn make_service() -> StorageService {
        make_service_with_limit(500)
    }

    // --- BatchExecutor::new() window and capacity tests ---

    #[test]
    fn at_capacity_when_no_permits() {
        let service = make_service_with_limit(0);
        let result = BatchExecutor::new(&service);
        assert!(matches!(result, Err(Error::AtCapacity)));
    }

    #[test]
    fn window_computation() {
        // 10 available → ceil(10 × 0.10) = 1
        let s = make_service_with_limit(10);
        assert_eq!(BatchExecutor::new(&s).unwrap().window(), 1);

        // 100 available → ceil(100 × 0.10) = 10
        let s = make_service_with_limit(100);
        assert_eq!(BatchExecutor::new(&s).unwrap().window(), 10);

        // 500 available → ceil(500 × 0.10) = 50
        let s = make_service_with_limit(500);
        assert_eq!(BatchExecutor::new(&s).unwrap().window(), 50);
    }

    #[test]
    fn window_clamped_to_min() {
        // 1–9 available → ceil(N × 0.10) < 1 → clamped to 1
        for n in 1..=9 {
            let s = make_service_with_limit(n);
            let w = BatchExecutor::new(&s).unwrap().window();
            assert_eq!(w, 1, "expected window 1 for {n} permits, got {w}");
        }
    }

    #[test]
    fn window_clamped_to_max() {
        // 501+ available → ceil(N × 0.10) > 50 → clamped to 50
        for n in [501, 1000, 10000] {
            let s = make_service_with_limit(n);
            let w = BatchExecutor::new(&s).unwrap().window();
            assert_eq!(w, 50, "expected window 50 for {n} permits, got {w}");
        }
    }

    // --- BatchExecutor::execute() correctness tests ---

    #[tokio::test]
    async fn execute_empty_stream() {
        let service = make_service();
        let executor = BatchExecutor::new(&service).unwrap();
        let outcomes: Vec<_> = executor
            .execute(service, make_context(), futures_util::stream::empty())
            .collect()
            .await;
        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn execute_runs_all_operations() {
        let service = make_service();
        let context = make_context();

        // Seed an object to retrieve and delete.
        service
            .insert_object(
                context.clone(),
                Some("key1".into()),
                Metadata::default(),
                futures_util::stream::once(async { Ok(Bytes::from("hello")) }).boxed(),
            )
            .await
            .unwrap();

        let ops: Vec<Operation> = vec![
            Operation::Get(Get { key: "key1".into() }),
            Operation::Get(Get {
                key: "nonexistent".into(),
            }),
            Operation::Insert(Insert {
                key: "key2".into(),
                metadata: Metadata::default(),
                payload: Bytes::from("world"),
            }),
            Operation::Delete(Delete { key: "key1".into() }),
        ];

        let executor = BatchExecutor::new(&service).unwrap();
        let outcomes: Vec<_> = executor
            .execute(service, context, futures_util::stream::iter(ops))
            .collect()
            .await;

        assert_eq!(outcomes.len(), 4);

        // Every outcome must carry a key.
        for outcome in &outcomes {
            assert!(!outcome.id.key.is_empty(), "outcome id must have a key");
        }
    }

    // --- Service-level concurrent execution and capacity tests ---

    /// A backend that pauses on `put_object` and signals via a channel, then
    /// waits for a shared `Notify` before completing.
    struct GatedBackend {
        inner: InMemoryBackend,
        paused_tx: tokio::sync::mpsc::Sender<()>,
        resume: Arc<tokio::sync::Notify>,
        in_flight: Arc<AtomicUsize>,
    }

    impl std::fmt::Debug for GatedBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("GatedBackend").finish()
        }
    }

    #[async_trait::async_trait]
    impl crate::backend::common::Backend for GatedBackend {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        async fn put_object(
            &self,
            id: &ObjectId,
            metadata: &Metadata,
            stream: PayloadStream,
        ) -> Result<()> {
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            let _ = self.paused_tx.send(()).await;
            self.resume.notified().await;
            let result = self.inner.put_object(id, metadata, stream).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn get_object(&self, id: &ObjectId) -> Result<Option<(Metadata, PayloadStream)>> {
            self.inner.get_object(id).await
        }

        async fn delete_object(&self, id: &ObjectId) -> Result<()> {
            self.inner.delete_object(id).await
        }
    }

    #[tokio::test]
    async fn concurrent_execution() {
        // Window = ceil(100 × 0.10) = 10.
        let (paused_tx, mut paused_rx) = tokio::sync::mpsc::channel::<()>(20);
        let resume = Arc::new(tokio::sync::Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));

        let gated = GatedBackend {
            inner: InMemoryBackend::new("gated-hv"),
            paused_tx,
            resume: Arc::clone(&resume),
            in_flight: Arc::clone(&in_flight),
        };
        let lt = InMemoryBackend::new("in-memory-lt");
        let service = StorageService::from_backends(Box::new(gated), Box::new(lt))
            .with_concurrency_limit(100);

        let executor = BatchExecutor::new(&service).unwrap();
        assert_eq!(executor.window(), 10);

        // Submit 10 inserts. With window=10, all should be in-flight simultaneously.
        let ops = futures_util::stream::iter((0..10).map(|i| {
            Operation::Insert(Insert {
                key: format!("key{i}"),
                metadata: Metadata::default(),
                payload: Bytes::from(format!("data{i}")),
            })
        }));

        let exec_handle = tokio::spawn(async move {
            executor
                .execute(service, make_context(), ops)
                .collect::<Vec<_>>()
                .await
        });

        // Wait for all 10 operations to pause inside the backend.
        for _ in 0..10 {
            paused_rx.recv().await.unwrap();
        }
        assert_eq!(in_flight.load(Ordering::SeqCst), 10);

        // Release all.
        resume.notify_waiters();

        let outcomes = exec_handle.await.unwrap();
        assert_eq!(outcomes.len(), 10);
        for outcome in &outcomes {
            assert!(
                matches!(&outcome.result, OperationResult::Inserted(Ok(_))),
                "unexpected result: {:?}",
                outcome.result
            );
        }
    }

    #[tokio::test]
    async fn at_capacity_within_batch() {
        // Service with limit=2; executor window=1 (ceil(2 × 0.10) = 1).
        // Fill both permits with background inserts that block, then execute
        // a batch — all operations should fail with AtCapacity while the
        // executor continues to process the entire stream.
        let (paused_tx, mut paused_rx) = tokio::sync::mpsc::channel::<()>(10);
        let resume = Arc::new(tokio::sync::Notify::new());

        let gated = GatedBackend {
            inner: InMemoryBackend::new("gated-cap"),
            paused_tx,
            resume: Arc::clone(&resume),
            in_flight: Arc::new(AtomicUsize::new(0)),
        };
        let lt = InMemoryBackend::new("in-memory-lt");
        let service =
            StorageService::from_backends(Box::new(gated), Box::new(lt)).with_concurrency_limit(2);

        let executor = BatchExecutor::new(&service).unwrap();

        // Hold both permits via background inserts that block in the gated backend.
        let svc1 = service.clone();
        tokio::spawn(async move {
            let _ = svc1
                .insert_object(
                    make_context(),
                    Some("blocker1".into()),
                    Metadata::default(),
                    futures_util::stream::once(async { Ok(Bytes::from("x")) }).boxed(),
                )
                .await;
        });
        let svc2 = service.clone();
        tokio::spawn(async move {
            let _ = svc2
                .insert_object(
                    make_context(),
                    Some("blocker2".into()),
                    Metadata::default(),
                    futures_util::stream::once(async { Ok(Bytes::from("x")) }).boxed(),
                )
                .await;
        });

        // Wait until both permits are held.
        paused_rx.recv().await.unwrap();
        paused_rx.recv().await.unwrap();

        // Both permits are held. Batch operations should all fail with AtCapacity,
        // but the executor should yield an outcome for every submitted operation.
        let ops = futures_util::stream::iter([
            Operation::Get(Get { key: "k1".into() }),
            Operation::Get(Get { key: "k2".into() }),
        ]);
        let outcomes: Vec<_> = executor
            .execute(service, make_context(), ops)
            .collect()
            .await;

        assert_eq!(
            outcomes.len(),
            2,
            "executor must yield an outcome for every op"
        );
        for outcome in &outcomes {
            assert!(
                matches!(
                    &outcome.result,
                    OperationResult::Got(Err(Error::AtCapacity))
                ),
                "expected AtCapacity, got {:?}",
                outcome.result
            );
        }

        resume.notify_waiters();
    }
}
