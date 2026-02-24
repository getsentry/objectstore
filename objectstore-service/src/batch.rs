//! Batch operation types and concurrent executor.
//!
//! [`BatchExecutor`] processes a stream of `(idx, Result<`[`Operation`]`, E>)` tuples
//! concurrently within a bounded window. Errors in the input stream pass through
//! unchanged; successful operations are executed against `TieredStorage` directly,
//! with [`tokio::spawn`] for panic isolation and run-to-completion guarantees.
//!
//! ## Window and Permit Reservation
//!
//! The concurrency window is derived from the service's available permits at the time
//! [`StorageService::batch`](crate::service::StorageService::batch) is called: `ceil(tasks_available × 0.10)`, clamped to
//! `[1, 50]`. The executor pre-acquires exactly `window` permits from the service's
//! `ConcurrencyLimiter` as a single bulk reservation. These permits are held for the
//! lifetime of the returned stream — released atomically when the stream is fully
//! consumed or dropped.
//!
//! This means:
//! - If the service is at capacity, [`StorageService::batch`](crate::service::StorageService::batch) fails immediately with
//!   [`Error::AtCapacity`] before any operations are read.
//! - During execution, operations call the storage backend directly without acquiring
//!   additional per-operation permits.
//!
//! ## Concurrency Model
//!
//! [`BatchExecutor::execute`] uses `buffer_unordered` to drive up to `window`
//! operations concurrently. The input stream is pulled lazily — at most `window`
//! operations are in-flight at once, bounding memory to roughly
//! `window × max_operation_size`. Results are yielded in completion order.
//!
//! Each operation is wrapped in a [`tokio::spawn`] for panic isolation: a panic in
//! one operation surfaces as [`Error::Panic`] for that item and does not affect the
//! others.
//!
//! ## Future Scope
//!
//! The window fraction (10%) is hard-coded. Configurable fractions, adaptive window
//! sizing, and backend-level batching optimizations (e.g. BigTable multi-read, GCS
//! batch API) are out of scope for the current implementation.

use std::sync::Arc;

use futures_util::{Stream, StreamExt};
use objectstore_types::metadata::Metadata;
use tokio::sync::OwnedSemaphorePermit;

use crate::PayloadStream;
use crate::error::{Error, Result};
use crate::id::{ObjectContext, ObjectId, ObjectKey};
use crate::service::GetResponse;
use crate::tiered::TieredStorage;

/// An insert operation: stores an object at the given key.
#[derive(Debug)]
pub struct Insert {
    /// The key to store the object under.
    pub key: ObjectKey,
    /// Metadata for the object.
    pub metadata: Metadata,
    /// The object payload. Batch inserts are fully buffered (≤1 MiB).
    pub payload: bytes::Bytes,
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

    /// Returns the permission required to perform this operation.
    pub fn permission(&self) -> objectstore_types::auth::Permission {
        match self {
            Operation::Get(_) => objectstore_types::auth::Permission::ObjectRead,
            Operation::Insert(_) => objectstore_types::auth::Permission::ObjectWrite,
            Operation::Delete(_) => objectstore_types::auth::Permission::ObjectDelete,
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

/// The response of a single executed batch operation.
///
/// Each variant carries the fields needed to render a response part.
/// The kind (`"insert"`, `"get"`, `"delete"`) is derivable via [`OpResponse::kind`].
pub enum OpResponse {
    /// An insert completed successfully.
    Inserted {
        /// The fully-qualified identifier assigned to the inserted object.
        id: ObjectId,
    },
    /// A get completed.
    Got {
        /// The key that was looked up.
        key: ObjectKey,
        /// The object content, or `None` if the object was not found.
        response: GetResponse,
    },
    /// A delete completed successfully.
    Deleted {
        /// The key that was deleted.
        key: ObjectKey,
    },
}

impl OpResponse {
    /// Returns the operation kind name.
    pub fn kind(&self) -> &'static str {
        match self {
            OpResponse::Inserted { .. } => "insert",
            OpResponse::Got { .. } => "get",
            OpResponse::Deleted { .. } => "delete",
        }
    }

    /// Returns the object key for this response.
    pub fn key(&self) -> &ObjectKey {
        match self {
            OpResponse::Inserted { id } => &id.key,
            OpResponse::Got { key, .. } => key,
            OpResponse::Deleted { key } => key,
        }
    }
}

impl std::fmt::Debug for OpResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpResponse::Inserted { id } => f.debug_struct("Inserted").field("id", id).finish(),
            OpResponse::Got {
                key,
                response: Some(_),
            } => f
                .debug_struct("Got")
                .field("key", key)
                .field("response", &"Some(<stream>)")
                .finish(),
            OpResponse::Got {
                key,
                response: None,
            } => f
                .debug_struct("Got")
                .field("key", key)
                .field("response", &"None")
                .finish(),
            OpResponse::Deleted { key } => f.debug_struct("Deleted").field("key", key).finish(),
        }
    }
}

/// Executes batch operations with bounded concurrency.
///
/// Construct via [`StorageService::batch`](crate::service::StorageService::batch),
/// which pre-acquires the concurrency window from the service's available permits.
///
/// See the [module documentation](self) for a full description of the window
/// calculation, permit reservation, and concurrency model.
#[derive(Debug)]
pub struct BatchExecutor {
    pub(crate) tiered: Arc<TieredStorage>,
    pub(crate) window: usize,
    pub(crate) reservation: OwnedSemaphorePermit,
}

impl BatchExecutor {
    /// Returns the concurrency window computed at construction.
    pub fn window(&self) -> usize {
        self.window
    }

    /// Executes the operations stream with bounded concurrency.
    ///
    /// Each item is a `(index, Result<Operation, E>)` tuple where `index` is the
    /// 0-based position of the operation in the original request. Error items pass
    /// through immediately; successful items are executed concurrently up to `window`
    /// at a time, each in an isolated [`tokio::spawn`].
    ///
    /// Results are yielded in completion order (not submission order). The permit
    /// reservation is held until the returned stream is fully consumed or dropped.
    pub fn execute<E>(
        self,
        context: ObjectContext,
        operations: impl Stream<Item = (usize, Result<Operation, E>)> + Send + 'static,
    ) -> impl Stream<Item = (usize, Result<OpResponse, E>)> + Send + 'static
    where
        E: From<Error> + Send + 'static,
    {
        let BatchExecutor {
            tiered,
            window,
            reservation,
        } = self;

        async_stream::stream! {
            // Hold the reservation for the stream's lifetime.
            let _reservation = reservation;

            let mut buffered = std::pin::pin!(operations
                .map(move |(idx, item)| {
                    let tiered = Arc::clone(&tiered);
                    let context = context.clone();
                    async move {
                        match item {
                            Err(e) => (idx, Err(e)),
                            Ok(op) => {
                                let handle = tokio::spawn(execute_single(tiered, context, op));
                                match handle.await {
                                    Ok(Ok(response)) => (idx, Ok(response)),
                                    Ok(Err(e)) => (idx, Err(E::from(e))),
                                    Err(join_err) => {
                                        let msg = if join_err.is_panic() {
                                            crate::service::extract_panic_message(
                                                join_err.into_panic(),
                                            )
                                        } else {
                                            "task cancelled".to_owned()
                                        };
                                        (idx, Err(E::from(Error::Panic(msg))))
                                    }
                                }
                            }
                        }
                    }
                })
                .buffer_unordered(window));

            while let Some(item) = buffered.next().await {
                yield item;
            }
        }
    }
}

async fn execute_single(
    tiered: Arc<TieredStorage>,
    context: ObjectContext,
    op: Operation,
) -> Result<OpResponse> {
    match op {
        Operation::Get(get) => {
            let id = ObjectId::new(context, get.key);
            let response = tiered.get_object(&id).await?;
            Ok(OpResponse::Got {
                key: id.key,
                response,
            })
        }
        Operation::Insert(insert) => {
            let stream: PayloadStream =
                futures_util::stream::once(futures_util::future::ready(Ok(insert.payload))).boxed();
            let id = tiered
                .insert_object(context, Some(insert.key), &insert.metadata, stream)
                .await?;
            Ok(OpResponse::Inserted { id })
        }
        Operation::Delete(delete) => {
            let id = ObjectId::new(context, delete.key);
            tiered.delete_object(&id).await?;
            Ok(OpResponse::Deleted { key: id.key })
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

    fn make_service_with_limit(limit: usize) -> crate::service::StorageService {
        let hv = InMemoryBackend::new("in-memory-hv");
        let lt = InMemoryBackend::new("in-memory-lt");
        crate::service::StorageService::from_backends(Box::new(hv), Box::new(lt))
            .with_concurrency_limit(limit)
    }

    fn make_service() -> crate::service::StorageService {
        make_service_with_limit(500)
    }

    // Wraps a plain `Vec<Operation>` as an indexed `Ok`-stream for `execute`.
    fn indexed_ok(
        ops: Vec<Operation>,
    ) -> impl futures_util::Stream<Item = (usize, Result<Operation, Error>)> {
        futures_util::stream::iter(ops.into_iter().enumerate().map(|(i, op)| (i, Ok(op))))
    }

    // --- BatchExecutor window and capacity tests ---

    #[test]
    fn at_capacity_when_no_permits() {
        let service = make_service_with_limit(0);
        assert!(matches!(service.batch(), Err(Error::AtCapacity)));
    }

    #[test]
    fn window_computation() {
        // 10 available → ceil(10 × 0.10) = 1
        let s = make_service_with_limit(10);
        assert_eq!(s.batch().unwrap().window(), 1);

        // 100 available → ceil(100 × 0.10) = 10
        let s = make_service_with_limit(100);
        assert_eq!(s.batch().unwrap().window(), 10);

        // 500 available → ceil(500 × 0.10) = 50
        let s = make_service_with_limit(500);
        assert_eq!(s.batch().unwrap().window(), 50);
    }

    #[test]
    fn window_clamped_to_min() {
        // 1–9 available → ceil(N × 0.10) < 1 → clamped to 1
        for n in 1..=9 {
            let s = make_service_with_limit(n);
            let w = s.batch().unwrap().window();
            assert_eq!(w, 1, "expected window 1 for {n} permits, got {w}");
        }
    }

    #[test]
    fn window_clamped_to_max() {
        // 501+ available → ceil(N × 0.10) > 50 → clamped to 50
        for n in [501, 1000, 10000] {
            let s = make_service_with_limit(n);
            let w = s.batch().unwrap().window();
            assert_eq!(w, 50, "expected window 50 for {n} permits, got {w}");
        }
    }

    // --- BatchExecutor::execute() correctness tests ---

    #[tokio::test]
    async fn execute_empty_stream() {
        let service = make_service();
        let executor = service.batch().unwrap();
        let outcomes: Vec<_> = executor
            .execute(
                make_context(),
                futures_util::stream::empty::<(usize, Result<Operation, Error>)>(),
            )
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

        let ops = vec![
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

        let executor = service.batch().unwrap();
        let outcomes: Vec<_> = executor.execute(context, indexed_ok(ops)).collect().await;

        assert_eq!(outcomes.len(), 4);

        for (_, result) in &outcomes {
            let response = result
                .as_ref()
                .unwrap_or_else(|e| panic!("unexpected error: {e:?}"));
            assert!(
                !response.key().is_empty(),
                "response must have a non-empty key"
            );
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
            stream: crate::PayloadStream,
        ) -> Result<()> {
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            let _ = self.paused_tx.send(()).await;
            self.resume.notified().await;
            let result = self.inner.put_object(id, metadata, stream).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn get_object(
            &self,
            id: &ObjectId,
        ) -> Result<Option<(Metadata, crate::PayloadStream)>> {
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
        let service = crate::service::StorageService::from_backends(Box::new(gated), Box::new(lt))
            .with_concurrency_limit(100);

        let executor = service.batch().unwrap();
        assert_eq!(executor.window(), 10);

        // Submit 10 inserts. With window=10, all should be in-flight simultaneously.
        let ops: Vec<Operation> = (0..10)
            .map(|i| {
                Operation::Insert(Insert {
                    key: format!("key{i}"),
                    metadata: Metadata::default(),
                    payload: Bytes::from(format!("data{i}")),
                })
            })
            .collect();

        let exec_handle = tokio::spawn(async move {
            executor
                .execute(make_context(), indexed_ok(ops))
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
        for (_, result) in &outcomes {
            assert!(
                matches!(result, Ok(OpResponse::Inserted { .. })),
                "unexpected result: {:?}",
                result
            );
        }
    }

    #[tokio::test]
    async fn batch_rejected_when_permits_exhausted() {
        // Service with limit=1. One background insert holds the only permit.
        // service.batch() must fail with AtCapacity.
        let (paused_tx, mut paused_rx) = tokio::sync::mpsc::channel::<()>(2);
        let resume = Arc::new(tokio::sync::Notify::new());

        let gated = GatedBackend {
            inner: InMemoryBackend::new("gated-cap"),
            paused_tx,
            resume: Arc::clone(&resume),
            in_flight: Arc::new(AtomicUsize::new(0)),
        };
        let lt = InMemoryBackend::new("in-memory-lt");
        let service = crate::service::StorageService::from_backends(Box::new(gated), Box::new(lt))
            .with_concurrency_limit(1);

        // Hold the only permit via a blocking insert.
        let svc = service.clone();
        tokio::spawn(async move {
            let _ = svc
                .insert_object(
                    make_context(),
                    Some("blocker".into()),
                    Metadata::default(),
                    futures_util::stream::once(async { Ok(Bytes::from("x")) }).boxed(),
                )
                .await;
        });
        paused_rx.recv().await.unwrap();

        // Permit is held — batch() must fail immediately with AtCapacity.
        assert!(
            matches!(service.batch(), Err(Error::AtCapacity)),
            "expected AtCapacity when all permits are held"
        );

        resume.notify_waiters();
    }
}
