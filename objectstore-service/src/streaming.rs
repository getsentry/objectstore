//! Streaming operation types and concurrent executor.
//!
//! [`StreamExecutor`] processes a stream of `(idx, Result<`[`Operation`]`, E>)` tuples
//! concurrently within a bounded window. Errors in the input stream pass through
//! unchanged; successful operations are executed against the backend directly,
//! with [`tokio::spawn`] for panic isolation and run-to-completion guarantees.
//!
//! ## Window and Permit Reservation
//!
//! The concurrency window is derived from the service's available permits at the time
//! [`StorageService::stream`](crate::service::StorageService::stream) is called: `ceil(tasks_available × 0.10)`.
//! The executor pre-acquires exactly `window` permits from the service's
//! `ConcurrencyLimiter` as a single bulk reservation. The reservation is shared
//! (via `Arc`) with every spawned task, so permits are released only after every
//! in-flight task completes — even if the output stream is dropped early.
//!
//! This means:
//! - If the service is at capacity, [`StorageService::stream`](crate::service::StorageService::stream) fails immediately with
//!   [`Error::AtCapacity`] before any operations are read.
//! - During execution, operations call the storage backend directly without acquiring
//!   additional per-operation permits.
//!
//! ## Concurrency Model
//!
//! [`StreamExecutor::execute`] uses `buffer_unordered` to drive up to `window`
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
//! sizing, and backend-level optimizations (e.g. BigTable multi-read, GCS batch API)
//! are out of scope for the current implementation.

use std::sync::Arc;

use futures_util::{Stream, StreamExt};
use objectstore_types::metadata::Metadata;

use crate::backend::common::Backend;
use crate::concurrency::ConcurrencyPermit;
use crate::error::{Error, Result};
use crate::id::{ObjectContext, ObjectId, ObjectKey};
use crate::service::GetResponse;

/// An insert operation: stores an object at the given key.
#[derive(Debug)]
pub struct Insert {
    /// The key to store the object under. When `None`, the service generates a key.
    pub key: Option<ObjectKey>,
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

/// An exists operation: checks whether an object exists by key.
#[derive(Debug)]
pub struct Exists {
    /// The key of the object to check.
    pub key: ObjectKey,
}

/// A single streaming operation.
#[derive(Debug)]
pub enum Operation {
    /// Insert a new object.
    Insert(Insert),
    /// Get an existing object.
    Get(Get),
    /// Delete an object.
    Delete(Delete),
    /// Check if an object exists.
    Exists(Exists),
}

impl Operation {
    /// Returns the key for this operation, if one was provided.
    pub fn key(&self) -> Option<&ObjectKey> {
        match self {
            Operation::Insert(op) => op.key.as_ref(),
            Operation::Get(op) => Some(&op.key),
            Operation::Delete(op) => Some(&op.key),
            Operation::Exists(op) => Some(&op.key),
        }
    }

    /// Returns the permission required to perform this operation.
    pub fn permission(&self) -> objectstore_types::auth::Permission {
        match self {
            Operation::Get(_) | Operation::Exists(_) => {
                objectstore_types::auth::Permission::ObjectRead
            }
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
            Operation::Exists(_) => "exists",
        }
    }
}

/// The response of a single executed streaming operation.
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
    /// An exists check completed.
    Exists {
        /// The key that was checked.
        key: ObjectKey,
        /// Whether the object exists.
        exists: bool,
    },
}

impl OpResponse {
    /// Returns the operation kind name.
    pub fn kind(&self) -> &'static str {
        match self {
            OpResponse::Inserted { .. } => "insert",
            OpResponse::Got { .. } => "get",
            OpResponse::Deleted { .. } => "delete",
            OpResponse::Exists { .. } => "exists",
        }
    }

    /// Returns the object key for this response.
    pub fn key(&self) -> &ObjectKey {
        match self {
            OpResponse::Inserted { id } => &id.key,
            OpResponse::Got { key, .. } => key,
            OpResponse::Deleted { key } => key,
            OpResponse::Exists { key, .. } => key,
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
                .field("response", &format_args!("Some(<stream>)"))
                .finish(),
            OpResponse::Got {
                key,
                response: None,
            } => f
                .debug_struct("Got")
                .field("key", key)
                .field("response", &format_args!("None"))
                .finish(),
            OpResponse::Deleted { key } => f.debug_struct("Deleted").field("key", key).finish(),
            OpResponse::Exists { key, exists } => f
                .debug_struct("Exists")
                .field("key", key)
                .field("exists", exists)
                .finish(),
        }
    }
}

/// Executes streaming operations with bounded concurrency.
///
/// Construct via [`StorageService::stream`](crate::service::StorageService::stream),
/// which pre-acquires the concurrency window from the service's available permits.
///
/// See the [module documentation](self) for a full description of the window
/// calculation, permit reservation, and concurrency model.
#[derive(Debug)]
pub struct StreamExecutor {
    pub(crate) backend: Arc<dyn Backend>,
    pub(crate) window: usize,
    pub(crate) reservation: ConcurrencyPermit,
}

impl StreamExecutor {
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
    /// reservation is held until every spawned task has completed — if the stream
    /// is dropped early, in-flight tasks run to completion before the permits are
    /// released.
    pub fn execute<E>(
        self,
        context: ObjectContext,
        operations: impl Stream<Item = (usize, Result<Operation, E>)> + Send + 'static,
    ) -> impl Stream<Item = (usize, Result<OpResponse, E>)> + Send + 'static
    where
        E: From<Error> + Send + 'static,
    {
        let StreamExecutor {
            backend,
            window,
            reservation,
        } = self;

        // Arc-wrap so each spawned task can hold a clone. Permits are released
        // only when the last clone is dropped — i.e. after every spawned task
        // completes, even if the output stream is dropped early.
        let reservation = Arc::new(reservation);

        operations
            .map(move |(idx, item)| {
                let permit = Arc::clone(&reservation);
                let backend = Arc::clone(&backend);
                let context = context.clone();
                async move {
                    let op = match item {
                        Ok(op) => op,
                        Err(e) => return (idx, Err(e)),
                    };

                    let spawn = crate::concurrency::spawn_metered(op.kind(), permit, async move {
                        execute_operation(backend, context, op).await
                    });

                    (idx, spawn.await.map_err(E::from))
                }
            })
            .buffer_unordered(window)
    }
}

async fn execute_operation(
    backend: Arc<dyn Backend>,
    context: ObjectContext,
    op: Operation,
) -> Result<OpResponse> {
    match op {
        Operation::Get(get) => {
            let id = ObjectId::new(context, get.key);
            let response = backend.get_object(&id).await?;
            Ok(OpResponse::Got {
                key: id.key,
                response,
            })
        }
        Operation::Insert(insert) => {
            let id = ObjectId::optional(context, insert.key);
            let stream = crate::stream::single(insert.payload);
            backend.put_object(&id, &insert.metadata, stream).await?;
            Ok(OpResponse::Inserted { id })
        }
        Operation::Delete(delete) => {
            let id = ObjectId::new(context, delete.key);
            backend.delete_object(&id).await?;
            Ok(OpResponse::Deleted { key: id.key })
        }
        Operation::Exists(exists) => {
            let id = ObjectId::new(context, exists.key);
            let found = backend.get_metadata(&id).await?.is_some();
            Ok(OpResponse::Exists {
                key: id.key,
                exists: found,
            })
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
    use crate::backend::common::PutResponse;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::backend::testing::{Hooks, TestBackend};
    use crate::error::Error;
    use crate::service::StorageService;
    use crate::stream::{self, ClientStream};

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_service_with_limit(limit: usize) -> StorageService {
        StorageService::new(Box::new(InMemoryBackend::new("in-memory")))
            .with_concurrency_limit(limit)
    }

    fn make_service() -> StorageService {
        make_service_with_limit(500)
    }

    // Wraps a plain `Vec<Operation>` as an indexed `Ok`-stream for `execute`.
    fn indexed_ok(
        ops: Vec<Operation>,
    ) -> impl futures_util::Stream<Item = (usize, Result<Operation, Error>)> {
        futures_util::stream::iter(ops.into_iter().enumerate().map(|(i, op)| (i, Ok(op))))
    }

    // --- StreamExecutor window and capacity tests ---

    #[test]
    fn at_capacity_when_no_permits() {
        let service = make_service_with_limit(0);
        assert!(matches!(service.stream(), Err(Error::AtCapacity)));
    }

    #[test]
    fn window_computation() {
        // ceil(1 × 0.10) = 1
        let s = make_service_with_limit(1);
        assert_eq!(s.stream().unwrap().window(), 1);

        // ceil(10 × 0.10) = 1
        let s = make_service_with_limit(10);
        assert_eq!(s.stream().unwrap().window(), 1);

        // ceil(100 × 0.10) = 10
        let s = make_service_with_limit(100);
        assert_eq!(s.stream().unwrap().window(), 10);

        // ceil(500 × 0.10) = 50
        let s = make_service_with_limit(500);
        assert_eq!(s.stream().unwrap().window(), 50);

        // ceil(1000 × 0.10) = 100
        let s = make_service_with_limit(1000);
        assert_eq!(s.stream().unwrap().window(), 100);
    }

    // --- StreamExecutor::execute() correctness tests ---

    #[tokio::test]
    async fn execute_empty_stream() {
        let service = make_service();
        let executor = service.stream().unwrap();
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
                stream::single("hello"),
            )
            .await
            .unwrap();

        let ops = vec![
            Operation::Get(Get { key: "key1".into() }),
            Operation::Get(Get {
                key: "nonexistent".into(),
            }),
            Operation::Insert(Insert {
                key: Some("key2".into()),
                metadata: Metadata::default(),
                payload: Bytes::from("world"),
            }),
            Operation::Delete(Delete { key: "key1".into() }),
        ];

        let executor = service.stream().unwrap();
        let outcomes: Vec<_> = executor.execute(context, indexed_ok(ops)).collect().await;

        assert_eq!(outcomes.len(), 4);

        for (_, result) in &outcomes {
            let response = result
                .as_ref()
                .unwrap_or_else(|e| panic!("unexpected error: {e:?}"));
            assert!(
                !response.key().as_str().is_empty(),
                "response must have a non-empty key"
            );
        }
    }

    #[tokio::test]
    async fn execute_exists_operation() {
        let service = make_service();
        let context = make_context();

        service
            .insert_object(
                context.clone(),
                Some("present".into()),
                Metadata::default(),
                stream::single("data"),
            )
            .await
            .unwrap();

        let ops = vec![
            Operation::Exists(Exists {
                key: "present".into(),
            }),
            Operation::Exists(Exists {
                key: "missing".into(),
            }),
        ];

        let executor = service.stream().unwrap();
        let mut outcomes: Vec<_> = executor.execute(context, indexed_ok(ops)).collect().await;
        outcomes.sort_by_key(|(idx, _)| *idx);

        assert_eq!(outcomes.len(), 2);

        match &outcomes[0].1 {
            Ok(OpResponse::Exists { key, exists }) => {
                assert_eq!(key.as_str(), "present");
                assert!(exists);
            }
            other => panic!("unexpected result: {other:?}"),
        }

        match &outcomes[1].1 {
            Ok(OpResponse::Exists { key, exists }) => {
                assert_eq!(key.as_str(), "missing");
                assert!(!exists);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // --- Service-level concurrent execution and capacity tests ---

    struct GateOnPut {
        paused_tx: tokio::sync::mpsc::Sender<()>,
        resume: Arc<tokio::sync::Notify>,
        in_flight: Arc<AtomicUsize>,
    }

    impl std::fmt::Debug for GateOnPut {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("GateOnPut").finish()
        }
    }

    #[async_trait::async_trait]
    impl Hooks for GateOnPut {
        async fn put_object(
            &self,
            inner: &InMemoryBackend,
            id: &ObjectId,
            metadata: &Metadata,
            stream: ClientStream,
        ) -> Result<PutResponse> {
            self.in_flight.fetch_add(1, Ordering::SeqCst);
            let _ = self.paused_tx.send(()).await;
            self.resume.notified().await;
            let result = inner.put_object(id, metadata, stream).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }
    }

    #[tokio::test]
    async fn concurrent_execution() {
        // Window = ceil(100 × 0.10) = 10.
        let (paused_tx, mut paused_rx) = tokio::sync::mpsc::channel::<()>(20);
        let resume = Arc::new(tokio::sync::Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));

        let gated = TestBackend::new(GateOnPut {
            paused_tx,
            resume: Arc::clone(&resume),
            in_flight: Arc::clone(&in_flight),
        });
        let service = StorageService::new(Box::new(gated)).with_concurrency_limit(100);

        let executor = service.stream().unwrap();
        assert_eq!(executor.window(), 10);

        // Submit 10 inserts. With window=10, all should be in-flight simultaneously.
        let ops: Vec<Operation> = (0..10)
            .map(|i| {
                Operation::Insert(Insert {
                    key: Some(format!("key{i}")),
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
        // service.stream() must fail with AtCapacity.
        let (paused_tx, mut paused_rx) = tokio::sync::mpsc::channel::<()>(2);
        let resume = Arc::new(tokio::sync::Notify::new());

        let gated = TestBackend::new(GateOnPut {
            paused_tx,
            resume: Arc::clone(&resume),
            in_flight: Arc::new(AtomicUsize::new(0)),
        });
        let service = StorageService::new(Box::new(gated)).with_concurrency_limit(1);

        // Hold the only permit via a blocking insert.
        let svc = service.clone();
        tokio::spawn(async move {
            let _ = svc
                .insert_object(
                    make_context(),
                    Some("blocker".into()),
                    Metadata::default(),
                    stream::single("x"),
                )
                .await;
        });
        paused_rx.recv().await.unwrap();

        // Permit is held — stream() must fail immediately with AtCapacity.
        assert!(
            matches!(service.stream(), Err(Error::AtCapacity)),
            "expected AtCapacity when all permits are held"
        );

        resume.notify_waiters();
    }
}
