//! Streaming operation types and concurrent executor.
//!
//! [`StreamExecutor`] processes a stream of `(idx, Result<`[`Operation`]`, E>)` tuples concurrently
//! within a bounded window. Errors in the input stream pass through unchanged; successful
//! operations are executed against the backend directly, with [`tokio::spawn`] for panic isolation
//! and run-to-completion guarantees.
//!
//! ## Permit Acquisition
//!
//! Streaming / batch operations are subject to the service's concurrency limiter. They count as
//! "bulk" operations, which are capped at a lower limit than regular operations. This ensures a
//! large bulk request doesn't automatically brings the service to its limits and leaves room for
//! regular requests.
//!
//! The regular acquire timeout applies: Operations that cannot acquire a permit within the
//! configured queue timeout fail with [`Error::AtCapacity`].
//!
//! ## Concurrency Model
//!
//! [`StreamExecutor::execute`] uses `buffer_unordered` with the bulk budget as the concurrency
//! bound. The input stream is pulled lazily and results are yielded in completion order. Each
//! operation is wrapped in a [`tokio::spawn`] for panic isolation: a panic in one operation
//! surfaces as [`Error::Panic`] for that item and does not affect the others.

use std::sync::Arc;

use futures_util::{Stream, StreamExt};
use objectstore_types::metadata::Metadata;

use crate::backend::common::Backend;
use crate::concurrency::ConcurrencyLimiter;
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

/// A head (metadata-only) operation: checks existence and retrieves metadata by key.
#[derive(Debug)]
pub struct Head {
    /// The key of the object to check.
    pub key: ObjectKey,
}

/// A single streaming operation.
#[derive(Debug)]
pub enum Operation {
    /// Insert a new object.
    Insert(Box<Insert>),
    /// Get an existing object.
    Get(Get),
    /// Delete an object.
    Delete(Delete),
    /// Head (metadata-only) check for an object.
    Head(Head),
}

impl Operation {
    /// Returns the key for this operation, if one was provided.
    pub fn key(&self) -> Option<&ObjectKey> {
        match self {
            Operation::Insert(op) => op.key.as_ref(),
            Operation::Get(op) => Some(&op.key),
            Operation::Delete(op) => Some(&op.key),
            Operation::Head(op) => Some(&op.key),
        }
    }

    /// Returns the permission required to perform this operation.
    pub fn permission(&self) -> objectstore_types::auth::Permission {
        match self {
            Operation::Get(_) | Operation::Head(_) => {
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
            Operation::Head(_) => "head",
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
    /// A head (metadata-only) check completed.
    Head {
        /// The key that was checked.
        key: ObjectKey,
        /// The metadata, or `None` if the object was not found.
        metadata: Option<Metadata>,
    },
}

impl OpResponse {
    /// Returns the operation kind name.
    pub fn kind(&self) -> &'static str {
        match self {
            OpResponse::Inserted { .. } => "insert",
            OpResponse::Got { .. } => "get",
            OpResponse::Deleted { .. } => "delete",
            OpResponse::Head { .. } => "head",
        }
    }

    /// Returns the object key for this response.
    pub fn key(&self) -> &ObjectKey {
        match self {
            OpResponse::Inserted { id } => &id.key,
            OpResponse::Got { key, .. } => key,
            OpResponse::Deleted { key } => key,
            OpResponse::Head { key, .. } => key,
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
            OpResponse::Head { key, metadata } => f
                .debug_struct("Head")
                .field("key", key)
                .field("metadata", &metadata.is_some())
                .finish(),
        }
    }
}

/// Executes streaming operations with bounded concurrency.
///
/// Construct via [`StorageService::stream`](crate::service::StorageService::stream).
/// Each operation acquires a bulk permit individually; aggregate bulk
/// concurrency is bounded by the bulk semaphore on the limiter.
///
/// See the [module documentation](self) for the concurrency model.
#[derive(Debug)]
pub struct StreamExecutor {
    pub(crate) backend: Arc<dyn Backend>,
    pub(crate) concurrency: ConcurrencyLimiter,
}

impl StreamExecutor {
    /// Executes the operations stream with bounded concurrency.
    ///
    /// Each item is a `(index, Result<Operation, E>)` tuple where `index` is the
    /// 0-based position of the operation in the original request. Error items pass
    /// through immediately; successful items acquire a bulk permit and execute
    /// in an isolated [`tokio::spawn`].
    ///
    /// Permit acquisition is sequential — only one acquire is in flight at a
    /// time, ensuring fairness with other streams and preventing a large
    /// batch from racing for all permits at once. Execution of operations
    /// that already hold a permit proceeds concurrently.
    ///
    /// Operations that cannot acquire a permit within the configured queue
    /// timeout fail with [`Error::AtCapacity`]. Results are yielded in
    /// completion order (not submission order).
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
            concurrency,
        } = self;

        let buffer = concurrency.total_bulk().max(1) as usize;

        operations
            // `then` awaits each closure before pulling the next item, making
            // permit acquisition sequential. this ensures fairness with other
            // streams and prevents a large batch from racing for all permits
            // at once.
            .then(move |(idx, item)| {
                let concurrency = concurrency.clone();
                async move {
                    let op = match item {
                        Ok(op) => op,
                        Err(e) => return (idx, Err(e)),
                    };
                    match concurrency.acquire_bulk().await {
                        Ok(permit) => (idx, Ok((op, permit))),
                        Err(e) => (idx, Err(E::from(e))),
                    }
                }
            })
            .map(move |(idx, result)| {
                let backend = Arc::clone(&backend);
                let context = context.clone();
                async move {
                    let (op, permit) = match result {
                        Ok(pair) => pair,
                        Err(e) => return (idx, Err(e)),
                    };

                    let spawn = crate::concurrency::spawn_metered(op.kind(), permit, {
                        execute_operation(backend, context, op)
                    });
                    (idx, spawn.await.map_err(E::from))
                }
            })
            .buffer_unordered(buffer)
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
            let response = backend.get_object(&id, None).await?;
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
        Operation::Head(head) => {
            let id = ObjectId::new(context, head.key);
            let metadata = backend.get_metadata(&id).await?;
            Ok(OpResponse::Head {
                key: id.key,
                metadata,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use futures_util::StreamExt;
    use objectstore_types::metadata::Metadata;
    use objectstore_types::scope::{Scope, Scopes};

    use super::*;
    use crate::backend::common::PutResponse;
    use crate::backend::in_memory::InMemoryBackend;
    use crate::backend::testing::{Hooks, TestBackend};
    use crate::concurrency::ConcurrencyLimiter;
    use crate::error::Error;
    use crate::service::StorageService;
    use crate::stream::{self, ClientStream};

    fn make_context() -> ObjectContext {
        ObjectContext {
            usecase: "testing".into(),
            scopes: Scopes::from_iter([Scope::create("testing", "value").unwrap()]),
        }
    }

    fn make_service_with_limit(limit: u32) -> StorageService {
        StorageService::new(Box::new(InMemoryBackend::new("in-memory")))
            .with_concurrency(ConcurrencyLimiter::new(limit))
    }

    fn make_service() -> StorageService {
        make_service_with_limit(500)
    }

    // Wraps a plain `Vec<Operation>` as an indexed `Ok`-stream for `execute`.
    fn indexed_ok(ops: Vec<Operation>) -> impl Stream<Item = (usize, Result<Operation, Error>)> {
        futures_util::stream::iter(ops.into_iter().enumerate().map(|(i, op)| (i, Ok(op))))
    }

    // --- StreamExecutor correctness tests ---

    #[tokio::test]
    async fn execute_empty_stream() {
        let service = make_service();
        let executor = service.stream();
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
            Operation::Insert(Box::new(Insert {
                key: Some("key2".into()),
                metadata: Metadata::default(),
                payload: Bytes::from("world"),
            })),
            Operation::Delete(Delete { key: "key1".into() }),
        ];

        let executor = service.stream();
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
    async fn execute_head_operation() {
        let service = make_service();
        let context = make_context();

        service
            .insert_object(
                context.clone(),
                Some("exists".into()),
                Metadata::default(),
                stream::single("data"),
            )
            .await
            .unwrap();

        let ops = vec![
            Operation::Head(Head {
                key: "exists".into(),
            }),
            Operation::Head(Head {
                key: "missing".into(),
            }),
        ];

        let executor = service.stream();
        let mut outcomes: Vec<_> = executor.execute(context, indexed_ok(ops)).collect().await;
        outcomes.sort_by_key(|(idx, _)| *idx);

        assert_eq!(outcomes.len(), 2);

        match &outcomes[0].1 {
            Ok(OpResponse::Head {
                key,
                metadata: Some(_),
            }) => assert_eq!(key.as_str(), "exists"),
            other => panic!("expected Head with metadata, got: {other:?}"),
        }

        match &outcomes[1].1 {
            Ok(OpResponse::Head {
                key,
                metadata: None,
            }) => assert_eq!(key.as_str(), "missing"),
            other => panic!("expected Head with None, got: {other:?}"),
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
        let (paused_tx, mut paused_rx) = tokio::sync::mpsc::channel::<()>(20);
        let resume = Arc::new(tokio::sync::Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));

        let gated = TestBackend::new(GateOnPut {
            paused_tx,
            resume: Arc::clone(&resume),
            in_flight: Arc::clone(&in_flight),
        });
        let service =
            StorageService::new(Box::new(gated)).with_concurrency(ConcurrencyLimiter::new(100));

        let ops: Vec<Operation> = (0..10)
            .map(|i| {
                Operation::Insert(Box::new(Insert {
                    key: Some(format!("key{i}")),
                    metadata: Metadata::default(),
                    payload: Bytes::from(format!("data{i}")),
                }))
            })
            .collect();

        let executor = service.stream();
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
                "unexpected result: {result:?}",
            );
        }
    }

    #[tokio::test]
    async fn bulk_respects_budget() {
        // Bulk budget = 1 (100% of max=1). Hold the permit via a normal
        // acquire; the bulk op should wait and eventually time out.
        let service = StorageService::new(Box::new(InMemoryBackend::new("in-memory")))
            .with_concurrency(
                ConcurrencyLimiter::new(1)
                    .with_queue(0, Duration::from_millis(1))
                    .with_bulk(100),
            );

        let _held = service.concurrency.acquire().await.unwrap();

        let ops = vec![Operation::Insert(Box::new(Insert {
            key: Some("blocked".into()),
            metadata: Metadata::default(),
            payload: Bytes::from("data"),
        }))];

        let executor = service.stream();
        let outcomes: Vec<_> = executor
            .execute(make_context(), indexed_ok(ops))
            .collect()
            .await;

        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(&outcomes[0].1, Err(Error::AtCapacity)),
            "expected AtCapacity, got {:?}",
            outcomes[0].1,
        );
    }
}
