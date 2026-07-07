The service layer is the core storage abstraction for objectstore. It provides
durable access to blobs through a dual-backend architecture that balances cost,
latency, and reliability. The service is designed as a library crate consumed by
the `objectstore-server`.

# Object Identification

Every object is uniquely identified by an [`ObjectId`](id::ObjectId), a logical
address that is **backend-independent** — the same `ObjectId` refers to the
same object regardless of which physical backend currently stores its data. This
allows objects to be transparently moved between backends (e.g. during
migrations or rebalancing) without changing their identity.

Identifiers are also designed to be **self-contained**: given an `ObjectId`, you
can always determine which usecase and organizational scope the object belongs
to. This makes references into objectstore meaningful on their own, without
requiring a lookup.

An `ObjectId` consists of an [`ObjectContext`](id::ObjectContext) (the _where_)
and a key (the _what_). The context contains:

- A **usecase** — a top-level namespace (e.g. `"attachments"`,
  `"debug-files"`) that groups related objects. A usecase can have its own
  server-level configuration such as rate limits or killswitches.
- **Scopes** — ordered key-value pairs that form a hierarchy within a usecase,
  such as `organization=17, project=42`. They act as both an organizational
  structure and an authorization boundary.

See the [`id`] module for details on storage path formatting, scope ordering,
and key generation.

# Stateless Design

The service layer has no caches or local state beyond what is needed for a
single request. This is intentional:

- **Object sizes vary wildly** — caching large objects is impractical.
- **Access patterns are write-once, read-few** — the hit rate for a cache
  would be low.
- **The high-volume backend already provides low latency** for the common
  case of small objects.
- **Horizontal scaling** — without shared caches, any service instance can
  handle any request. There is no need to shard requests for read-after-write
  consistency or to replicate cache state.

The service orchestrates mature, battle-tested backends and keeps its own
footprint minimal.

Each storage operation runs to completion even if the caller is cancelled (e.g.,
due to a client disconnect). This ensures that multi-step operations such as
writing redirect tombstones are never left partially applied. Post-commit
cleanup of unreferenced long-term blobs runs in background tasks so it does not
block the caller. Operations are also panic-isolated — a failure in one request
does not bring down the service.

# Two-Tier Backend System

[`TieredStorage`](backend::tiered::TieredStorage) is the
[`Backend`](backend::common::Backend) implementation that provides the two-tier
system. It is the typical backend passed to [`StorageService::new`], though any
`Backend` implementation can be used. The two-tier split exists because no
single storage system optimally handles both small, frequently-accessed objects
and large, infrequently-accessed ones:

- **High-volume backend** (typically
  [BigTable](backend::StorageConfig::BigTable)): optimized for low-latency reads
  and writes of small objects. Objects in practice are small (metadata blobs,
  event attachments, etc.), so this path handles the majority of traffic by
  volume.
- **Long-term backend** (typically [GCS](backend::StorageConfig::Gcs)):
  optimized for large objects and long retention periods where per-byte storage
  cost matters more than access latency.

The threshold is **1 MiB**. `TieredStorage` routes objects at or below this
size to the high-volume backend; objects exceeding it go to the long-term
backend.

See [`backend::StorageConfig`] for available backend implementations.

## Redirect Tombstones

For large objects, `TieredStorage` stores a **redirect tombstone** in the
high-volume backend — a marker that carries the target `ObjectId` where the real
payload lives in the long-term backend. Reads check only the high-volume
backend: they either find the object directly (small) or follow the tombstone's
target to long-term storage (large), without probing both backends.

How tombstones are physically stored is determined by the
[`HighVolumeBackend`](crate::backend::common::HighVolumeBackend)
implementation. Refer to the backend's own documentation for storage format
details.

## Cross-Tier Consistency

Because a single logical object may span both backends (tombstone in HV, payload
in LT), mutations must keep them in sync without distributed locks. The
high-volume backend must implement
[`HighVolumeBackend`](backend::common::HighVolumeBackend), which provides
compare-and-swap operations that `TieredStorage` uses to atomically commit
cross-tier state changes — rolling back on conflict so that concurrent writers
never corrupt each other's data. After the commit point, cleanup of the
now-unreferenced LT blob is performed in the background so the caller is not
blocked by cross-backend I/O. [`Backend::join`](backend::common::Backend::join)
waits for outstanding cleanup during graceful shutdown.

See the [`backend::tiered`] module documentation for the per-operation
sequences.

# Cost of Goods Sold (COGS) Accounting

[`StorageService::new`] wraps the configured backend in a
[`CountingBackend`](backend::counting::CountingBackend), a
[`Backend`](backend::common::Backend) decorator that increments the
`objectstore.cogs.usage` counter (tagged with an `app_feature` derived from the
usecase) once per operation. Multipart operations are also counted.

For COGS purposes we use operation count as a proxy for compute cost under the
assumption that each operation we serve has a basically flat CPU cost. Large
payloads take longer, but they can be streamed in the background while other
operations are served so they don't really cost more.

Wrapping the outermost backend owned by `StorageService` covers every operation
called by `StorageService` itself as well as batched operations that are run
through [`StreamExecutor`](crate::streaming::StreamExecutor).

Notably, operations that fail before reaching `StorageService` (e.g. auth or
rate-limiting failures at a higher layer) are not counted.

# Metadata and Payload

Every object consists of structured **metadata** and a binary **payload**.
Metadata contains a set of built-in keys with special semantics (such as
expiration policies) as well as arbitrary user-defined key-value pairs.
Metadata is always stored alongside the payload in
the same backend — never in a separate data store. This ensures that inspecting
a backend directly is sufficient to resolve an object together with its
metadata, without joining across stores.

Metadata is small and always fully loaded into memory, while the payload
streams. Backends can serve metadata independently of the payload (e.g. BigTable
uses separate column families; GCS stores metadata as object headers), which
enables efficient metadata-only reads.

Individual metadata keys are **mutable** — they can be updated without
rewriting the payload. Payloads, however, can only be replaced in their
entirety.

## Streaming and Buffering

Data flows in streams throughout the API to keep memory consumption low. See the
[`stream`] module for the stream types and related utilities.

On **writes**, the incoming request body arrives as a [`ClientStream`]. The
service buffers it only up to the 1 MiB threshold to determine which backend to
use. Once exceeded, the buffered bytes are prepended to the remaining stream and
everything flows through to the long-term backend without further accumulation.

On **reads**, the backend returns a [`PayloadStream`] that the service forwards
to the caller. Not all backends stream small payloads (e.g. BigTable returns
them in a single response), but for large objects in the long-term backend, data
is streamed end-to-end.

## Expiration

Expiration policies are part of the built-in object metadata and can carry
special semantics. The service delegates expiry **entirely** to the backend
implementation, allowing each backend to leverage its underlying system's native
capabilities. For example, BigTable has built-in TTL via garbage collection
policies, and GCS supports object lifecycle management. The service does not
perform active garbage collection.

Apart from the expiration policy, metadata during object creation must carry a
`time_expires` field with the correct expiration timestamp. This is ensured
during metadata creation by the server.

# Backpressure

The service applies backpressure to protect backends from overload. Rather than
queueing work when capacity is exhausted, the service rejects operations
immediately so the caller can shed load or retry.

# Error Classification

The service error type exposes a fine-grained [`ErrorKind`](error::ErrorKind)
that describes the *cause* of a failure independent of HTTP semantics, so API
layers can map failures without matching every specific variant and remain free
to decide how each kind maps onto a status code.

## Concurrency Limit

A semaphore caps the total number of in-flight backend operations across all
callers. A permit is acquired before each operation is spawned and held until
the task completes — including on panic — so the limit counts *running*
operations, not queued ones. When no permits are available, the operation fails
with [`Error::AtCapacity`](error::Error::AtCapacity).

The default limit is [`DEFAULT_CONCURRENCY_LIMIT`](service::DEFAULT_CONCURRENCY_LIMIT). Callers can override it via
[`StorageService::with_concurrency_limit`].

## Multipart Uploads

When the configured backend supports it, [`StorageService`] exposes multipart
upload operations (initiate, upload part, list parts, complete, abort). These
delegate to the [`MultipartUploadBackend`](backend::common::MultipartUploadBackend)
trait, accessed via [`Backend::as_multipart_upload_backend`](backend::common::Backend::as_multipart_upload_backend).
Multipart operations share the same concurrency limiter as regular operations.

## Streaming Concurrency

The [`streaming`](streaming) module provides [`StreamExecutor`](streaming::StreamExecutor)
for running a stream of operations concurrently within a bounded window. It is
intended for efficient handling of batch requests, where multiple operations
arrive together and should be dispatched in parallel rather than sequentially.
See the [module documentation](streaming) for the window formula, permit
reservation, lazy pulling, memory bounds, and concurrency model.

## Further Plans

More backpressure mechanisms (e.g. per-backend limits, adaptive throttling) may
be added here in the future.
