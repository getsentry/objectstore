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

# Two-Tier Backend System

Every [`StorageService`] is initialized with two backends: a **high-volume**
backend and a **long-term** backend. This split exists because no single storage
system optimally handles both small, frequently-accessed objects and large,
infrequently-accessed ones:

- **High-volume backend** (typically
  [BigTable](StorageConfig::BigTable)): optimized for low-latency reads and
  writes of small objects. Most objects in practice are small (metadata blobs,
  debug symbols, etc.), so this path handles the majority of traffic.
- **Long-term backend** (typically [GCS](StorageConfig::Gcs)): optimized for
  large objects where per-byte storage cost matters more than access latency.

The threshold is **1 MiB**. Objects at or below this size go to the high-volume
backend; objects exceeding it go to the long-term backend.

See [`StorageConfig`] for available backend implementations.

# Metadata and Payload

Every object consists of structured **metadata** and a binary **payload**.
Metadata contains a set of built-in keys with special semantics (such as
expiration policies and redirect tombstone markers) as well as arbitrary
user-defined key-value pairs. Metadata is always stored alongside the payload in
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

Data flows as a [`PayloadStream`] throughout the API to keep memory consumption
low.

On **writes**, the service buffers the incoming stream only up to the 1 MiB
threshold to determine which backend to use. Once exceeded, the buffered bytes
are prepended to the remaining stream and everything flows through to the
long-term backend without further accumulation.

On **reads**, the service streams the response back wherever the backend
supports it. Not all backends stream small payloads (e.g. BigTable returns them
in a single response), but for large objects in the long-term backend, data is
streamed end-to-end.

## Expiration

Expiration policies are part of the built-in object metadata and can carry
special semantics. The service delegates expiry **entirely** to the backend
implementation, allowing each backend to leverage its underlying system's native
capabilities. For example, BigTable has built-in TTL via garbage collection
policies, and GCS supports object lifecycle management. The service does not
perform active garbage collection.

# Backpressure

The service applies backpressure to protect backends from overload. Rather than
queueing work when capacity is exhausted, the service rejects operations
immediately so the caller can shed load or retry.

## Concurrency Limit

A semaphore caps the total number of in-flight backend operations across all
callers. A permit is acquired before each operation is spawned and held until
the task completes — including on panic — so the limit counts *running*
operations, not queued ones. When no permits are available, the operation fails
with [`Error::AtCapacity`](error::Error::AtCapacity).

The default limit is [`DEFAULT_CONCURRENCY_LIMIT`](service::DEFAULT_CONCURRENCY_LIMIT). Callers can override it via
[`StorageService::with_concurrency_limit`].

## Further Plans

More backpressure mechanisms (e.g. per-backend limits, adaptive throttling) may
be added here in the future.

# Tombstone Consistency

The tombstone system maintains consistency through **operation ordering** rather
than distributed locks. The invariant is: a redirect tombstone is always the
**last thing written** and the **last thing removed**.

- On **write**: the real object is persisted in the long-term backend first,
  then the tombstone is written to the high-volume backend. If the tombstone
  write fails, the real object is rolled back, leaving nothing in either
  backend.
- On **delete**: the real object is removed from the long-term backend first,
  then the tombstone is removed from the high-volume backend. If the long-term
  delete fails, the tombstone remains and the data stays reachable — no orphan.

This ensures that at every intermediate step, either the data is fully
reachable (tombstone points to a live object) or fully absent — never an orphan
in either backend.

# Task Isolation and Cancellation

Each operation is spawned in a separate task for panic isolation. A failure in
one operation does not affect other in-flight work. See
[`Error::Panic`](error::Error).

Operations support different cancellation strategies based on their consistency
requirements:

## Insert — Stream-Level Cancellation

Insert operations support **stream-level cancellation**. When the caller's
future is dropped (client disconnect), a cancellation token fires, causing the
payload stream to return an error on its next poll. This aborts the in-flight
backend upload promptly, releasing the concurrency permit and avoiding wasted
bandwidth.

The cancellation boundary is **stream exhaustion**:

- If the token fires while the stream is still yielding chunks, the long-term
  backend upload is aborted before any data is committed. No tombstone is
  needed or written — the system is clean.
- If the token fires after the stream is fully consumed, the long-term upload
  has already committed successfully. The token has no effect and the operation
  continues normally, writing the tombstone and satisfying the consistency
  invariant above.

This means cancellation is always safe with respect to tombstone consistency:
either nothing is written, or both the object and its tombstone are written.

## Read and Metadata — Task-Level Cancellation

Read (`get_object`) and metadata (`get_metadata`) operations support
**task-level cancellation**. When the caller's future is dropped, a
`CancellationToken` fires inside the spawned task, racing against the backend
call via `select!`. The backend future is dropped at its current `.await` point,
releasing the concurrency permit and closing any in-flight HTTP/RPC connections.

Since reads are side-effect-free, this is always safe — there is no consistency
invariant to protect. This prevents disconnected clients from holding permits
and starving valid requests under load.

## Delete — Run-to-Completion

Delete operations **run to completion** even if the caller is cancelled. This
protects the tombstone ordering invariant described above: a delete that is
mid-flight must finish removing the long-term object before it removes the
tombstone.
