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
writing redirect tombstones are never left partially applied. Operations are also
panic-isolated — a failure in one request does not bring down the service.

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
