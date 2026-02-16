# Service Layer Architecture

The service layer is the core storage abstraction for objectstore. It provides
durable access to blobs through a dual-backend architecture that balances cost,
latency, and reliability. The service is designed as a library crate consumed by
the `objectstore-server`.

## Two-Tier Backend System

Every [`StorageService`] is initialized with two backends: a **high-volume**
backend and a **long-term** backend. This split exists because no single storage
system optimally handles both small, frequently-accessed objects and large,
infrequently-accessed ones:

- **High-volume backend** (typically BigTable): optimized for low-latency reads
  and writes of small objects. Most objects in practice are small (metadata
  blobs, debug symbols, etc.), so this path handles the majority of traffic.
- **Long-term backend** (typically GCS): optimized for large objects where
  per-byte storage cost matters more than access latency.

The threshold is **1 MiB**. Objects at or below
this size go to the high-volume backend; objects exceeding it go to the
long-term backend.

### Backend Routing at Insert Time

When [`StorageService::insert_object`] is called, the service does not know the
total object size upfront because data arrives as a stream. Instead, it buffers
the incoming stream up to the 1 MiB threshold:

1. Read chunks from the [`PayloadStream`] into a buffer.
2. If the buffer stays within 1 MiB and the stream ends, store entirely in the
   high-volume backend.
3. If the buffer exceeds 1 MiB, route to the long-term backend. The buffered
   bytes are prepended to the remaining stream so nothing is lost.

## Redirect Tombstones

When an object is stored in the long-term backend, every future read must check
the long-term backend — which is typically slower and more expensive for "not
found" lookups. To avoid this, the service writes a **redirect tombstone** in
the high-volume backend alongside the real object in the long-term backend.

A redirect tombstone is an empty object with
[`is_redirect_tombstone: true`](objectstore_types::Metadata::is_redirect_tombstone)
in its metadata. It acts as a signpost: "the real data lives in the other
backend."

### How Each Operation Handles Tombstones

**Read** ([`StorageService::get_object`], [`StorageService::get_metadata`]):
1. Look up the object in the high-volume backend.
2. If the result is a tombstone, follow the redirect and fetch from the
   long-term backend instead.

**Write** ([`StorageService::insert_object`]):
1. If the object has a caller-provided key and a tombstone already exists at
   that key, route the new write to the long-term backend (preserving the
   existing tombstone as a redirect to the new data).
2. For long-term writes: first write the real object, then write the tombstone.
   If the tombstone write fails, the real object is cleaned up to avoid orphans.

**Delete** ([`StorageService::delete_object`]):
1. Attempt to delete from the high-volume backend, but skip deletion if it's a
   tombstone.
2. If a tombstone was found: delete the long-term object first, then delete the
   tombstone. This ordering ensures that if the long-term delete fails, the
   tombstone remains and the data is still reachable (no orphaned data).

## Backend Abstraction

All storage backends implement a common `Backend` trait with these operations:

| Method               | Description                                            |
|----------------------|--------------------------------------------------------|
| `put_object`         | Store an object with metadata and a payload stream     |
| `get_object`         | Retrieve an object's metadata and payload stream       |
| `get_metadata`       | Retrieve only metadata (default: calls `get_object`)   |
| `delete_object`      | Delete an object                                       |
| `delete_non_tombstone` | Delete only if not a redirect tombstone              |

### Supported Implementations

| Backend              | Name          | Typical Role   | Notes                                |
|----------------------|---------------|----------------|--------------------------------------|
| **BigTable**         | `"bigtable"`  | High-volume    | Separate column families for payload and metadata; supports TTI debounce |
| **GCS**              | `"gcs"`       | Long-term      | Metadata stored as GCS object metadata with `x-sn-*` headers |
| **LocalFS**          | `"local-fs"`  | Development    | Stores metadata as a JSON header prepended to the file |
| **S3-Compatible**    | `"s3"`        | Either         | Generic S3 protocol support          |

Backends are type-erased into a `BoxedBackend` (`Box<dyn Backend>`) so the
service can work with any combination.

## Object Identification

Every object is uniquely identified by an [`ObjectId`](id::ObjectId), which
consists of two parts:

- **[`ObjectContext`](id::ObjectContext)**: the _where_ — a usecase string plus
  an ordered collection of scopes.
- **Key**: the _what_ — a unique identifier within the context. Can be
  caller-provided or server-generated (UUID v4).

### Usecases

A usecase (sometimes called a "product") is a top-level namespace like
`"attachments"` or `"debug-files"`. It groups related objects and can have
usecase-specific server configuration (rate limits, killswitches, etc.).

### Scopes

[`Scopes`](objectstore_types::scope::Scopes) are ordered key-value pairs that
form a hierarchy within a usecase — for example,
`organization=17, project=42`. They serve as both an organizational structure
and an authorization boundary. See the
[`objectstore-types` docs](objectstore_types) for details on scope validation
and formatting.

### Storage Paths

The `ObjectId` is converted to a storage path for backends:

```text
{usecase}/{scope1_key}.{scope1_value}/{scope2_key}.{scope2_value}/objects/{key}
```

For example: `attachments/org.17/project.42/objects/abc123`

## Streaming Architecture

The service uses [`PayloadStream`] (a `BoxStream<'static, io::Result<Bytes>>`)
throughout its API to avoid buffering entire objects in memory. This is
important for large objects that may be tens or hundreds of megabytes.

Streaming is used at every layer: clients stream data to the server, the server
streams to the service, and the service streams to backends. The chunk-based
buffering during insert (up to the 1 MiB threshold) is the only point where
data is accumulated, and only up to the threshold — once exceeded, the rest
streams through.
