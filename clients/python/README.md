# Objectstore Client

The client is used to interface with the [Objectstore](https://getsentry.github.io/objectstore/) backend. It handles
responsibilities like transparent compression, and making sure that uploads and
downloads are done as efficiently as possible.

## Quick Start

```python
from objectstore_client import Client, Usecase

client = Client("http://localhost:8888")
session = client.session(Usecase("attachments"), org=42, project=1337)

# Upload
key = session.put(b"Hello, world!")

# Download
result = session.get(key)
content = result.payload.read()

# Delete
session.delete(key)
```

## Core Concepts

### Usecases and Scopes

A `Usecase` represents a server-side namespace with its own configuration defaults.
Within a Usecase, Scopes provide further isolation — typically keyed by organization
and project IDs. A Session ties a Client to a specific Usecase + Scope for operations.

Scope components form a hierarchical path, so their order matters:
`org=42;project=1337` and `project=1337;org=42` are different scopes. We recommend
using `org` and `project` as the first two components.

```python
# Scope with org and project (recommended first components)
session = client.session(Usecase("attachments"), org=42, project=1337)

# Additional components are appended after org/project
session = client.session(Usecase("attachments"), org=42, project=1337, app_slug="email_app")
```

### Expiration

Objects can expire automatically using Time To Live (from creation) or Time To Idle
(from last access). Defaults are set at the Usecase level and can be overridden per-upload.
Without an expiration policy, objects use manual expiration (no auto-deletion).

**We strongly recommend setting an expiration policy on every Usecase** to prevent
unbounded storage growth. Choose `TimeToIdle` for cache-like data that should stay
alive while actively used, or `TimeToLive` for data with a fixed retention period.

```python
from datetime import timedelta
from objectstore_client import Usecase, TimeToIdle, TimeToLive

# Set default expiration on the Usecase
usecase = Usecase("attachments", expiration_policy=TimeToIdle(timedelta(days=30)))

# Override per-upload
session.put(b"payload", expiration_policy=TimeToLive(timedelta(hours=1)))
```

### Origin Tracking

We encourage setting the `origin` on every upload to track where the payload was
originally obtained from (e.g., the IP address of the Sentry SDK or CLI). This is
optional but helps with auditing and debugging.

```python
session.put(b"payload", origin="203.0.113.42")
```

### Compression

Uploads are compressed with Zstd by default, and downloads are transparently decompressed.
Compression can be overridden per-upload:

```python
# upload as-is and record no encoding:
session.put(video_data, compress="none")

# upload as-is, but record the encoding so that downloads still decompress:
session.put(zstd_data, precompressed="zstd")
```

### Custom Metadata

Arbitrary key-value pairs can be attached to objects and retrieved on download.

```python
session.put(b"payload", metadata={"source": "upload-service"})
```

### Multipart Upload API

For large objects, use multipart uploads to upload parts independently and then
assemble them into a final object.

**Important:** unlike single-object uploads, multipart uploads do **not** auto-compress.
`compression` on `initiate_multipart_upload` behaves like `precompressed` above: it only
records the algorithm, and the caller must pre-compress each part accordingly.

```python
from concurrent.futures import ThreadPoolExecutor

import zstandard

from objectstore_client.multipart import MultipartCompleteError

upload = session.initiate_multipart_upload(
    key="my-large-object",
    compression="zstd",
    metadata={"source": "upload-service"},
)

compressor = zstandard.ZstdCompressor()
chunks = [b"part1", b"part2", b"part3", b"part4"]

def upload_part(part_number: int, data: bytes):
    compressed = compressor.compress(data)
    return upload.put_part(
        compressed, part_number=part_number, content_length=len(compressed)
    )

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(upload_part, i + 1, chunk)
        for i, chunk in enumerate(chunks)
    ]
    parts = [f.result() for f in futures]

try:
    key = upload.complete(parts)
except MultipartCompleteError:
    upload.abort()
    raise
```

To resume an in-progress multipart upload after a process restart, persist the
`key` and `upload_id`, then reconstruct the upload handle later:

```python
saved_key = upload.key
saved_upload_id = upload.upload_id

resumed = session.resume_multipart_upload(saved_key, saved_upload_id)
existing_parts = resumed.list_parts()

# Upload missing parts...

key = resumed.complete(new_parts + existing_parts)
```

### Many API (batch operations)

`session.many()` executes a number of operations with as few requests as
possible. Those within the batch protocol's per-part limit of 1 MB are grouped
into requests to Objectstore's batch endpoint which cuts network overhead
considerably. Inserts too large for that, or of unknown size, are sent as
individual requests instead.

Pass any iterable of `Get`, `Put`, `Delete`, and `Head` operations, which live in
the `many` module. Results come back as `GetResult`, `PutResult`, `DeleteResult`,
and `HeadResult` objects, each carrying the object's `key` and an `error` that is
`None` when the operation succeeded:

```python
from objectstore_client import Client, Usecase, many

client = Client("http://localhost:8888")
session = client.session(Usecase("attachments"), org=42, project=1337)

results = session.many(
    [
        many.Put(b"file1 contents", key="file1"),
        many.Put(b"file2 contents", key="file2"),
        many.Get("file3"),
        many.Delete("file4"),
        many.Head("file5"),
    ]
)

for result in results:
    if result.error is not None:
        ...  # this operation failed
    elif isinstance(result, many.GetResult):
        # `response` is None if the object does not exist.
        payload = result.response.payload if result.response else None
```

`session.many()` returns an `OperationResults` object which is a lazy iterator.
As the iterator is consumed, it assembles batch requests and sends them to
Objectstore. As responses come in, the operation results are yielded. Abandoning
the iterator without fully consuming it will cancel whatever has not been
dispatched yet.

If successful results don't need to be processed or inspected, callers can call
`raise_for_failures()` to drain the results and raise an `ExceptionGroup` with
all per-operation errors, or `failures()` which returns the failed results as a
list:

```python
session.many([many.Delete("file1"), many.Delete("file2")]).raise_for_failures()

for failure in session.many([many.Delete("file3")]).failures():
    print(failure.key, failure.error)
```

#### Concurrency

`concurrency` caps how many requests are in flight, and defaults to `3`.
Requests run on a thread pool created by `session.many()`, which is shut down
when the results are exhausted or abandoned. When `concurrency` is set to `1`,
requests are run serially on the caller thread instead, with no thread pool.

```python
client = Client("http://localhost:8888")
session = client.session(Usecase("attachments"), org=42, project=1337)

for result in session.many(operations, concurrency=8):
    ...
```

Note: when a `Client` is built with custom `connection_kwargs` that include
`"block": True`, the `concurrency` argument is clamped to the connection pool's
configured size. An illustrative example:

```python
# Client created with a pool size of 4 and block=True
client = Client(
    "http://localhost:8888",
    connection_kwargs={"maxsize": 4, "block": True},
)
session = client.session(Usecase("attachments"), org=42, project=1337)

# `concurrency` is clamped to `4` because `block=True` was set
for failure in session.many(operations, concurrency=16).failures():
    print(failure.key, failure.error)
```

Results are yielded as responses are received, and the order isn't necessarily
the same order that operations were given in. Each result carries an `index`
field that corresponds to the index of the `Get` / `Put` / `Delete` / `Head`
operation in the operation iterable passed into `session.many()`. This `index`
allows a keyless `Put` operation to be linked with its result to learn the key
that was assigned.

```python
uploads = [many.Put(b"first"), many.Put(b"second")]

for result in session.many(uploads):
    print(f"{uploads[result.index].contents!r} was stored as {result.key}")
```

An `ErrorResult` carries `index=None` when the response part it came from could
not be attributed to any operation at all.

Within a single batch, the Objectstore server processes individual operations
concurrently and each operation's relative order is undefined. Two operations on
the same key therefore race, and `session.many()` does nothing to prevent that.

#### Metrics

When a metrics backend is configured, `session.many()` emits some metrics:
- `storage.batch.latency`: a timer recording a batch request's execution time,
  tagged with a (bucketed) number of operations included in the batch
- `stoarge.batch.operations`: a simple counter of individual operations, tagged
  with each operation's kind (i.e. `PUT`/`GET`/`DELETE`).

An operation that doesn't qualify for batching will be sent through the
`session`'s regular single-operation API for that operation and will emit
single-object metrics on that path rather than batch metrics here.

### Authentication

If your Objectstore instance enforces authorization, you must configure authentication
via the `token` parameter on `Client`. It accepts either:

- A **`SecretKey`** — for internal services that have access to an EdDSA keypair.
  The key signs a fresh JWT for each request, scoped to the specific usecase
  and scope being accessed, and can also sign pre-signed URLs.
- A **`str`** — a pre-signed JWT, used as-is for every request.
  Use this for external services that receive a token from another source.

```python
from objectstore_client import Client, Usecase
from objectstore_client.auth import SecretKey

# Option 1: Internal service with a keypair
client = Client(
    "http://localhost:8888",
    token=SecretKey(kid="my-service", secret_key="<private key>"),
)

# Option 2: External service with a pre-signed JWT
# Use SecretKey.token_for_scope() to obtain a static token from an
# internal service, then pass it to the external consumer:
from objectstore_client.scope import Scope

token = SecretKey(
    kid="my-service", secret_key="<private key>",
).token_for_scope("my_app", Scope(org=42, project=1337))

client = Client("http://localhost:8888", token=token)
```

### Object URLs

`Session.object_url` returns a GET URL for an object. By default the URL carries
no auth, so the recipient needs their own key to generate a token for it. Pass
`token_validity` to embed a read-only token in the URL instead, valid for the
given duration (requires `SecretKey` authentication - see above section).

```python
from datetime import timedelta

url = session.object_url("my-key", token_validity=timedelta(hours=1))
```

### Pre-signed URLs

> **Experimental:** pre-signed URLs are an experimental feature and this API may
> change in a future release.

A **pre-signed URL** is a time-limited URL that authorizes a single request on
one object without the recipient needing an auth token. This is useful for
handing a download link to a browser or an external service, or for returning
an HTTP redirect so that clients can download objects directly from
Objectstore.

Pre-signed URLS currently only support `GET` and `HEAD` (a pre-signed URL for
`GET` authorizes `HEAD` too, and viceversa), so for uploads you should use
scoped JWTs, as described above.

`Session.presigned_object_url` signs the URL with the session's `SecretKey`,
so it requires a `SecretKey` and raises `ValueError` otherwise.
Only `GET` and `HEAD` may be pre-signed, the granted permissions are those
configured server-side for the signing key, and the
validity may not exceed one week.

```python
from datetime import timedelta

# The recipient can fetch this with any HTTP client, no auth header needed.
url = session.presigned_object_url("GET", "my-key", duration=timedelta(hours=1))

import urllib.request
with urllib.request.urlopen(url) as resp:
    content = resp.read()
```

## Configuration

In production, store the `Client` and `Usecase` at module level and reuse them.
The following shows all available constructor options with their defaults:

```python
from objectstore_client import Client, Usecase

client = Client(
    "http://localhost:8888",
    propagate_traces=False,  # default
    retries=3,               # default: 3 connect retries, no read retries
    timeout_ms=None,         # default: no read timeout (connect: 100ms)
    connection_kwargs={},    # default: empty (override urllib3.HTTPConnectionPool kwargs)
    # metrics_backend=...,   # default: no-op
    # token=...,             # see Authentication section
)

attachments = Usecase("attachments")
```

See the docstrings on `Client`, `Usecase`, and `Session` for full parameter documentation.

### Tracing

When the Sentry SDK is initialized, every method that talks to Objectstore
automatically emits a span, with no setup required on your side. If you also
want a span for the underlying HTTP request, Sentry's `StdlibIntegration`
covers that, it's enabled by default:

```python
import sentry_sdk

sentry_sdk.init(
    dsn="...",
    traces_sample_rate=1.0,
    # Optional: controls which hosts get `sentry-trace`/`baggage` headers.
    # trace_propagation_targets=["http://objectstore"],
)
```

## Development

### Environment Setup

The considerations for setting up the development environment that can be found in the main [README](../README.md) apply for this package as well.

### Pre-commit hook

A configuration to set up a git pre-commit hook using [pre-commit](https://github.com/pre-commit/pre-commit) is available at the root of the repository.

To install it, run
```sh
pre-commit install
```

The hook will automatically run some checks before every commit, including the linters and formatters we run in CI.
