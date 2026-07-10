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

Uploads are compressed with Zstd by default. Downloads are transparently decompressed.
You can override compression per-upload for pre-compressed or uncompressible data.

```python
session.put(already_compressed_data, compression="none")
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
The caller must pre-compress each part according to the compression set as part of the metadata
when initiating the upload.

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

### Pre-signed URLs

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
