# Objectstore Client

The client is used to interface with the objectstore backend. It handles
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
`org=42/project=1337` and `project=1337/org=42` are different scopes. We recommend
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
    # token_generator=...,   # for authorized Objectstore instances
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
