# Objectstore Client

The client is used to interface with the objectstore backend. It handles
responsibilities like transparent compression, and making sure that uploads and
downloads are done as efficiently as possible.

## Usage

```python
import datetime

import urllib3

from objectstore_client import (
    Client,
    NoOpMetricsBackend,
    TimeToIdle,
    TimeToLive,
    Usecase,
)

# This should be stored in a global variable and reused, in order to reuse the connection
client = Client(
    "http://localhost:8888",
    # Optionally, bring your own metrics backend to record things like latency, throughput, and payload sizes
    metrics_backend=NoOpMetricsBackend(),
    # Optionally, enable distributed traces in Sentry
    propagate_traces=True,
    # Optionally, set timeout and retries
    timeout_ms=500, # 500ms timeout for requests
    retries=3,      # Number of connection retries
    # For further customization, provide additional kwargs for urllib3.HTTPConnectionPool
    connection_kwargs={"maxsize": 10},
)

# This could also be stored in a global/shared variable, as you will deal with a fixed number of usecases with statically defined defaults
my_usecase = Usecase(
    "my-usecase",
    # Optionally, define defaults for all operations within this Usecase
    expiration_policy=TimeToLive(datetime.timedelta(days=1)),
)

# Start a Session, tied to your Usecase and a Scope.
# A Scope is a (possibly nested) namespace within Objectstore that provides isolation within a Usecase.
# The Scope is given as a sequence of key-value pairs through kwargs.
# Note that order matters!
# The admitted characters for keys and values are: `A-Za-z0-9_-()$!+*'`.
# You're encouraged to use the organization and project ID as the first components of the scope, as follows:
session = client.session(
    my_usecase, org=42, project=1337, app_slug="email_app"
)

# The following operations will raise an exception on failure

# Write an object and metadata
object_key = session.put(
    b"Hello, world!",
    # You can pass in your own identifier for the object to decide where to store the file.
    # Otherwise, Objectstore will pick an identifier and return it.
    # A put request to an existing identifier overwrites the contents and metadata.
    # id="hello",
    metadata={"key": "value"},
    # Overrides the default defined at the Usecase level
    expiration_policy=TimeToIdle(datetime.timedelta(days=30)),
)

# Read an object and its metadata
result = session.get(object_key)

content = result.payload.read()
assert content == b"Hello, world!"
assert result.metadata.custom["key"] == "value"

# Delete an object
session.delete(object_key)
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
