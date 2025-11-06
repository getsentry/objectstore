# Objectstore Client

The client is used to interface with the objectstore backend. It handles
responsibilities like transparent compression, and making sure that uploads and
downloads are done as efficiently as possible.

## Usage

```python
import datetime

import urllib3

from objectstore_client import (
    NoOpMetricsBackend,
    Objectstore,
    Scope,
    SentryScope,
    TimeToIdle,
    TimeToLive,
    Usecase,
)

# This can be stored in a global variable and reused, as it contains params that should likely remain the same across all uses
objectstore = Objectstore(
    "http://localhost:8888",
    # You can bring your own metrics backend to record things like latency, throughput, and payload sizes
    metrics_backend=NoOpMetricsBackend(),
    # Enables distributed traces in Sentry
    propagate_traces=False,
    # Optionally, provide kwargs for urllib3.HTTPConnectionPool
    # If you don't specify these, reasonable defaults are automatically applied
    timeout=urllib3.Timeout(connect=0.5, read=0.5),
    # ...
)

# This could also be stored in a global/shared variable, as you would most likely deal with a fixed number of usecases with statically defined defaults
my_usecase = Usecase(
    "my-usecase",
    compression="zstd",
    expiration_policy=TimeToLive(datetime.timedelta(days=1)),
)

# Get a client scoped to your usecase and scope
client = objectstore.get_client(my_usecase, SentryScope(organization=42, project=1337))

# We encourage using SentryScope, as it should fit most usecases.
# You can also use an arbitrary Scope, like so:
client = objectstore.get_client(
    my_usecase, Scope(organization=42, project=1337, app_slug="email_app")
)
# In that case, please still incorporate `organization` and/or `project` as the first two components if it's reasonable to do so

# These operations will raise an exception on failure

object_id = client.put(
    b"Hello, world!",
    # You can pass in your own identifier for the object to decide where to store the file.
    # Otherwise, Objectstore will pick a random one and return it.
    # A put request to an existing identifier overwrites the contents and metadata.
    # id="hello",
    metadata={"key": "value"},
    # Overrides the default defined at the Usecase level
    expiration_policy=TimeToIdle(datetime.timedelta(days=30)),
)

result = client.get(object_id)

content = result.payload.read()
assert content == b"Hello, world!"
assert result.metadata.custom["key"] == "value"

client.delete(object_id)
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
