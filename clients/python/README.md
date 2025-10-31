# Objectstore Client

The client is used to interface with the objectstore backend. It handles
responsibilities like transparent compression, and making sure that uploads and
downloads are done as efficiently as possible.

## Usage

```python
import datetime

from objectstore_client import ClientBuilder, NoOpMetricsBackend, TimeToLive

client_builder = ClientBuilder(
    "http://localhost:8888"
    "my_usecase",
    metrics_backend=NoOpMetricsBackend(),  # optionally, provide your own MetricsBackend implementation
)
client = client_builder.for_project(42, 424242)

object_id = client.put(
    b"Hello, world!",
    metadata={"key": "value"},
    expiration_policy=TimeToLive(datetime.timedelta(days=1)),
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
