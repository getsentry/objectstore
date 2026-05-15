from objectstore_client.auth import Permission, TokenGenerator, TokenProvider
from objectstore_client.client import (
    Client,
    GetResponse,
    Session,
    Usecase,
)
from objectstore_client.errors import RequestError
from objectstore_client.metadata import (
    Compression,
    ExpirationPolicy,
    Metadata,
    TimeToIdle,
    TimeToLive,
)
from objectstore_client.metrics import MetricsBackend, NoOpMetricsBackend
from objectstore_client.utils import parse_accept_encoding

# Set canonical module so Sphinx doesn't see ambiguous cross-references
# between e.g. objectstore_client.Session and objectstore_client.client.Session.
for _sym in list(locals().values()):
    if isinstance(_sym, type) and _sym.__module__.startswith("objectstore_client."):
        _sym.__module__ = __name__

__all__ = [
    "Client",
    "Usecase",
    "Session",
    "GetResponse",
    "RequestError",
    "Compression",
    "ExpirationPolicy",
    "Metadata",
    "Permission",
    "TimeToIdle",
    "TimeToLive",
    "TokenProvider",
    "TokenGenerator",
    "MetricsBackend",
    "NoOpMetricsBackend",
    "parse_accept_encoding",
]
