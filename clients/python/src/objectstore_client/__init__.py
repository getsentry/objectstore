from objectstore_client.auth import Permission, SecretKey, TokenProvider
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
    "SecretKey",
    "TokenGenerator",
    "MetricsBackend",
    "NoOpMetricsBackend",
    "parse_accept_encoding",
]


def __getattr__(name: str) -> type:
    import warnings

    if name == "TokenGenerator":
        warnings.warn(
            "TokenGenerator has been renamed to SecretKey; "
            "update your imports to use SecretKey instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return SecretKey
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
