from __future__ import annotations

import string
from io import BytesIO
from types import SimpleNamespace
from typing import IO, Any, Literal, NamedTuple, cast
from urllib.parse import urlencode

import sentry_sdk
import urllib3
import zstandard
from urllib3.connectionpool import HTTPConnectionPool

from objectstore_client.metadata import (
    HEADER_EXPIRATION,
    HEADER_META_PREFIX,
    Compression,
    ExpirationPolicy,
    Metadata,
    format_expiration,
)
from objectstore_client.metrics import (
    MetricsBackend,
    NoOpMetricsBackend,
    measure_storage_operation,
)

Permission = Literal["read", "write"]


class GetResult(NamedTuple):
    metadata: Metadata
    payload: IO[bytes]


class Usecase:
    """
    An identifier for a workload in Objectstore, along with defaults to use for all
    operations within that usecase.

    Usecases need to be statically defined in Objectstore's configuration.
    Objectstore can make decisions based on the usecase. For example, choosing the most
    suitable storage backend.
    """

    name: str
    _compression: Compression
    _expiration_policy: ExpirationPolicy | None

    def __init__(
        self,
        name: str,
        compression: Compression = "zstd",
        expiration_policy: ExpirationPolicy | None = None,
    ):
        self.name = name
        self._compression = compression
        self._expiration_policy = expiration_policy


# URL safe characters, except for `.` which we use as separator between key and value
# of Scope components
SCOPE_ALLOWED_CHARS = set(string.ascii_letters + string.digits + "-_()$!+*'")


class Scope:
    """
    A (possibly nested) namespace within a usecase, given as a sequence of key-value
    pairs passed as kwargs.
    The emtpy scope is not admitted. Order of the components matters.

    The admitted characters for keys and values are: `[A-Za-z0-9_-]`.

    Users are free to choose the scope structure that best suits their usecase.
    The combination of Usecase and Scope will determine the physical path of the
    blob in the underlying storage backend.

    For most usecases, [SentryScope] should be used.
    """

    def __init__(self, **scopes: str | int | bool) -> None:
        if len(scopes) == 0:
            raise ValueError("At least 1 scope is needed")

        parts = []
        for key, value in scopes.items():
            value = str(value)
            if any(c not in SCOPE_ALLOWED_CHARS for c in value):
                raise ValueError(
                    f"Invalid scope value {value}. The valid character set is: "
                    f"{''.join(SCOPE_ALLOWED_CHARS)}"
                )

            formatted = f"{key}.{value}"
            parts.append(formatted)

        self._str = "/".join(parts)

    def __repr__(self) -> str:
        return self._str


class SentryScope(Scope):
    """
    The recommended Scope that should fit for most usecases within Sentry.
    """

    def __init__(self, organization: int, project: int | None) -> None:
        if project:
            super().__init__(organization=organization, project=project)
        else:
            super().__init__(organization=organization)


_CONNECTION_POOL_DEFAULTS = SimpleNamespace(
    # We only retry connection problems, as we cannot rewind our compression stream.
    retries=urllib3.Retry(connect=3, redirect=5, read=0),
    # The read timeout is defined to be "between consecutive read operations",
    # which should mean one chunk of the response, with a large response being
    # split into multiple chunks.
    # We define both as 500ms which is still very conservative,
    # given that we are in the same network,
    # and expect our backends to respond in <100ms.
    timeout=urllib3.Timeout(connect=0.5, read=0.5),
)


class Objectstore:
    """A connection to the Objectstore service."""

    def __init__(
        self,
        base_url: str,
        metrics_backend: MetricsBackend | None = None,
        propagate_traces: bool = False,
        **connection_kwargs: Any,
    ):
        connection_kwargs_to_use = vars(_CONNECTION_POOL_DEFAULTS)
        if connection_kwargs:
            for k, v in connection_kwargs.items():
                connection_kwargs_to_use[k] = v

        self._pool = urllib3.connectionpool.connection_from_url(
            base_url, **connection_kwargs_to_use
        )
        self._metrics_backend = metrics_backend or NoOpMetricsBackend()
        self._propagate_traces = propagate_traces

    def get_client(self, usecase: Usecase, scope: Scope) -> Client:
        return Client(
            self._pool, self._metrics_backend, self._propagate_traces, usecase, scope
        )


class Client:
    def __init__(
        self,
        pool: HTTPConnectionPool,
        metrics_backend: MetricsBackend,
        propagate_traces: bool,
        usecase: Usecase,
        scope: Scope,
    ):
        self._pool = pool
        self._metrics_backend = metrics_backend
        self._propagate_traces = propagate_traces
        self._usecase = usecase
        self._scope = scope

    def _make_headers(self) -> dict[str, str]:
        if self._propagate_traces:
            return dict(sentry_sdk.get_current_scope().iter_trace_propagation_headers())
        return {}

    def _make_url(self, id: str | None, full: bool = False) -> str:
        base_path = f"/v1/{id}" if id else "/v1/"
        qs = urlencode({"usecase": self._usecase.name, "scope": str(self._scope)})
        if full:
            return f"http://{self._pool.host}:{self._pool.port}{base_path}?{qs}"
        else:
            return f"{base_path}?{qs}"

    def put(
        self,
        contents: bytes | IO[bytes],
        id: str | None = None,
        compression: Compression | Literal["none"] | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expiration_policy: ExpirationPolicy | None = None,
    ) -> str:
        """
        Uploads the given `contents` to blob storage.

        If no `id` is provided, one will be automatically generated and returned
        from this function.

        The client will select the configured `default_compression` if none is given
        explicitly.
        This can be overridden by explicitly giving a `compression` argument.
        Providing `"none"` as the argument will instruct the client to not apply
        any compression to this upload, which is useful for uncompressible formats.
        """
        headers = self._make_headers()
        body = BytesIO(contents) if isinstance(contents, bytes) else contents
        original_body: IO[bytes] = body

        compression = compression or self._usecase._compression
        if compression == "zstd":
            cctx = zstandard.ZstdCompressor()
            body = cctx.stream_reader(original_body)
            headers["Content-Encoding"] = "zstd"

        if content_type:
            headers["Content-Type"] = content_type

        expiration_policy = expiration_policy or self._usecase._expiration_policy
        if expiration_policy:
            headers[HEADER_EXPIRATION] = format_expiration(expiration_policy)

        if metadata:
            for k, v in metadata.items():
                headers[f"{HEADER_META_PREFIX}{k}"] = v

        with measure_storage_operation(
            self._metrics_backend, "put", self._usecase.name
        ) as metric_emitter:
            response = self._pool.request(
                "PUT",
                self._make_url(id),
                body=body,
                headers=headers,
                preload_content=True,
                decode_content=True,
            )
            raise_for_status(response)
            res = response.json()

            # Must do this after streaming `body` as that's what is responsible
            # for advancing the seek position in both streams
            metric_emitter.record_uncompressed_size(original_body.tell())
            if compression and compression != "none":
                metric_emitter.record_compressed_size(body.tell(), compression)
            return res["key"]

    def get(self, id: str, decompress: bool = True) -> GetResult:
        """
        This fetches the blob with the given `id`, returning an `IO` stream that
        can be read.

        By default, content that was uploaded compressed will be automatically
        decompressed, unless `decompress=True` is passed.
        """

        headers = self._make_headers()
        with measure_storage_operation(
            self._metrics_backend, "get", self._usecase.name
        ):
            response = self._pool.request(
                "GET",
                self._make_url(id),
                preload_content=False,
                decode_content=False,
                headers=headers,
            )
            raise_for_status(response)
        # OR: should I use `response.stream()`?
        stream = cast(IO[bytes], response)
        metadata = Metadata.from_headers(response.headers)

        if metadata.compression and decompress:
            if metadata.compression != "zstd":
                raise NotImplementedError(
                    "Transparent decoding of anything but `zstd` is not implemented yet"
                )

            metadata.compression = None
            dctx = zstandard.ZstdDecompressor()
            stream = dctx.stream_reader(stream, read_across_frames=True)

        return GetResult(metadata, stream)

    def object_url(self, id: str) -> str:
        """
        Generates a GET url to the object with the given `id`.

        This can then be used by downstream services to fetch the given object.
        NOTE however that the service does not strictly follow HTTP semantics,
        in particular in relation to `Accept-Encoding`.
        """
        return self._make_url(id, full=True)

    def delete(self, id: str) -> None:
        """
        Deletes the blob with the given `id`.
        """

        headers = self._make_headers()
        with measure_storage_operation(
            self._metrics_backend, "delete", self._usecase.name
        ):
            response = self._pool.request(
                "DELETE",
                self._make_url(id),
                headers=headers,
            )
            raise_for_status(response)


class ClientError(Exception):
    def __init__(self, message: str, status: int, response: str):
        super().__init__(message)
        self.status = status
        self.response = response


def raise_for_status(response: urllib3.BaseHTTPResponse) -> None:
    if response.status >= 400:
        res = str(response.data or response.read())
        raise ClientError(
            f"Objectstore request failed with status {response.status}",
            response.status,
            res,
        )
