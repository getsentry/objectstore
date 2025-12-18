from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from io import BytesIO
from typing import IO, Any, Literal, NamedTuple, cast
from urllib.parse import urlparse

import sentry_sdk
import urllib3
import zstandard
from urllib3.connectionpool import HTTPConnectionPool

from objectstore_client.auth import TokenGenerator
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
from objectstore_client.scope import Scope


class GetResponse(NamedTuple):
    metadata: Metadata
    payload: IO[bytes]


class RequestError(Exception):
    """Exception raised if an API call to Objectstore fails."""

    def __init__(self, message: str, status: int, response: str):
        super().__init__(message)
        self.status = status
        self.response = response


class Usecase:
    """
    An identifier for a workload in Objectstore, along with defaults to use for all
    operations within that Usecase.

    Usecases need to be statically defined in Objectstore's configuration server-side.
    Objectstore can make decisions based on the Usecase. For example, choosing the most
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


# Connect timeout used unless overridden in connection parameters.
DEFAULT_CONNECT_TIMEOUT = 0.1


@dataclass
class _ConnectionDefaults:
    retries: urllib3.Retry = urllib3.Retry(connect=3, read=0)
    """We only retry connection problems, as we cannot rewind our compression stream."""

    timeout: urllib3.Timeout = urllib3.Timeout(
        connect=DEFAULT_CONNECT_TIMEOUT, read=None
    )
    """
    The read timeout is defined to be "between consecutive read operations", which
    should mean one chunk of the response, with a large response being split into
    multiple chunks.

    By default, the client limits the connection phase to 100ms, and has no read
    timeout.
    """


class Client:
    """A client for Objectstore. Constructing it initializes a connection pool."""

    def __init__(
        self,
        base_url: str,
        metrics_backend: MetricsBackend | None = None,
        propagate_traces: bool = False,
        retries: int | None = None,
        timeout_ms: float | None = None,
        connection_kwargs: Mapping[str, Any] | None = None,
        token_generator: TokenGenerator | None = None,
    ):
        """
        Initialize an Objectstore client.

        Args:
            base_url: The base URL of the Objectstore server (e.g.,
                "http://objectstore:8888"). metrics_backend: Optional metrics backend
                for tracking storage operations. Defaults to ``NoOpMetricsBackend`` if
                not provided.
            propagate_traces: Whether to propagate Sentry trace headers in requests to
                objectstore. Defaults to ``False``.
            retries: Number of connection retries for failed requests.
                Defaults to ``3`` if not specified. Note: only connection failures are
                retried, not read failures (as compression streams cannot be rewound).
            timeout_ms: Read timeout in milliseconds for API requests. The read timeout
                is the maximum time to wait between consecutive read operations on the
                socket (i.e., between receiving chunks of data). Defaults to no read
                timeout if not specified. The connection timeout is always 100ms. To
                override the connection timeout, pass a custom ``urllib3.Timeout``
                object via ``connection_kwargs``. For example:

                .. code-block:: python

                    client = Client(
                        "http://objectstore:8888", connection_kwargs={
                            "timeout": urllib3.Timeout(connect=1.0, read=5.0)
                        }
                    )

            connection_kwargs: Additional keyword arguments to pass to the underlying
                urllib3 connection pool (e.g., custom headers, SSL settings, advanced
                timeouts).
        """
        connection_kwargs_to_use = asdict(_ConnectionDefaults())

        if retries:
            connection_kwargs_to_use["retries"] = urllib3.Retry(
                connect=retries,
                # we only retry connection problems, as we cannot rewind our
                # compression stream
                read=0,
            )

        if timeout_ms:
            connection_kwargs_to_use["timeout"] = urllib3.Timeout(
                connect=DEFAULT_CONNECT_TIMEOUT, read=timeout_ms / 1000
            )

        if connection_kwargs:
            connection_kwargs_to_use = {**connection_kwargs_to_use, **connection_kwargs}

        self._pool = urllib3.connectionpool.connection_from_url(
            base_url, **connection_kwargs_to_use
        )
        self._base_path = urlparse(base_url).path
        self._metrics_backend = metrics_backend or NoOpMetricsBackend()
        self._propagate_traces = propagate_traces
        self._token_generator = token_generator

    def session(self, usecase: Usecase, **scopes: str | int | bool) -> Session:
        """
        Create a [Session] with the Objectstore server, tied to a specific [Usecase] and
        [Scope].

        A Scope is a (possibly nested) namespace within a Usecase, given as a sequence
        of key-value pairs passed as kwargs.
        IMPORTANT: the order of the kwargs matters!

        The admitted characters for keys and values are: `A-Za-z0-9_-()$!+*'`.

        Users are free to choose the scope structure that best suits their Usecase.
        The combination of Usecase and Scope will determine the physical key/path of the
        blob in the underlying storage backend.

        For most usecases, it's recommended to use the organization and project ID as
        the first components of the scope, as follows:
        ```
        client.session(usecase, org=organization_id, project=project_id, ...)
        ```
        """

        return Session(
            self._pool,
            self._base_path,
            self._metrics_backend,
            self._propagate_traces,
            usecase,
            Scope(**scopes),
            self._token_generator,
        )


class Session:
    """
    A session with the Objectstore server, scoped to a specific [Usecase] and Scope.

    This should never be constructed directly, use [Client.session].
    """

    def __init__(
        self,
        pool: HTTPConnectionPool,
        base_path: str,
        metrics_backend: MetricsBackend,
        propagate_traces: bool,
        usecase: Usecase,
        scope: Scope,
        token_generator: TokenGenerator | None,
    ):
        self._pool = pool
        self._base_path = base_path
        self._metrics_backend = metrics_backend
        self._propagate_traces = propagate_traces
        self._usecase = usecase
        self._scope = scope
        self._token_generator = token_generator

    def _make_headers(self) -> dict[str, str]:
        headers = dict(self._pool.headers)
        if self._propagate_traces:
            headers.update(
                dict(sentry_sdk.get_current_scope().iter_trace_propagation_headers())
            )
        if self._token_generator:
            token = self._token_generator.sign_for_scope(
                self._usecase.name, self._scope
            )
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _make_url(self, key: str | None, full: bool = False) -> str:
        relative_path = f"/v1/objects/{self._usecase.name}/{self._scope}/{key or ''}"
        path = self._base_path.rstrip("/") + relative_path
        if full:
            return f"http://{self._pool.host}:{self._pool.port}{path}"
        return path

    def put(
        self,
        contents: bytes | IO[bytes],
        key: str | None = None,
        compression: Compression | Literal["none"] | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expiration_policy: ExpirationPolicy | None = None,
    ) -> str:
        """
        Uploads the given `contents` to blob storage.

        If no `key` is provided, one will be automatically generated and returned
        from this function.

        The client will select the configured `default_compression` if none is given
        explicitly.
        This can be overridden by explicitly giving a `compression` argument.
        Providing `"none"` as the argument will instruct the client to not apply
        any compression to this upload, which is useful for uncompressible formats.

        You can use the utility function `objectstore_client.utils.guess_mime_type`
        to attempt to guess a `content_type` based on magic bytes.
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

        if key == "":
            key = None

        with measure_storage_operation(
            self._metrics_backend, "put", self._usecase.name
        ) as metric_emitter:
            response = self._pool.request(
                "POST" if not key else "PUT",
                self._make_url(key),
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

    def get(self, key: str, decompress: bool = True) -> GetResponse:
        """
        This fetches the blob with the given `key`, returning an `IO` stream that
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
                self._make_url(key),
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

        return GetResponse(metadata, stream)

    def object_url(self, key: str) -> str:
        """
        Generates a GET url to the object with the given `key`.

        This can then be used by downstream services to fetch the given object.
        NOTE however that the service does not strictly follow HTTP semantics,
        in particular in relation to `Accept-Encoding`.
        """
        return self._make_url(key, full=True)

    def delete(self, key: str) -> None:
        """
        Deletes the blob with the given `key`.
        """

        headers = self._make_headers()
        with measure_storage_operation(
            self._metrics_backend, "delete", self._usecase.name
        ):
            response = self._pool.request(
                "DELETE",
                self._make_url(key),
                headers=headers,
            )
            raise_for_status(response)


def raise_for_status(response: urllib3.BaseHTTPResponse) -> None:
    if response.status >= 400:
        res = (response.data or response.read() or b"").decode("utf-8", "replace")
        raise RequestError(
            f"Objectstore request failed with status {response.status}",
            response.status,
            res,
        )
