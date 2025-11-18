from __future__ import annotations

import string
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from io import BytesIO
from typing import IO, Any, Literal, NamedTuple, cast

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


# Characters allowed in a Scope's key and value.
# These are the URL safe characters, except for `.` which we use as separator between
# key and value of Scope components.
SCOPE_VALUE_ALLOWED_CHARS = set(string.ascii_letters + string.digits + "-_()$!+'")


@dataclass
class _ConnectionDefaults:
    retries: urllib3.Retry = urllib3.Retry(connect=3, read=0)
    """We only retry connection problems, as we cannot rewind our compression stream."""

    timeout: urllib3.Timeout = urllib3.Timeout(connect=0.5, read=0.5)
    """
    The read timeout is defined to be "between consecutive read operations",
    which should mean one chunk of the response, with a large response being
    split into multiple chunks.
    We define both as 500ms which is still very conservative,
    given that we are in the same network, and expect our backends to respond in <100ms.
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
    ):
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
                connect=timeout_ms * 100, read=timeout_ms * 100
            )

        if connection_kwargs:
            connection_kwargs_to_use = {**connection_kwargs_to_use, **connection_kwargs}

        self._pool = urllib3.connectionpool.connection_from_url(
            base_url, **connection_kwargs_to_use
        )
        self._metrics_backend = metrics_backend or NoOpMetricsBackend()
        self._propagate_traces = propagate_traces

    def session(self, usecase: Usecase, **scopes: str | int | bool) -> Session:
        """
        Create a [Session] with the Objectstore server, tied to a specific [Usecase] and
        Scope.

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

        parts = []
        for key, value in scopes.items():
            if not key:
                raise ValueError("Scope key cannot be empty")
            if not value:
                raise ValueError("Scope value cannot be empty")

            if any(c not in SCOPE_VALUE_ALLOWED_CHARS for c in key):
                raise ValueError(
                    f"Invalid scope key {key}. The valid character set is: "
                    f"{''.join(SCOPE_VALUE_ALLOWED_CHARS)}"
                )

            value = str(value)
            if any(c not in SCOPE_VALUE_ALLOWED_CHARS for c in value):
                raise ValueError(
                    f"Invalid scope value {value}. The valid character set is: "
                    f"{''.join(SCOPE_VALUE_ALLOWED_CHARS)}"
                )

            formatted = f"{key}.{value}"
            parts.append(formatted)
        scope_str = "/".join(parts)

        return Session(
            self._pool,
            self._metrics_backend,
            self._propagate_traces,
            usecase,
            scope_str,
        )


class Session:
    """
    A session with the Objectstore server, scoped to a specific [Usecase] and Scope.

    This should never be constructed directly, use [Client.session].
    """

    def __init__(
        self,
        pool: HTTPConnectionPool,
        metrics_backend: MetricsBackend,
        propagate_traces: bool,
        usecase: Usecase,
        scope: str,
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

    def _make_url(self, key: str | None, full: bool = False) -> str:
        base_path = f"/v1/{self._usecase.name}/{self._scope}/objects/{key or ''}"
        if full:
            return f"http://{self._pool.host}:{self._pool.port}{base_path}"
        else:
            return f"{base_path}"

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

        with measure_storage_operation(
            self._metrics_backend, "put", self._usecase.name
        ) as metric_emitter:
            response = self._pool.request(
                "PUT",
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
        res = str(response.data or response.read())
        raise RequestError(
            f"Objectstore request failed with status {response.status}",
            response.status,
            res,
        )
