from __future__ import annotations

from io import BytesIO
from typing import IO, Literal, NamedTuple, NotRequired, Self, TypedDict, cast
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


class Scope(TypedDict):
    organization: int
    project: NotRequired[int]


class GetResult(NamedTuple):
    metadata: Metadata
    payload: IO[bytes]


class ClientBuilder:
    def __init__(
        self,
        objectstore_base_url: str,
        usecase: str,
        metrics_backend: MetricsBackend | None = None,
        propagate_traces: bool = False,
        default_expiration_policy: ExpirationPolicy | None = None,
    ):
        self._base_url = objectstore_base_url
        self._usecase = usecase
        self._default_compression: Compression = "zstd"
        self._default_expiration_policy = (
            format_expiration(default_expiration_policy)
            if default_expiration_policy
            else None
        )
        self._propagate_traces = propagate_traces
        self._metrics_backend = metrics_backend or NoOpMetricsBackend()

    def _make_client(self, scope: str) -> Client:
        pool = urllib3.connectionpool.connection_from_url(self._base_url)
        return Client(
            pool,
            self._default_compression,
            self._default_expiration_policy,
            self._usecase,
            scope,
            self._propagate_traces,
            self._metrics_backend,
        )

    def default_compression(self, default_compression: Compression) -> Self:
        self._default_compression = default_compression
        return self

    def for_organization(self, organization_id: int) -> Client:
        return self._make_client(f"org.{organization_id}")

    def for_project(self, organization_id: int, project_id: int) -> Client:
        return self._make_client(f"org.{organization_id}/proj.{project_id}")


class Client:
    _default_compression: Compression

    def __init__(
        self,
        pool: HTTPConnectionPool,
        default_compression: Compression,
        default_expiration_policy: str | None,
        usecase: str,
        scope: str,
        propagate_traces: bool,
        metrics_backend: MetricsBackend,
    ):
        self._pool = pool
        self._default_compression = default_compression
        self._default_expiration_policy = default_expiration_policy
        self._usecase = usecase
        self._scope = scope
        self._propagate_traces = propagate_traces
        self._metrics_backend = metrics_backend

    def _make_headers(self) -> dict[str, str]:
        if self._propagate_traces:
            return dict(sentry_sdk.get_current_scope().iter_trace_propagation_headers())
        return {}

    def _make_url(self, id: str | None, full: bool = False) -> str:
        base_path = f"/v1/{id}" if id else "/v1/"
        qs = urlencode({"usecase": self._usecase, "scope": self._scope})
        if full:
            return f"http://{self._pool.host}:{self._pool.port}{base_path}?{qs}"
        else:
            return f"{base_path}?{qs}"

    def put(
        self,
        contents: bytes | IO[bytes],
        id: str | None = None,
        compression: Compression | Literal["none"] | None = None,
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

        compression = compression or self._default_compression
        if compression == "zstd":
            cctx = zstandard.ZstdCompressor()
            body = cctx.stream_reader(original_body)
            headers["Content-Encoding"] = "zstd"

        if expiration_policy:
            headers[HEADER_EXPIRATION] = format_expiration(expiration_policy)
        elif self._default_expiration_policy:
            headers[HEADER_EXPIRATION] = self._default_expiration_policy

        if metadata:
            for k, v in metadata.items():
                headers[f"{HEADER_META_PREFIX}{k}"] = v

        with measure_storage_operation(
            self._metrics_backend, "put", self._usecase
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
        with measure_storage_operation(self._metrics_backend, "get", self._usecase):
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
        with measure_storage_operation(self._metrics_backend, "delete", self._usecase):
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
