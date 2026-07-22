from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import IO, Any, Literal, NamedTuple, cast
from urllib.parse import urlparse

import sentry_sdk
import urllib3
import zstandard
from urllib3.connectionpool import HTTPConnectionPool

from objectstore_client import presign, utils
from objectstore_client.auth import Permission, SecretKey, TokenProvider
from objectstore_client.errors import raise_for_status
from objectstore_client.metadata import (
    HEADER_EXPIRATION,
    HEADER_FILENAME,
    HEADER_META_PREFIX,
    HEADER_ORIGIN,
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
from objectstore_client.multipart import MultipartUpload
from objectstore_client.scope import Scope

# Query parameter carrying a JWT, mirroring the `x-os-auth` header.
PARAM_AUTH = "os_auth"


class GetResponse(NamedTuple):
    metadata: Metadata
    payload: IO[bytes]


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
    """
    A client for Objectstore. Constructing it initializes a connection pool.

    Args:
        base_url: The base URL of the Objectstore server (e.g.,
            "http://objectstore:8888"). metrics_backend: Optional metrics backend for
            tracking storage operations. Defaults to ``NoOpMetricsBackend`` if not
            provided.
        propagate_traces: Whether to propagate Sentry trace headers in requests to
            objectstore. Defaults to ``False``.
        retries: Number of connection retries for failed requests.
            Defaults to ``3`` if not specified. **Note:** only connection failures are
            retried, not read failures (as compression streams cannot be rewound).
        timeout_ms: Read timeout in milliseconds for API requests. The read timeout
            is the maximum time to wait between consecutive read operations on the
            socket (i.e., between receiving chunks of data). Defaults to no read timeout
            if not specified. The connection timeout is always 100ms. To override the
            connection timeout, pass a custom ``urllib3.Timeout`` object via
            ``connection_kwargs``. For example:

            .. code-block:: python

                client = Client(
                    "http://objectstore:8888", connection_kwargs={
                        "timeout": urllib3.Timeout(connect=1.0, read=5.0)
                    }
                )

        connection_kwargs: Additional keyword arguments to pass to the underlying
            urllib3 connection pool (e.g., custom headers, SSL settings, advanced
            timeouts).
        token: A ``SecretKey`` that signs a fresh JWT for each request
            using an EdDSA keypair, or a static pre-signed JWT string used
            as-is for every request. Use a ``SecretKey`` for internal
            services that have access to the signing key, and a string for
            external services that receive a token from another source.
    """

    def __init__(
        self,
        base_url: str,
        metrics_backend: MetricsBackend | None = None,
        propagate_traces: bool = False,
        retries: int | None = None,
        timeout_ms: float | None = None,
        connection_kwargs: Mapping[str, Any] | None = None,
        token: TokenProvider | None = None,
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
        self._token = token

    def session(self, usecase: Usecase, **scopes: str | int | bool) -> Session:
        """
        Create a [Session] with the Objectstore server, tied to a specific [Usecase] and
        [Scope].

        A Scope is a (possibly nested) namespace within a Usecase, given as a sequence
        of key-value pairs passed as kwargs.
        IMPORTANT: the order of the kwargs matters!

        The admitted characters for keys and values are: `A-Za-z0-9_-()$!+'`.

        Users are free to choose the scope structure that best suits their Usecase.
        The combination of Usecase and Scope will determine the physical key/path of the
        blob in the underlying storage backend.

        For most usecases, it's recommended to use the organization and project ID as
        the first components of the scope, as follows:
        ```
        client.session(usecase, org=organization_id, project=project_id, ...)
        ```

        Args:
            usecase: The Usecase to scope this session to.
            **scopes: Key-value pairs defining the scope within the usecase.
        """

        return Session(
            self._pool,
            self._base_path,
            self._metrics_backend,
            self._propagate_traces,
            usecase,
            Scope(**scopes),
            self._token,
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
        token: TokenProvider | None = None,
    ):
        self._pool = pool
        self._base_path = base_path
        self._metrics_backend = metrics_backend
        self._propagate_traces = propagate_traces
        self._usecase = usecase
        self._scope = scope
        self._token = token

    def mint_token(
        self,
        permissions: list[Permission] | None = None,
        expiry_seconds: int | None = None,
    ) -> str:
        """
        Returns a signed token.

        When ``permissions`` is ``None``, the generator's default
        permissions are used. When provided, they must be a subset of
        the generator's permissions.

        When ``expiry_seconds`` is ``None``, the generator's default
        expiry is used.

        Raises ``ValueError`` if no ``SecretKey`` is configured
        or if any requested permission is not granted to the key.
        """
        if not isinstance(self._token, SecretKey):
            raise ValueError("no secret key configured on this session")
        return self._token.token_for_scope(
            self._usecase.name,
            self._scope,
            permissions,
            expiry_seconds,
        )

    def _auth_token(self) -> str | None:
        """Returns a token for internal auth headers."""
        if isinstance(self._token, SecretKey):
            return self._token.token_for_scope(self._usecase.name, self._scope)
        elif isinstance(self._token, str):
            return self._token
        return None

    def _make_headers(self) -> dict[str, str]:
        headers = dict(self._pool.headers)
        if self._propagate_traces:
            headers.update(
                dict(sentry_sdk.get_current_scope().iter_trace_propagation_headers())
            )
        if token := self._auth_token():
            headers["x-os-auth"] = f"Bearer {token}"
        return headers

    def _base_url(self) -> str:
        # urllib3 stores IPv6 hosts unbracketed (e.g. "::1"); bracket them so
        # the result is a valid absolute URL.
        host = self._pool.host
        if ":" in host:
            host = f"[{host}]"
        return f"{self._pool.scheme}://{host}:{self._pool.port}"

    def _make_url(self, key: str | None, full: bool = False) -> str:
        relative_path = f"/v1/objects/{self._usecase.name}/{self._scope}/{key or ''}"
        path = utils.encode_path(self._base_path.rstrip("/") + relative_path)
        if full:
            return f"{self._base_url()}{path}"
        return path

    def _make_multipart_url(
        self,
        action: str | None,
        key: str | None,
        query: str | None = None,
    ) -> str:
        if action == "parts":
            resource = "objects:multipart:parts"
        elif action == "complete":
            resource = "objects:multipart:complete"
        else:
            resource = "objects:multipart"

        relative_path = f"/v1/{resource}/{self._usecase.name}/{self._scope}/{key or ''}"
        path = utils.encode_path(self._base_path.rstrip("/") + relative_path)
        if query:
            return f"{path}?{query}"
        return path

    def put(
        self,
        contents: bytes | IO[bytes],
        key: str | None = None,
        compression: Compression | Literal["none"] | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expiration_policy: ExpirationPolicy | None = None,
        origin: str | None = None,
        filename: str | None = None,
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
        if compression and compression not in ("none", "zstd"):
            raise ValueError(f"Invalid compression: {compression}")

        headers = self._make_headers()
        body = BytesIO(contents) if isinstance(contents, bytes) else contents
        original_body: IO[bytes] = body

        compression = compression or self._usecase._compression
        if compression == "zstd":
            cctx = zstandard.ZstdCompressor()
            body = cctx.stream_reader(original_body)
            body = cast(IO[bytes], utils._ZstdCompressionReaderWrapper(body))
            headers["Content-Encoding"] = "zstd"

        if content_type:
            headers["Content-Type"] = content_type

        expiration_policy = expiration_policy or self._usecase._expiration_policy
        if expiration_policy:
            headers[HEADER_EXPIRATION] = format_expiration(expiration_policy)

        if origin:
            headers[HEADER_ORIGIN] = origin

        if filename is not None:
            headers[HEADER_FILENAME] = filename

        if metadata:
            for k, v in metadata.items():
                headers[f"{HEADER_META_PREFIX}{k}"] = v

        if key == "":
            key = None

        with measure_storage_operation(
            self._metrics_backend, "put", self._usecase.name
        ) as metric_emitter:
            retries = None  # by default use the pool's value, set by the Client
            if compression == "zstd":
                # For compressed bodies, don't attempt read retries,
                # as the stream cannot be rewound after data has been consumed.
                pool_retries = self._pool.retries
                if isinstance(pool_retries, urllib3.Retry):
                    retries = pool_retries.new(read=0)
                elif isinstance(pool_retries, int):
                    retries = urllib3.Retry(pool_retries, read=0)

            response = self._pool.request(
                "POST" if not key else "PUT",
                self._make_url(key),
                body=body,
                headers=headers,
                preload_content=True,
                decode_content=True,
                retries=retries,
            )
            raise_for_status(response)
            res = response.json()

            # Must do this after streaming `body` as that's what is responsible
            # for advancing the seek position in both streams
            metric_emitter.record_uncompressed_size(original_body.tell())
            if compression and compression != "none":
                metric_emitter.record_compressed_size(body.tell(), compression)
            return res["key"]

    def get(
        self,
        key: str,
        decompress: bool = True,
        accept_encoding: Sequence[str] | None = None,
    ) -> GetResponse:
        """
        This fetches the blob with the given `key`, returning an `IO` stream that
        can be read.

        By default, content that was uploaded compressed will be automatically
        decompressed, unless `decompress=False` is passed.

        If `accept_encoding` is provided, any compression algorithm listed there
        will be passed through compressed instead of decompressed, even when
        `decompress=True`. For example, passing `accept_encoding=["zstd"]` returns
        the raw zstd-compressed bytes when the stored object is zstd-compressed.
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

        encoding_accepted = accept_encoding is not None and (
            "*" in accept_encoding or metadata.compression in accept_encoding
        )
        if metadata.compression and decompress and not encoding_accepted:
            if metadata.compression != "zstd":
                raise NotImplementedError(
                    "Transparent decoding of anything but `zstd` is not implemented yet"
                )

            metadata.compression = None
            dctx = zstandard.ZstdDecompressor()
            stream = dctx.stream_reader(stream, read_across_frames=True)

        return GetResponse(metadata, stream)

    def object_url(self, key: str, token_validity: timedelta | None = None) -> str:
        """
        Generates a GET url to the object with the given `key`.

        This can then be used by downstream services to fetch the given object.
        NOTE however that the service does not strictly follow HTTP semantics,
        in particular in relation to `Accept-Encoding`.

        When ``token_validity`` is provided, read-only authorization
        information is embedded in the returned URL's query string, valid
        for the given duration.

        Raises ``ValueError`` if ``token_validity`` is provided but no
        ``SecretKey`` is configured on this session.
        """
        url = self._make_url(key, full=True)
        if token_validity is None:
            return url

        token = self.mint_token(
            permissions=[Permission.OBJECT_READ],
            expiry_seconds=math.ceil(token_validity.total_seconds()),
        )
        # A JWT's compact serialization is base64url, whose alphabet is URL-safe,
        # so it can go in the query string as-is with no further encoding.
        return f"{url}?{PARAM_AUTH}={token}"

    def presigned_object_url(
        self,
        method: Literal["GET", "HEAD"],
        key: str,
        duration: timedelta = timedelta(hours=1),
    ) -> str:
        """
        Generates a pre-signed URL authorizing a single ``method`` request on the
        object with the given ``key``, valid for ``duration``.

        .. warning::
            Experimental: pre-signed URLs are an experimental feature and this
            API may change in a future release.

        Raises ``ValueError`` if no ``SecretKey`` is configured on this
        session's client, if ``method`` is not supported, or if ``duration``
        is above the one-week maximum.

        The returned URL carries a signature that allows the recipient to perform
        a request without an auth token. It can be handed to any HTTP client;
        the URL is already percent-encoded, and must be transmitted verbatim.

        Note that `HEAD` and `GET` are considered equivalent from the point of view
        of signatures, meaning that a signature for `GET` can also be used for
        `HEAD`, and viceversa.
        """
        if method not in presign.SUPPORTED_METHODS:
            raise ValueError(
                f"unsupported pre-signed method {method!r}, "
                f"expected one of {presign.SUPPORTED_METHODS}"
            )

        if not isinstance(self._token, SecretKey):
            raise ValueError("no secret key configured on this session")

        if duration > presign.MAX_PRESIGN_DURATION:
            raise ValueError(
                f"duration {duration} exceeds the maximum of "
                f"{presign.MAX_PRESIGN_DURATION}"
            )
        duration_secs = math.ceil(duration.total_seconds())

        # `_make_url` already percent-encodes the path identically to the wire.
        encoded_path = self._make_url(key)
        timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        encoded_query = utils.encode_query(
            f"{presign.PARAM_KID}={self._token.kid}"
            f"&{presign.PARAM_TIMESTAMP}={timestamp}"
            f"&{presign.PARAM_DURATION}={duration_secs}"
        )

        canonical = presign.build_canonical_form(method, encoded_path, encoded_query)
        signature = self._token.signature_for_canonical_form(canonical)

        return (
            f"{self._base_url()}"
            f"{encoded_path}?{encoded_query}&{presign.PARAM_SIG}={signature}"
        )

    def head(self, key: str) -> Metadata | None:
        """
        Checks whether an object exists and retrieves its metadata.

        Returns the object's ``Metadata`` if it exists, ``None`` otherwise.

        If the object has a TTI expiration policy, this is considered an access,
        and therefore bumps its expiration.
        """
        headers = self._make_headers()
        with measure_storage_operation(
            self._metrics_backend, "head", self._usecase.name
        ):
            response = self._pool.request(
                "HEAD",
                self._make_url(key),
                headers=headers,
            )
            if response.status == 404:
                return None
            raise_for_status(response)
            return Metadata.from_headers(response.headers)

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

    def initiate_multipart_upload(
        self,
        *,
        key: str | None = None,
        compression: Compression | Literal["none"] | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expiration_policy: ExpirationPolicy | None = None,
        origin: str | None = None,
        filename: str | None = None,
    ) -> MultipartUpload:
        """
        Initiates a multipart upload.

        Returns a :class:`~objectstore_client.multipart.MultipartUpload` handle
        that can be used to upload parts, list parts, complete, or abort.

        **Important:** unlike :meth:`put`, the ``compression`` parameter only
        records the compression algorithm in the object's metadata.
        The caller is responsible for compressing each part in accordance with the
        chosen algorithm before passing it to
        :meth:`~objectstore_client.multipart.MultipartUpload.upload_part`.
        """
        if compression and compression not in ("none", "zstd"):
            raise ValueError(f"Invalid compression: {compression}")

        headers = self._make_headers()

        compression = compression or self._usecase._compression
        if compression and compression != "none":
            headers["Content-Encoding"] = compression

        if content_type:
            headers["Content-Type"] = content_type

        expiration_policy = expiration_policy or self._usecase._expiration_policy
        if expiration_policy:
            headers[HEADER_EXPIRATION] = format_expiration(expiration_policy)

        if origin:
            headers[HEADER_ORIGIN] = origin

        if filename is not None:
            headers[HEADER_FILENAME] = filename

        if metadata:
            for k, v in metadata.items():
                headers[f"{HEADER_META_PREFIX}{k}"] = v

        if key == "":
            key = None

        with measure_storage_operation(
            self._metrics_backend, "multipart.initiate", self._usecase.name
        ):
            response = self._pool.request(
                "POST" if not key else "PUT",
                self._make_multipart_url(None, key),
                headers=headers,
                preload_content=True,
                decode_content=True,
            )
            raise_for_status(response)
            res = response.json()
            return MultipartUpload(self, res["key"], res["upload_id"])

    def resume_multipart_upload(self, key: str, upload_id: str) -> MultipartUpload:
        """
        Reconstructs a multipart upload handle.

        This does not make any network calls.
        Use it to resume an upload after a process restart or to
        continue an upload started elsewhere.
        """
        return MultipartUpload(self, key, upload_id)
