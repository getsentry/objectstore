from __future__ import annotations

import math
import warnings
from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from io import BytesIO
from os import PathLike
from typing import IO, Any, Literal, NamedTuple, cast
from urllib.parse import urlparse

import urllib3
import zstandard
from urllib3.connectionpool import HTTPConnectionPool

from objectstore_client import _resumable, presign, utils
from objectstore_client.auth import Permission, SecretKey, TokenProvider
from objectstore_client.errors import (
    ExpiryExtensionRejected,
    ObjectNotFound,
    RequestError,
    raise_for_status,
)
from objectstore_client.metadata import (
    HEADER_EXPIRATION,
    HEADER_FILENAME,
    HEADER_META_PREFIX,
    HEADER_ORIGIN,
    Compression,
    ExpirationPolicy,
    Metadata,
    format_expiration,
    format_timedelta,
)
from objectstore_client.metrics import (
    MetricsBackend,
    NoOpMetricsBackend,
    measure_storage_operation,
)
from objectstore_client.scope import Scope
from objectstore_client.tracing import storage_span

# Query parameter carrying a JWT, mirroring the `x-os-auth` header.
PARAM_AUTH = "os_auth"

# Identifies this library to the server, mirroring the Rust client's user agent.
USER_AGENT = f"objectstore-client/{version('objectstore-client')}"


def _response_options_query(
    disposition: Literal["inline", "attachment"],
) -> str:
    """Validate and encode the response-content-disposition query parameter."""
    if disposition not in ("inline", "attachment"):
        raise ValueError(f"unsupported response content disposition {disposition!r}")
    return utils.encode_query(f"response-content-disposition={disposition}")


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

    Names must be 3–64 ASCII characters from ``[A-Za-z0-9_-]``. Any permitted
    character may appear in any position. Names are case-sensitive and are never
    normalized. The name is read-only after construction.
    """

    _name: str
    _compression: Compression
    _expiration_policy: ExpirationPolicy | None

    def __init__(
        self,
        name: str,
        compression: Compression = "zstd",
        expiration_policy: ExpirationPolicy | None = None,
        resumable_threshold_bytes: int | Literal["none"] = 256 * 1024 * 1024,
    ):
        """Create a usecase.

        Raises ``ValueError`` if the name violates the naming rules on ``Usecase``,
        or if the ``resumable_threshold_bytes`` is invalid.
        """
        if not 3 <= len(name) <= 64:
            raise ValueError("usecase name must be 3–64 ASCII characters long")
        if any(not (c.isascii() and c.isalnum()) and c not in "_-" for c in name):
            raise ValueError(
                "usecase name must contain only ASCII letters, digits, '_' or '-'"
            )
        self._name = name

        if resumable_threshold_bytes != "none" and (
            type(resumable_threshold_bytes) is not int or resumable_threshold_bytes < 0
        ):
            raise ValueError(
                'resumable_threshold_bytes must be a non-negative integer or "none"'
            )
        self._resumable_threshold_bytes = resumable_threshold_bytes
        self._compression = compression
        self._expiration_policy = expiration_policy

    @property
    def name(self) -> str:
        """The validated usecase name."""
        return self._name


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
            timeouts). By default, requests carry a ``User-Agent`` header of
            ``objectstore-client/{version}``; pass a ``headers`` mapping with a
            ``User-Agent`` key here to override it.
        token: A ``SecretKey`` that signs a fresh JWT for each request
            using an EdDSA keypair, or a static pre-signed JWT string used
            as-is for every request. Use a ``SecretKey`` for internal
            services that have access to the signing key, and a string for
            external services that receive a token from another source.
        spool_dir: Directory that temporary files are created in when preparing
            resumable uploads. ``None`` uses Python's default temporary directory.
    """

    def __init__(
        self,
        base_url: str,
        metrics_backend: MetricsBackend | None = None,
        retries: int | None = None,
        timeout_ms: float | None = None,
        connection_kwargs: Mapping[str, Any] | None = None,
        token: TokenProvider | None = None,
        spool_dir: str | PathLike[str] | None = None,
    ):
        connection_kwargs_to_use = asdict(_ConnectionDefaults())

        if retries is not None:
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

        headers = urllib3.HTTPHeaderDict({"User-Agent": USER_AGENT})
        headers.update(connection_kwargs_to_use.get("headers") or {})
        connection_kwargs_to_use["headers"] = headers

        self._pool = urllib3.connectionpool.connection_from_url(
            base_url, **connection_kwargs_to_use
        )
        self._base_path = urlparse(base_url).path
        self._metrics_backend = metrics_backend or NoOpMetricsBackend()
        self._token = token
        self._spool_dir = spool_dir

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
            usecase,
            Scope(**scopes),
            self._token,
            self._spool_dir,
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
        usecase: Usecase,
        scope: Scope,
        token: TokenProvider | None = None,
        spool_dir: str | PathLike[str] | None = None,
    ):
        self._pool = pool
        self._base_path = base_path
        self._metrics_backend = metrics_backend
        self._usecase = usecase
        self._scope = scope
        self._token = token
        self._spool_dir = spool_dir

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

        resolved_expiry = (
            expiry_seconds if expiry_seconds is not None else self._token.expiry_seconds
        )
        resolved_permissions = (
            permissions if permissions is not None else self._token.permissions
        )
        with storage_span(
            "mint_token",
            self._usecase,
            self._scope,
            expiry_seconds=resolved_expiry,
        ) as span:
            token = self._token.token_for_scope(
                self._usecase.name,
                self._scope,
                permissions,
                expiry_seconds,
            )
            span.set_attribute(
                "objectstore.permissions", [str(p) for p in resolved_permissions]
            )
            return token

    def _auth_token(self) -> str | None:
        """Returns a token for internal auth headers."""
        if isinstance(self._token, SecretKey):
            return self._token.token_for_scope(self._usecase.name, self._scope)
        elif isinstance(self._token, str):
            return self._token
        return None

    def _make_headers(self) -> dict[str, str]:
        headers = dict(self._pool.headers)
        if token := self._auth_token():
            headers["x-os-auth"] = f"Bearer {token}"
        return headers

    def _metadata_headers(
        self,
        compression: Compression | None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expiration_policy: ExpirationPolicy | None = None,
        origin: str | None = None,
        filename: str | None = None,
    ) -> dict[str, str]:
        """Build upload headers with the given compression and usecase expiration."""
        headers = self._make_headers()
        if compression and compression != "none":
            headers["Content-Encoding"] = compression
        if content_type:
            headers["Content-Type"] = content_type
        expiration_policy = expiration_policy or self._usecase._expiration_policy
        if expiration_policy:
            headers[HEADER_EXPIRATION] = format_expiration(expiration_policy)
        if origin:
            headers[HEADER_ORIGIN] = utils.encode_header_value(origin)
        if filename is not None:
            headers[HEADER_FILENAME] = utils.encode_header_value(filename)
        if metadata:
            for k, v in metadata.items():
                headers[f"{HEADER_META_PREFIX}{k}"] = utils.encode_header_value(v)
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

    def put(
        self,
        contents: bytes | IO[bytes],
        key: str | None = None,
        compress: Compression | None = None,
        compression: Compression | Literal["none"] | None = None,
        precompressed: Compression | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expiration_policy: ExpirationPolicy | None = None,
        origin: str | None = None,
        filename: str | None = None,
        resumable_threshold_bytes: int | Literal["none"] | None = None,
    ) -> str:
        """
        Uploads the given `contents` to blob storage.

        If no `key` is provided, one will be automatically generated and returned
        from this function.

        The client will select the Usecase's configured compression if none is given
        explicitly. This can be overridden by explicitly giving a `compress` argument.
        Providing `"none"` as the argument will instruct the client to not apply
        any compression to this upload, which is useful for uncompressible formats.

        If `contents` is already compressed, pass the algorithm as `precompressed`
        instead. The payload is then uploaded verbatim and the algorithm is recorded
        in the object's metadata, so downloads decompress it transparently.

        You can use the utility function `objectstore_client.utils.guess_mime_type`
        to attempt to guess a `content_type` based on magic bytes.

        `compression` is deprecated in favor of `compress`.

        .. rubric:: Resumable Uploads

        This method uses Objectstore's Resumable Uploads API when the size of
        ``contents`` is at least ``resumable_threshold_bytes``.
        Pass ``resumable_threshold_bytes="none"`` to disable Resumable Uploads.
        When not overridden, this threshold defaults to the one set on the Usecase.

        Transient failures and writes that make no progress are retried up to
        twice. The retry count resets whenever the server reports a higher offset.

        For seekable binary streams, the remaining size is measured by seeking to
        the end and restoring the original position. Prefer streams with efficient
        seeking: transforming streams may read their entire input to find the end.
        Non-seekable streams and streams whose size cannot be determined use a
        regular upload without spooling.

        Resumable uploads reuse the input unless compression is needed, in which
        case the compressed payload is spooled in a temporary file
        (``tempfile.TemporaryFile``) on disk before sending.
        Set ``Client(spool_dir=<path>)`` if you wish to customize the directory where
        the temporary files are created in.
        """
        if compression is not None:
            warnings.warn(
                "`compression` is deprecated, use `compress` instead",
                DeprecationWarning,
                stacklevel=2,
            )
            if compress is not None:
                raise ValueError("Cannot pass both `compress` and `compression`")
            compress = compression

        if compress is not None and precompressed is not None:
            raise ValueError("Cannot pass both `compress` and `precompressed`")
        if compress and compress not in ("none", "zstd"):
            raise ValueError(f"Invalid compression: {compress}")
        if precompressed and precompressed != "zstd":
            raise ValueError(f"Invalid compression: {precompressed}")

        if resumable_threshold_bytes is None:
            resumable_threshold_bytes = self._usecase._resumable_threshold_bytes
        if resumable_threshold_bytes != "none" and (
            type(resumable_threshold_bytes) is not int or resumable_threshold_bytes < 0
        ):
            raise ValueError(
                'resumable_threshold_bytes must be a non-negative integer or "none"'
            )

        source_size = None
        if resumable_threshold_bytes != "none":
            source_size = _resumable.get_size(contents)

        encoding = precompressed or compress or self._usecase._compression

        compress_with = encoding if precompressed is None else "none"

        headers = self._metadata_headers(
            compression=encoding,
            content_type=content_type,
            metadata=metadata,
            expiration_policy=expiration_policy,
            origin=origin,
            filename=filename,
        )

        if key == "":
            key = None

        with (
            storage_span("put", self._usecase, self._scope) as span,
            measure_storage_operation(
                self._metrics_backend, "put", self._usecase.name
            ) as metrics,
        ):
            result_key: str | None
            if (
                resumable_threshold_bytes != "none"
                and source_size is not None
                and source_size >= resumable_threshold_bytes
            ):
                with _resumable.prepare_body(
                    contents,
                    source_size,
                    compress_with != "none",
                    self._spool_dir,
                ) as prepared:
                    if prepared is None:
                        # Temporary-file creation failed before consuming the input.
                        result_key, source_size, compressed_size = self._put_direct(
                            contents, key, headers, compress=compress_with != "none"
                        )
                    else:
                        body, source_size, compressed_size = prepared
                        result_key = _resumable.upload(
                            self,
                            body,
                            compressed_size,
                            key=key,
                            compression=encoding,
                            content_type=content_type,
                            metadata=metadata,
                            expiration_policy=expiration_policy,
                            origin=origin,
                            filename=filename,
                        )
                        if result_key is None:
                            # Session creation failed or resumables are unsupported.
                            # Send the prepared body without compressing it again.
                            headers["Content-Length"] = str(compressed_size)
                            result_key, _, _ = self._put_direct(
                                body, key, headers, compress=False
                            )
            else:
                result_key, source_size, compressed_size = self._put_direct(
                    contents, key, headers, compress=compress_with != "none"
                )

            if precompressed is None and source_size is not None:
                metrics.record_uncompressed_size(source_size)
            if encoding != "none" and compressed_size is not None:
                metrics.record_compressed_size(compressed_size, encoding)

            # Set after the response, since the key may be server-generated.
            span.set_attribute("objectstore.key", result_key)
            span.set_attribute("objectstore.compression", encoding)
            if metrics.uncompressed_size is not None:
                span.set_attribute(
                    "objectstore.uncompressed_size", metrics.uncompressed_size
                )
            if metrics.compressed_size is not None:
                span.set_attribute(
                    "objectstore.compressed_size", metrics.compressed_size
                )
            return result_key

    def _put_direct(
        self,
        contents: bytes | IO[bytes],
        key: str | None,
        headers: dict[str, str],
        compress: bool,
    ) -> tuple[str, int | None, int | None]:
        """Return the key and final source/body positions when available."""
        with (
            BytesIO(contents) if isinstance(contents, bytes) else nullcontext(contents)
        ) as original_body:
            body = original_body
            retries = None  # by default use the pool's value, set by the Client
            try:
                if compress:
                    cctx = zstandard.ZstdCompressor()
                    body = cctx.stream_reader(original_body, closefd=False)
                    body = cast(IO[bytes], utils._ZstdCompressionReaderWrapper(body))

                if compress:
                    # For on-the-fly compression, don't attempt read retries,
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

                # Read positions after streaming, without failing an upload if
                # the source does not support tell().
                try:
                    source_size = original_body.tell()
                except (AttributeError, OSError, ValueError):
                    source_size = None
                try:
                    compressed_size = body.tell()
                except (AttributeError, OSError, ValueError):
                    compressed_size = None
                return res["key"], source_size, compressed_size
            finally:
                if body is not original_body:
                    body.close()

    def get(
        self,
        key: str,
        decompress: bool = True,
        accept_encoding: Sequence[str] | None = None,
    ) -> GetResponse | None:
        """
        This fetches the blob with the given `key`, returning an `IO` stream that
        can be read.

        Returns ``None`` if the object does not exist.

        By default, content that was uploaded compressed will be automatically
        decompressed, unless `decompress=False` is passed.

        If `accept_encoding` is provided, any compression algorithm listed there
        will be passed through compressed instead of decompressed, even when
        `decompress=True`. For example, passing `accept_encoding=["zstd"]` returns
        the raw zstd-compressed bytes when the stored object is zstd-compressed.
        """

        headers = self._make_headers()
        with storage_span("get", self._usecase, self._scope, key=key) as span:
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
                if response.status == 404:
                    response.read()  # drain body so urllib3 returns the connection
                    span.set_attribute("objectstore.found", False)
                    return None
                raise_for_status(response)
                span.set_attribute("objectstore.found", True)

            # OR: should I use `response.stream()`?
            stream = cast(IO[bytes], response)
            metadata = Metadata.from_headers(response.headers)
            span.set_attribute(
                "objectstore.compression", metadata.compression or "none"
            )

            encoding_accepted = accept_encoding is not None and (
                "*" in accept_encoding or metadata.compression in accept_encoding
            )
            decompressed = False
            if metadata.compression and decompress and not encoding_accepted:
                if metadata.compression != "zstd":
                    raise NotImplementedError(
                        "Transparent decoding of anything but `zstd` is not "
                        "implemented yet"
                    )

                metadata.compression = None
                dctx = zstandard.ZstdDecompressor()
                stream = dctx.stream_reader(stream, read_across_frames=True)
                decompressed = True

            span.set_attribute("objectstore.decompressed", decompressed)
            return GetResponse(metadata, stream)

    def object_url(
        self,
        key: str,
        token_validity: timedelta | None = None,
        response_content_disposition: Literal["inline", "attachment"] | None = None,
    ) -> str:
        """
        Generates a GET url to the object with the given `key`.

        This can then be used by downstream services to fetch the given object.
        NOTE however that the service does not strictly follow HTTP semantics,
        in particular in relation to `Accept-Encoding`.

        When ``token_validity`` is provided, read-only authorization
        information is embedded in the returned URL's query string, valid
        for the given duration.

        ``response_content_disposition`` selects ``attachment`` (default) or ``inline``
        for successful GET/HEAD responses, retaining the stored filename.

        These options are not bound by the auth token and can be changed by recipients.
        Use :meth:`presigned_object_url` to bind the options to a signature.

        Raises ``ValueError`` if ``token_validity`` is provided but no
        ``SecretKey`` is configured on this session, or if disposition is invalid.
        """
        url = self._make_url(key, full=True)
        if response_content_disposition is not None:
            url = f"{url}?{_response_options_query(response_content_disposition)}"
        if token_validity is None:
            return url

        token = self.mint_token(
            permissions=[Permission.OBJECT_READ],
            expiry_seconds=math.ceil(token_validity.total_seconds()),
        )
        # A JWT's compact serialization is base64url, whose alphabet is URL-safe,
        # so it can go in the query string as-is with no further encoding.
        separator = "&" if response_content_disposition is not None else "?"
        return f"{url}{separator}{PARAM_AUTH}={token}"

    def presigned_object_url(
        self,
        method: Literal["GET", "HEAD"],
        key: str,
        duration: timedelta = timedelta(hours=1),
        response_content_disposition: Literal["inline", "attachment"] | None = None,
    ) -> str:
        """
        Generates a pre-signed URL authorizing a single ``method`` request on the
        object with the given ``key``, valid for ``duration``.

        .. warning::
            Experimental: pre-signed URLs are an experimental feature and this
            API may change in a future release.

        Raises ``ValueError`` if no ``SecretKey`` is configured on this
        session's client, if ``method`` is not supported, or if ``duration``
        is above the one-week maximum, or if disposition is invalid.

        The returned URL carries a signature that allows the recipient to perform
        a request without an auth token. It can be handed to any HTTP client;
        the URL is already percent-encoded, and must be transmitted verbatim.

        ``response_content_disposition`` selects ``attachment`` (default) or ``inline``
        for successful GET/HEAD responses, retaining the stored filename.

        Response options are included in the signature when supplied.

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

        with storage_span(
            "presign",
            self._usecase,
            self._scope,
            key=key,
            method=method,
            duration_seconds=duration_secs,
        ):
            # `_make_url` already percent-encodes the path identically to the wire.
            encoded_path = self._make_url(key)
            timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            encoded_query = utils.encode_query(
                f"{presign.PARAM_KID}={self._token.kid}"
                f"&{presign.PARAM_TIMESTAMP}={timestamp}"
                f"&{presign.PARAM_DURATION}={duration_secs}"
            )

            if response_content_disposition is not None:
                encoded_query += (
                    f"&{_response_options_query(response_content_disposition)}"
                )

            canonical = presign.build_canonical_form(
                method, encoded_path, encoded_query
            )
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
        with (
            storage_span("head", self._usecase, self._scope, key=key) as span,
            measure_storage_operation(
                self._metrics_backend, "head", self._usecase.name
            ),
        ):
            response = self._pool.request(
                "HEAD",
                self._make_url(key),
                headers=headers,
                preload_content=True,
            )
            if response.status == 404:
                span.set_attribute("objectstore.found", False)
                return None
            raise_for_status(response)
            span.set_attribute("objectstore.found", True)
            return Metadata.from_headers(response.headers)

    def extend_expiry(
        self,
        key: str,
        at: datetime | None = None,
        from_creation: timedelta | None = None,
        from_now: timedelta | None = None,
    ) -> None:
        """Extend an object's expiration deadline, preserving its payload.

        Supply exactly one target: ``at`` is a timezone-aware absolute datetime;
        ``from_creation`` is total lifetime since creation or replacement; and
        ``from_now`` is lifetime from server request start. Relative targets are
        resolved by the server, not added to the existing deadline. Fractional
        duration seconds are truncated; absolute deadlines round upward to seconds.

        An already-sufficient deadline succeeds without being shortened. Success
        returns no value and does not indicate whether the deadline changed.
        Requires object-write permission.

        Actual extensions adjust TTL duration to approximately match the total
        lifetime since creation; TTI duration remains unchanged.

        Raises ``ValueError`` for missing or multiple targets, naive datetimes,
        or negative durations. Zero durations are valid. Objects observed absent
        or expired raise ``ObjectNotFound``. Rejected extensions raise
        ``ExpiryExtensionRejected`` for non-expiring or concurrently changed
        objects, or missing creation metadata needed for a creation-relative
        target. Both exceptions subclass ``RequestError``. Rejection does not
        guarantee that the object still exists, since it may be deleted concurrently.
        Other HTTP errors propagate normally. Retrying ``from_now`` establishes
        a new server-time anchor and can extend the deadline further.

        Example::

            from objectstore_client import ExpiryExtensionRejected, ObjectNotFound

            try:
                session.extend_expiry(key, from_now=timedelta(days=30))
            except ObjectNotFound:
                print("Object is missing or expired")
            except ExpiryExtensionRejected:
                print("Extension was rejected")
        """

        if sum(value is not None for value in (at, from_creation, from_now)) != 1:
            raise ValueError("Supply exactly one of at, from_creation, or from_now")

        extension: dict[str, str]
        if at is not None:
            if at.utcoffset() is None:
                raise ValueError("at must be a timezone-aware datetime")
            extension = {"at": at.astimezone(UTC).isoformat()}
        else:
            delta = from_creation if from_creation is not None else from_now
            assert delta is not None
            if delta < timedelta(0):
                raise ValueError("Expiry extension duration must not be negative")
            extension = {
                "after": format_timedelta(delta),
                "from": "creation" if from_creation is not None else "now",
            }

        headers = self._make_headers()
        with (
            storage_span("extend_expiry", self._usecase, self._scope, key=key),
            measure_storage_operation(
                self._metrics_backend, "extend_expiry", self._usecase.name
            ),
        ):
            response = self._pool.request(
                "PATCH",
                self._make_url(key),
                headers=headers,
                json={"extend_expiry": extension},
                preload_content=True,
            )
            error_type: type[RequestError] = RequestError
            if response.status == 404:
                error_type = ObjectNotFound
            elif response.status == 409:
                error_type = ExpiryExtensionRejected
            raise_for_status(response, error_type=error_type)

    def delete(self, key: str) -> None:
        """
        Deletes the blob with the given `key`.
        """

        headers = self._make_headers()
        with (
            storage_span("delete", self._usecase, self._scope, key=key),
            measure_storage_operation(
                self._metrics_backend, "delete", self._usecase.name
            ),
        ):
            response = self._pool.request(
                "DELETE",
                self._make_url(key),
                headers=headers,
            )
            raise_for_status(response)

    def _create_upload(
        self,
        object_length: int,
        key: str | None = None,
        compression: Compression | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expiration_policy: ExpirationPolicy | None = None,
        origin: str | None = None,
        filename: str | None = None,
    ) -> _resumable.ResumableUpload | None:
        """Create a resumable upload, or return None if the server declines.

        `object_length` is the total length of the object to be uploaded.
        Unlike regular uploads, the client does not automatically compress chunk
        contents.
        The caller must pre-compress the object according to `compression`, then
        create the upload with the post-compression `object_length`. Offsets
        similarly refer to the bytes after compression.
        """
        if object_length < 0:
            raise ValueError("Object length must not be negative")
        if compression is not None and compression not in ("none", "zstd"):
            raise ValueError(f"Invalid compression: {compression}")

        headers = self._metadata_headers(
            compression=compression,
            content_type=content_type,
            metadata=metadata,
            expiration_policy=expiration_policy,
            origin=origin,
            filename=filename,
        )
        headers["Upload-Length"] = str(object_length)

        key = key or None
        with (
            storage_span(
                "resumable.create", self._usecase, self._scope, key=key
            ) as span,
            measure_storage_operation(
                self._metrics_backend, "resumable.create", self._usecase.name
            ),
        ):
            response = self._pool.request(
                "PUT" if key else "POST",
                f"{self._make_url(key)}?upload_type=resumable",
                headers=headers,
                preload_content=True,
                decode_content=True,
            )
            if response.status == 501:
                return None
            raise_for_status(response)
            res = response.json()
            span.set_attribute("objectstore.key", res["key"])
            return _resumable.ResumableUpload(
                self,
                res["key"],
                res["session"],
                object_length,
                res.get("granularity", 0),
            )

    def _resume_upload(self, key: str, token: str) -> _resumable.ResumableUpload:
        """Reconstruct a resumable upload handle, without contacting the server."""
        return _resumable.ResumableUpload(self, key, token)
