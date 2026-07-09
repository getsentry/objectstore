"""
Batch (bulk) operations for the Objectstore client.

A :class:`ManyBuilder` lets you enqueue multiple get/put/delete/head operations
and dispatch them efficiently. Operations that fit within the per-part size
limit are grouped into multipart batch requests sent to the dedicated
``objects:batch`` endpoint; operations too large to batch (oversized inserts)
fall back to individual requests. Both kinds run concurrently using thread
pools whose sizes are configurable.

Each request part carries ``x-sn-batch-operation-kind`` and (except for keyless
inserts) ``x-sn-batch-operation-key`` headers, and the server echoes an
``x-sn-batch-operation-index`` plus ``x-sn-batch-operation-status`` on each
response part so results can be correlated back to their operations.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from io import BytesIO
from typing import IO, TYPE_CHECKING, Literal
from urllib.parse import quote, unquote

import zstandard
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata

from objectstore_client.client import GetResponse
from objectstore_client.errors import RequestError, raise_for_status
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

if TYPE_CHECKING:
    from objectstore_client.client import Session

# Maximum number of operations to send in a single batch request.
MAX_BATCH_OPS = 1000

# Maximum (post-compression, estimated) size of a single part's body in a batch
# request. Inserts larger than this are sent as individual requests instead.
MAX_BATCH_PART_SIZE = 1024 * 1024  # 1 MB

# Maximum total (estimated) body size to include in a single batch request.
MAX_BATCH_BODY_SIZE = 100 * 1024 * 1024  # 100 MB

# Default maximum number of concurrent individual (non-batch) requests.
DEFAULT_INDIVIDUAL_CONCURRENCY = 5

# Default maximum number of concurrent batch requests.
DEFAULT_BATCH_CONCURRENCY = 3

HEADER_BATCH_OPERATION_KIND = "x-sn-batch-operation-kind"
HEADER_BATCH_OPERATION_KEY = "x-sn-batch-operation-key"
HEADER_BATCH_OPERATION_INDEX = "x-sn-batch-operation-index"
HEADER_BATCH_OPERATION_STATUS = "x-sn-batch-operation-status"


@dataclass
class _GetOp:
    key: str
    decompress: bool
    accept_encoding: Sequence[str] | None


@dataclass
class _InsertOp:
    body: bytes | IO[bytes]
    key: str | None
    compression: Compression | None
    content_type: str | None
    metadata: dict[str, str] | None
    expiration_policy: ExpirationPolicy | None
    origin: str | None
    filename: str | None


@dataclass
class _DeleteOp:
    key: str


@dataclass
class _HeadOp:
    key: str


_Operation = _GetOp | _InsertOp | _DeleteOp | _HeadOp


@dataclass
class GetResult:
    """Result of a ``get`` operation in a batch."""

    key: str
    response: GetResponse | None
    """The fetched object, or ``None`` if it was not found (404)."""
    error: Exception | None
    """The error that occurred, or ``None`` if the operation succeeded."""


@dataclass
class PutResult:
    """Result of a ``put`` operation in a batch."""

    key: str
    error: Exception | None
    """The error that occurred, or ``None`` if the operation succeeded."""


@dataclass
class DeleteResult:
    """Result of a ``delete`` operation in a batch."""

    key: str
    error: Exception | None
    """The error that occurred, or ``None`` if the operation succeeded."""


@dataclass
class HeadResult:
    """Result of a ``head`` operation in a batch."""

    key: str
    metadata: Metadata | None
    """The object metadata, or ``None`` if it was not found (404)."""
    error: Exception | None
    """The error that occurred, or ``None`` if the operation succeeded."""


@dataclass
class ErrorResult:
    """
    An error that could not be attributed to a specific operation.

    This can happen when a response part has a missing or malformed index or
    status header, references an unknown operation index, or when the batch
    request itself fails before the server can respond per-operation.
    """

    error: Exception


OperationResult = GetResult | PutResult | DeleteResult | HeadResult | ErrorResult


class OperationResults(list["OperationResult"]):
    """
    The results of a :meth:`ManyBuilder.send` call.

    This is a ``list`` of :data:`OperationResult`\\ s. The order is **not**
    guaranteed to match the order in which operations were enqueued, since
    operations execute concurrently.
    """

    def failures(self) -> list[OperationResult]:
        """Returns the subset of results that carry an error."""
        return [r for r in self if r.error is not None]

    def raise_for_failures(self) -> None:
        """
        Raises an :class:`ExceptionGroup` of all per-operation errors, if any.

        Does nothing when every operation succeeded.
        """
        errors = [r.error for r in self if r.error is not None]
        if errors:
            raise ExceptionGroup("one or more batch operations failed", errors)


class ManyBuilder:
    """
    Enqueues multiple operations to be executed together.

    Construct one via :meth:`~objectstore_client.client.Session.many`, enqueue
    operations with :meth:`get`/:meth:`put`/:meth:`delete`/:meth:`head`, then
    call :meth:`send` to execute them.
    """

    def __init__(self, session: Session):
        self._session = session
        self._ops: list[_Operation] = []
        self._max_individual_concurrency: int | None = None
        self._max_batch_concurrency: int | None = None

    def get(
        self,
        key: str,
        *,
        decompress: bool = True,
        accept_encoding: Sequence[str] | None = None,
    ) -> ManyBuilder:
        """Enqueues a ``get`` for ``key``. See :meth:`Session.get`."""
        self._ops.append(_GetOp(key, decompress, accept_encoding))
        return self

    def put(
        self,
        contents: bytes | IO[bytes],
        *,
        key: str | None = None,
        compression: Compression | Literal["none"] | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expiration_policy: ExpirationPolicy | None = None,
        origin: str | None = None,
        filename: str | None = None,
    ) -> ManyBuilder:
        """Enqueues a ``put`` of ``contents``. See :meth:`Session.put`."""
        if compression and compression not in ("none", "zstd"):
            raise ValueError(f"Invalid compression: {compression}")
        self._ops.append(
            _InsertOp(
                body=contents,
                key=key or None,
                compression=compression,
                content_type=content_type,
                metadata=metadata,
                expiration_policy=expiration_policy,
                origin=origin,
                filename=filename,
            )
        )
        return self

    def delete(self, key: str) -> ManyBuilder:
        """Enqueues a ``delete`` for ``key``. See :meth:`Session.delete`."""
        self._ops.append(_DeleteOp(key))
        return self

    def head(self, key: str) -> ManyBuilder:
        """Enqueues a ``head`` for ``key``. See :meth:`Session.head`."""
        self._ops.append(_HeadOp(key))
        return self

    def max_individual_concurrency(self, concurrency: int) -> ManyBuilder:
        """
        Sets the maximum number of concurrent individual (non-batch) requests.

        Operations that exceed the per-part size limit are sent individually.
        Defaults to ``5``.
        """
        self._max_individual_concurrency = concurrency
        return self

    def max_batch_concurrency(self, concurrency: int) -> ManyBuilder:
        """
        Sets the maximum number of concurrent batch requests.

        Batchable operations are grouped into chunks sent as multipart batch
        requests. Defaults to ``3``.
        """
        self._max_batch_concurrency = concurrency
        return self

    def send(self) -> OperationResults:
        """
        Executes all enqueued operations and returns their results.

        Batchable operations are chunked and sent as batch requests; oversized
        inserts are sent individually. Both run concurrently. The results are
        **not** guaranteed to be in the order the operations were enqueued.
        """
        individual_concurrency = self._max_individual_concurrency or DEFAULT_INDIVIDUAL_CONCURRENCY
        batch_concurrency = self._max_batch_concurrency or DEFAULT_BATCH_CONCURRENCY

        batchable, individual = self._partition()
        chunks = list(_iter_batches(batchable))

        results = OperationResults()
        with (
            ThreadPoolExecutor(max_workers=individual_concurrency) as individual_pool,
            ThreadPoolExecutor(max_workers=batch_concurrency) as batch_pool,
        ):
            individual_futures = [
                individual_pool.submit(self._execute_individual, op)
                for op in individual
            ]
            batch_futures = [
                batch_pool.submit(self._run_batch, chunk) for chunk in chunks
            ]
            for individual_future in individual_futures:
                results.append(individual_future.result())
            for batch_future in batch_futures:
                results.extend(batch_future.result())

        return results

    def _partition(self) -> tuple[list[tuple[_Operation, int]], list[_Operation]]:
        """Splits operations into ``(batchable, individual)``.

        Each batchable entry is paired with its estimated post-compression size,
        used for chunking. Only oversized (or unsized) inserts go individual.
        """
        batchable: list[tuple[_Operation, int]] = []
        individual: list[_Operation] = []
        for op in self._ops:
            if isinstance(op, _InsertOp):
                size = self._insert_size(op)
                if size is not None and size <= MAX_BATCH_PART_SIZE:
                    batchable.append((op, size))
                else:
                    individual.append(op)
            else:
                batchable.append((op, 0))
        return batchable, individual

    def _insert_size(self, op: _InsertOp) -> int | None:
        """Estimates the post-compression body size of an insert.

        Returns ``None`` when the size cannot be determined (e.g. a
        non-seekable stream), signalling that it must be sent individually.
        """
        if isinstance(op.body, bytes):
            raw = len(op.body)
        else:
            size = _stream_size(op.body)
            if size is None:
                return None
            raw = size

        if self._effective_compression(op) == "zstd":
            return _zstd_compress_bound(raw)
        return raw

    def _effective_compression(self, op: _InsertOp) -> Compression:
        compression = op.compression or self._session._usecase._compression
        return compression

    # -- execution --------------------------------------------------------------

    def _execute_individual(self, op: _Operation) -> OperationResult:
        """Executes a single operation via the ordinary (non-batch) endpoints."""
        if isinstance(op, _InsertOp):
            try:
                key = self._session.put(
                    op.body,
                    key=op.key,
                    compression=op.compression,
                    content_type=op.content_type,
                    metadata=op.metadata,
                    expiration_policy=op.expiration_policy,
                    origin=op.origin,
                    filename=op.filename,
                )
                return PutResult(key, None)
            except Exception as exc:
                return PutResult(op.key or "<unknown>", exc)
        elif isinstance(op, _GetOp):
            try:
                response = self._session.get(
                    op.key,
                    decompress=op.decompress,
                    accept_encoding=op.accept_encoding,
                )
                return GetResult(op.key, response, None)
            except RequestError as exc:
                if exc.status == 404:
                    return GetResult(op.key, None, None)
                return GetResult(op.key, None, exc)
            except Exception as exc:
                return GetResult(op.key, None, exc)
        elif isinstance(op, _DeleteOp):
            try:
                self._session.delete(op.key)
                return DeleteResult(op.key, None)
            except Exception as exc:
                return DeleteResult(op.key, exc)
        else:
            try:
                metadata = self._session.head(op.key)
                return HeadResult(op.key, metadata, None)
            except Exception as exc:
                return HeadResult(op.key, None, exc)

    def _run_batch(self, ops: list[_Operation]) -> list[OperationResult]:
        """Runs one chunk as a batch request, converting failures to results."""
        try:
            return self._send_batch(ops)
        except Exception as exc:
            return [_error_result_for(op, exc) for op in ops]

    def _send_batch(self, ops: list[_Operation]) -> list[OperationResult]:
        fields = [self._build_part(op) for op in ops]
        body, content_type = encode_multipart_formdata(fields)

        headers = self._session._make_headers()
        headers["Content-Type"] = content_type

        response = self._session._pool.request(
            "POST",
            self._session._make_batch_url(),
            body=body,
            headers=headers,
            preload_content=True,
            decode_content=True,
        )
        raise_for_status(response)

        boundary = _extract_boundary(response.headers.get("content-type", ""))
        parts = _parse_multipart_response(response.data or b"", boundary)
        return self._results_from_parts(ops, parts)

    def _build_part(self, op: _Operation) -> RequestField:
        if isinstance(op, _GetOp):
            return _simple_part("get", op.key)
        if isinstance(op, _DeleteOp):
            return _simple_part("delete", op.key)
        if isinstance(op, _HeadOp):
            return _simple_part("head", op.key)
        return self._insert_part(op)

    def _insert_part(self, op: _InsertOp) -> RequestField:
        headers: dict[str, str] = {HEADER_BATCH_OPERATION_KIND: "insert"}
        if op.key is not None:
            headers[HEADER_BATCH_OPERATION_KEY] = quote(op.key, safe="")

        payload = op.body if isinstance(op.body, bytes) else op.body.read()

        if self._effective_compression(op) == "zstd":
            payload = zstandard.ZstdCompressor().compress(payload)
            headers["Content-Encoding"] = "zstd"

        if op.content_type:
            headers["Content-Type"] = op.content_type
        expiration = op.expiration_policy or self._session._usecase._expiration_policy
        if expiration:
            headers[HEADER_EXPIRATION] = format_expiration(expiration)
        if op.origin:
            headers[HEADER_ORIGIN] = op.origin
        if op.filename is not None:
            headers[HEADER_FILENAME] = op.filename
        if op.metadata:
            for meta_key, meta_value in op.metadata.items():
                headers[f"{HEADER_META_PREFIX}{meta_key}"] = meta_value

        return RequestField("part", data=payload, headers=headers)

    # -- response correlation ---------------------------------------------------

    def _results_from_parts(
        self,
        ops: list[_Operation],
        parts: list[tuple[dict[str, str], bytes]],
    ) -> list[OperationResult]:
        results: list[OperationResult] = []
        seen: set[int] = set()
        for headers, part_body in parts:
            index, result = self._result_from_part(ops, headers, part_body)
            if index is not None:
                seen.add(index)
            results.append(result)

        for index in range(len(ops)):
            if index not in seen:
                results.append(
                    _error_result_for(
                        ops[index],
                        RequestError(
                            f"server did not return a response for operation at "
                            f"index {index}",
                            status=0,
                            response="",
                        ),
                    )
                )
        return results

    def _result_from_part(
        self,
        ops: list[_Operation],
        headers: dict[str, str],
        part_body: bytes,
    ) -> tuple[int | None, OperationResult]:
        index_raw = headers.get(HEADER_BATCH_OPERATION_INDEX)
        if index_raw is None or not index_raw.isdigit():
            return None, ErrorResult(
                RequestError(
                    f"missing or invalid {HEADER_BATCH_OPERATION_INDEX} header",
                    status=0,
                    response="",
                )
            )
        index = int(index_raw)

        status = _parse_status(headers.get(HEADER_BATCH_OPERATION_STATUS))
        if status is None:
            return None, ErrorResult(
                RequestError(
                    f"missing or invalid {HEADER_BATCH_OPERATION_STATUS} header",
                    status=0,
                    response="",
                )
            )

        if index >= len(ops):
            return None, ErrorResult(
                RequestError(
                    f"response references unknown operation index {index}",
                    status=0,
                    response="",
                )
            )
        op = ops[index]

        key_header = headers.get(HEADER_BATCH_OPERATION_KEY)
        key = unquote(key_header) if key_header else op.key

        is_error = status >= 400 and not (
            isinstance(op, (_GetOp, _HeadOp)) and status == 404
        )

        # For successful responses the key is always present; for errors it may
        # be absent (e.g. a server-generated-key insert that failed before a key
        # was assigned), in which case we fall back to a sentinel.
        if key is None:
            if is_error:
                key = "<unknown>"
            else:
                return index, ErrorResult(
                    RequestError(
                        f"missing {HEADER_BATCH_OPERATION_KEY} header",
                        status=0,
                        response="",
                    )
                )

        if is_error:
            message = part_body.decode("utf-8", "replace")
            error = RequestError(
                f"operation failed with HTTP status code {status}",
                status=status,
                response=message,
            )
            return index, _typed_error(op, key, error)

        if isinstance(op, _GetOp):
            if status == 404:
                return index, GetResult(key, None, None)
            metadata = Metadata.from_headers(headers)
            payload = _maybe_decompress(part_body, metadata, op)
            return index, GetResult(key, GetResponse(metadata, payload), None)
        if isinstance(op, _InsertOp):
            return index, PutResult(key, None)
        if isinstance(op, _DeleteOp):
            return index, DeleteResult(key, None)
        # head
        if status == 404:
            return index, HeadResult(key, None, None)
        return index, HeadResult(key, Metadata.from_headers(headers), None)


def _typed_error(op: _Operation, key: str, error: Exception) -> OperationResult:
    if isinstance(op, _GetOp):
        return GetResult(key, None, error)
    if isinstance(op, _InsertOp):
        return PutResult(key, error)
    if isinstance(op, _DeleteOp):
        return DeleteResult(key, error)
    return HeadResult(key, None, error)


def _error_result_for(op: _Operation, error: Exception) -> OperationResult:
    return _typed_error(op, op.key or "<unknown>", error)


def _simple_part(kind: str, key: str) -> RequestField:
    return RequestField(
        "part",
        data=b"",
        headers={
            HEADER_BATCH_OPERATION_KIND: kind,
            HEADER_BATCH_OPERATION_KEY: quote(key, safe=""),
        },
    )


def _maybe_decompress(payload: bytes, metadata: Metadata, op: _GetOp) -> IO[bytes]:
    """Applies transparent zstd decompression, mirroring :meth:`Session.get`."""
    accept = op.accept_encoding
    encoding_accepted = accept is not None and (
        "*" in accept or metadata.compression in accept
    )
    if metadata.compression and op.decompress and not encoding_accepted:
        if metadata.compression != "zstd":
            raise NotImplementedError(
                "Transparent decoding of anything but `zstd` is not implemented yet"
            )
        metadata.compression = None
        dctx = zstandard.ZstdDecompressor()
        return dctx.stream_reader(BytesIO(payload), read_across_frames=True)
    return BytesIO(payload)


def _stream_size(stream: IO[bytes]) -> int | None:
    """Returns the number of readable bytes remaining, or ``None`` if unknown."""
    try:
        if not stream.seekable():
            return None
        position = stream.tell()
        end = stream.seek(0, 2)  # SEEK_END
        stream.seek(position)
        return end - position
    except (OSError, ValueError):
        return None


def _zstd_compress_bound(src_size: int) -> int:
    """Python port of ``ZSTD_compressBound`` (worst-case compressed size)."""
    margin = ((128 << 10) - src_size) >> 11 if src_size < (128 << 10) else 0
    return src_size + (src_size >> 8) + margin


def _iter_batches(
    ops: list[tuple[_Operation, int]],
) -> Iterator[list[_Operation]]:
    """Groups operations into batches respecting the count and size limits."""
    batch: list[_Operation] = []
    batch_size = 0
    for op, size in ops:
        if batch and (
            len(batch) >= MAX_BATCH_OPS or batch_size + size > MAX_BATCH_BODY_SIZE
        ):
            yield batch
            batch, batch_size = [], 0
        batch.append(op)
        batch_size += size
    if batch:
        yield batch


def _parse_status(value: str | None) -> int | None:
    """Parses a status header like ``"200 OK"`` into its numeric code."""
    if not value:
        return None
    token = value.split(" ", 1)[0]
    try:
        return int(token)
    except ValueError:
        return None


def _extract_boundary(content_type: str) -> str:
    for segment in content_type.split(";"):
        segment = segment.strip()
        if segment.startswith("boundary="):
            boundary = segment[len("boundary=") :].strip()
            if len(boundary) >= 2 and boundary[0] == '"' and boundary[-1] == '"':
                boundary = boundary[1:-1]
            return boundary
    raise RequestError(
        "missing multipart boundary in response Content-Type",
        status=0,
        response=content_type,
    )


def _parse_multipart_response(
    body: bytes, boundary: str
) -> list[tuple[dict[str, str], bytes]]:
    """Parses a ``multipart/form-data`` body into ``(headers, body)`` parts."""
    delimiter = b"--" + boundary.encode("latin-1")
    parts: list[tuple[dict[str, str], bytes]] = []

    for segment in body.split(delimiter):
        # Skip the preamble (empty) and the closing boundary (starts with "--").
        if not segment or segment.startswith(b"--"):
            continue
        # Each part segment is `\r\n<headers>\r\n\r\n<body>\r\n`.
        if segment.startswith(b"\r\n"):
            segment = segment[2:]
        header_blob, separator, part_body = segment.partition(b"\r\n\r\n")
        if not separator:
            continue
        if part_body.endswith(b"\r\n"):
            part_body = part_body[:-2]

        headers: dict[str, str] = {}
        for line in header_blob.split(b"\r\n"):
            if not line:
                continue
            name, _, value = line.partition(b":")
            headers[name.decode("utf-8", "replace").strip().lower()] = value.decode(
                "utf-8", "replace"
            ).strip()
        parts.append((headers, part_body))

    return parts
