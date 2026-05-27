"""Batch operations API for executing multiple get/put/delete operations."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from io import BytesIO
from typing import IO, TYPE_CHECKING, Literal, NamedTuple
from urllib.parse import quote, unquote

import zstandard

from objectstore_client.metadata import (
    HEADER_EXPIRATION,
    HEADER_META_PREFIX,
    HEADER_ORIGIN,
    Compression,
    ExpirationPolicy,
    Metadata,
    format_expiration,
)
from objectstore_client.multipart import (
    RequestPart,
    ResponsePart,
    encode_multipart,
    iter_multipart_response,
)

if TYPE_CHECKING:
    from objectstore_client.client import GetResponse, RequestError, Session

__all__ = [
    "Put",
    "Get",
    "Delete",
    "Operation",
    "ManyResponse",
    "execute_many",
    "MAX_BATCH_OPS",
    "MAX_BATCH_PART_SIZE",
    "MAX_BATCH_BODY_SIZE",
    "MAX_BATCH_CONCURRENCY",
]

# ---------------------------------------------------------------------------
# Constants (matching Rust client)
# ---------------------------------------------------------------------------

MAX_BATCH_OPS: int = 1000
"""Maximum number of operations per batch request."""

MAX_BATCH_PART_SIZE: int = 1024 * 1024  # 1 MB
"""Maximum body size for a single part in a batch request."""

MAX_BATCH_BODY_SIZE: int = 100 * 1024 * 1024  # 100 MB
"""Maximum total body size for a single batch request."""

MAX_BATCH_CONCURRENCY: int = 3
"""Default maximum number of concurrent batch/individual HTTP requests."""

# ---------------------------------------------------------------------------
# Batch protocol header constants
# ---------------------------------------------------------------------------

HEADER_BATCH_OP_KIND = "x-sn-batch-operation-kind"
HEADER_BATCH_OP_KEY = "x-sn-batch-operation-key"
HEADER_BATCH_OP_INDEX = "x-sn-batch-operation-index"
HEADER_BATCH_OP_STATUS = "x-sn-batch-operation-status"

# ---------------------------------------------------------------------------
# Operation types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Put:
    """A put operation to enqueue in a batch."""

    contents: bytes | IO[bytes]
    key: str | None = None
    compression: Compression | Literal["none"] | None = None
    content_type: str | None = None
    metadata: dict[str, str] | None = None
    expiration_policy: ExpirationPolicy | None = None
    origin: str | None = None


@dataclass(frozen=True)
class Get:
    """A get operation to enqueue in a batch."""

    key: str


@dataclass(frozen=True)
class Delete:
    """A delete operation to enqueue in a batch."""

    key: str


Operation = Put | Get | Delete

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


class ManyResponse(NamedTuple):
    """Result for a single operation in a batch.

    The ``key`` is always the object key (for puts: the server-assigned key).
    The ``response`` is:
    - ``GetResponse`` for a successful get
    - ``RequestError`` for a per-operation failure (not raised)
    - ``None`` for a successful put, successful delete, or get-not-found (404)
    """

    key: str
    response: GetResponse | RequestError | None


# ---------------------------------------------------------------------------
# Internal: prepared put
# ---------------------------------------------------------------------------


@dataclass
class _PreparedPut:
    """A Put operation with body materialized, compressed, and headers built."""

    key: str | None
    body: bytes
    headers: dict[str, str]


def _prepare_put(
    op: Put,
    default_compression: Compression | Literal["none"],
    default_expiration: ExpirationPolicy | None,
) -> _PreparedPut:
    """Materialize a Put's body (compress if needed) and build metadata headers."""
    if isinstance(op.contents, bytes):
        raw = op.contents
    else:
        raw = op.contents.read()

    compression = op.compression or default_compression
    headers: dict[str, str] = {}

    if compression == "zstd":
        cctx = zstandard.ZstdCompressor()
        body = cctx.compress(raw)
        headers["Content-Encoding"] = "zstd"
    else:
        body = raw

    if op.content_type:
        headers["Content-Type"] = op.content_type

    expiration = op.expiration_policy or default_expiration
    if expiration:
        headers[HEADER_EXPIRATION] = format_expiration(expiration)

    if op.origin:
        headers[HEADER_ORIGIN] = op.origin

    if op.metadata:
        for k, v in op.metadata.items():
            headers[f"{HEADER_META_PREFIX}{k}"] = v

    return _PreparedPut(key=op.key, body=body, headers=headers)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _classify(
    op: Operation, body_size: int
) -> tuple[Literal["batchable", "individual"], int]:
    """Classify an operation as batchable or individual.

    Get and Delete are always batchable (size 0).
    Put is individual if the compressed body exceeds MAX_BATCH_PART_SIZE.
    """
    if isinstance(op, (Get, Delete)):
        return "batchable", 0
    # Put
    if body_size > MAX_BATCH_PART_SIZE:
        return "individual", body_size
    return "batchable", body_size


# ---------------------------------------------------------------------------
# Batching
# ---------------------------------------------------------------------------

# A classified op ready for dispatch: (original_index, operation, prepared_put_or_none)
_ClassifiedOp = tuple[int, Operation, _PreparedPut | None]


def _iter_batches(
    ops: Sequence[tuple[int, Operation, _PreparedPut | None, int]],
) -> Iterator[list[_ClassifiedOp]]:
    """Split batchable operations into batches respecting count and size limits.

    Each element is (original_index, operation, prepared, body_size).
    Yields lists of (original_index, operation, prepared) with the size dropped.
    """
    remaining = iter(ops)
    pending: tuple[int, Operation, _PreparedPut | None, int] | None = None

    while True:
        batch: list[_ClassifiedOp] = []
        batch_size = 0

        if pending is not None:
            idx, op, prepared, op_size = pending
            batch.append((idx, op, prepared))
            batch_size += op_size
            pending = None

        for idx, op, prepared, op_size in remaining:
            if len(batch) >= MAX_BATCH_OPS:
                pending = (idx, op, prepared, op_size)
                break
            if batch and batch_size + op_size > MAX_BATCH_BODY_SIZE:
                pending = (idx, op, prepared, op_size)
                break
            batch.append((idx, op, prepared))
            batch_size += op_size

        if not batch:
            return

        yield batch


# ---------------------------------------------------------------------------
# Batch request/response
# ---------------------------------------------------------------------------


def _build_batch_parts(
    ops: Sequence[tuple[int, Operation, _PreparedPut | None]],
) -> Iterator[RequestPart]:
    """Yield multipart request parts from classified operations."""
    for _idx, op, prepared in ops:
        headers: dict[str, str] = {}

        if isinstance(op, Get):
            headers[HEADER_BATCH_OP_KIND] = "get"
            headers[HEADER_BATCH_OP_KEY] = quote(op.key, safe="")
            yield RequestPart(headers=headers, body=b"")

        elif isinstance(op, Delete):
            headers[HEADER_BATCH_OP_KIND] = "delete"
            headers[HEADER_BATCH_OP_KEY] = quote(op.key, safe="")
            yield RequestPart(headers=headers, body=b"")

        elif isinstance(op, Put):
            assert prepared is not None
            headers[HEADER_BATCH_OP_KIND] = "insert"
            if prepared.key is not None:
                headers[HEADER_BATCH_OP_KEY] = quote(prepared.key, safe="")
            headers.update(prepared.headers)
            yield RequestPart(headers=headers, body=prepared.body)


def _parse_batch_response(
    response_parts: Iterable[ResponsePart],
    ops: Sequence[tuple[int, Operation, _PreparedPut | None]],
) -> Iterator[tuple[int, ManyResponse]]:
    """Stream multipart response parts into indexed ManyResponse tuples."""
    from objectstore_client.client import GetResponse, RequestError

    # Build a map from batch-local index to (original_index, operation, prepared)
    index_map = {batch_idx: entry for batch_idx, entry in enumerate(ops)}

    seen_indices: set[int] = set()

    for part in response_parts:
        part_headers = part.headers

        # Parse operation index
        index_str = part_headers.get(HEADER_BATCH_OP_INDEX)
        if index_str is None:
            continue
        batch_idx = int(index_str)
        seen_indices.add(batch_idx)

        entry = index_map.get(batch_idx)
        if entry is None:
            continue
        original_idx, op, prepared = entry

        # Parse status
        status_str = part_headers.get(HEADER_BATCH_OP_STATUS, "")
        status_code_str = status_str.split(" ", 1)[0] if status_str else "0"
        status_code = int(status_code_str)

        # Parse key from response
        encoded_key = part_headers.get(HEADER_BATCH_OP_KEY)
        response_key = unquote(encoded_key) if encoded_key else None

        # Determine the key to use in the result
        if isinstance(op, (Get, Delete)):
            result_key = response_key or op.key
        else:
            result_key = (
                response_key or (prepared.key if prepared else None) or "<unknown>"
            )

        # Handle errors (status >= 400, except 404 for GET)
        is_get_not_found = isinstance(op, Get) and status_code == 404
        if status_code >= 400 and not is_get_not_found:
            error_message = part.body.decode("utf-8", "replace")
            error = RequestError(
                f"Batch operation failed with status {status_code}",
                status_code,
                error_message,
            )
            yield (original_idx, ManyResponse(key=result_key, response=error))
            continue

        # Handle GET not found
        if is_get_not_found:
            yield (original_idx, ManyResponse(key=result_key, response=None))
            continue

        # Successful operations
        if isinstance(op, Get):
            metadata = Metadata.from_headers(part_headers)
            payload = BytesIO(part.body)

            # Decompress if needed
            if metadata.compression == "zstd":
                dctx = zstandard.ZstdDecompressor()
                decompressed = dctx.decompress(part.body)
                payload = BytesIO(decompressed)
                metadata.compression = None

            response = GetResponse(metadata=metadata, payload=payload)
            yield (original_idx, ManyResponse(key=result_key, response=response))

        elif isinstance(op, Put):
            yield (original_idx, ManyResponse(key=result_key, response=None))

        elif isinstance(op, Delete):
            yield (original_idx, ManyResponse(key=result_key, response=None))

    # After all parts arrive, report any operations the server didn't respond to.
    for batch_idx, entry in index_map.items():
        if batch_idx not in seen_indices:
            original_idx, op, prepared = entry
            if isinstance(op, (Get, Delete)):
                key = op.key
            else:
                key = (prepared.key if prepared else None) or "<unknown>"

            error = RequestError(
                f"Server did not return a response for operation at index {batch_idx}",
                0,
                "",
            )
            yield (original_idx, ManyResponse(key=key, response=error))


def _send_batch(
    session: Session,
    ops: Sequence[tuple[int, Operation, _PreparedPut | None]],
) -> Iterator[tuple[int, ManyResponse]]:
    """Send a batch of operations as a single multipart request."""
    from objectstore_client.client import RequestError

    parts = _build_batch_parts(ops)
    content_type, body_iter = encode_multipart(parts)

    batch_url = session._make_batch_url()
    headers = session._make_headers()
    headers["Content-Type"] = content_type

    try:
        response = session._pool.request(
            "POST",
            batch_url,
            body=body_iter,
            headers=headers,
            preload_content=False,
        )

        if response.status >= 400:
            error_body = response.read().decode("utf-8", "replace")
            error = RequestError(
                f"Batch request failed with status {response.status}",
                response.status,
                error_body,
            )
            yield from _batch_level_error(ops, error)
            return

        response_content_type = response.headers.get("Content-Type", "")
        yield from _parse_batch_response(
            iter_multipart_response(response_content_type, response.stream(65536)),
            ops,
        )
    except RequestError:
        raise
    except Exception as exc:
        error = RequestError(f"Batch request failed: {exc}", 0, str(exc))
        yield from _batch_level_error(ops, error)


def _batch_level_error(
    ops: Sequence[tuple[int, Operation, _PreparedPut | None]],
    error: RequestError,
) -> list[tuple[int, ManyResponse]]:
    """Produce error results for all operations when the entire batch fails."""
    results: list[tuple[int, ManyResponse]] = []
    for original_idx, op, prepared in ops:
        if isinstance(op, (Get, Delete)):
            key = op.key
        else:
            key = (prepared.key if prepared else None) or "<unknown>"
        results.append((original_idx, ManyResponse(key=key, response=error)))
    return results


# ---------------------------------------------------------------------------
# Individual execution (for oversized ops)
# ---------------------------------------------------------------------------


def _execute_individual(
    session: Session, original_idx: int, op: Operation, prepared: _PreparedPut | None
) -> tuple[int, ManyResponse]:
    """Execute a single operation via the non-batch endpoint."""
    from objectstore_client.client import RequestError

    try:
        if isinstance(op, Get):
            response = session.get(op.key)
            return (original_idx, ManyResponse(key=op.key, response=response))

        elif isinstance(op, Delete):
            session.delete(op.key)
            return (original_idx, ManyResponse(key=op.key, response=None))

        elif isinstance(op, Put):
            key = session.put(
                op.contents,
                key=op.key,
                compression=op.compression,
                content_type=op.content_type,
                metadata=op.metadata,
                expiration_policy=op.expiration_policy,
                origin=op.origin,
            )
            return (original_idx, ManyResponse(key=key, response=None))

    except RequestError as exc:
        if isinstance(op, (Get, Delete)):
            key = op.key
        else:
            key = op.key or "<unknown>"
        return (original_idx, ManyResponse(key=key, response=exc))


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def execute_many(
    session: Session,
    operations: Iterable[Operation],
    concurrency: int = MAX_BATCH_CONCURRENCY,
) -> Iterator[ManyResponse]:
    """Execute multiple operations, batching where possible.

    Args:
        session: The session to execute operations against.
        operations: The operations to execute. Any iterable is accepted,
            including generators; it is consumed exactly once.
        concurrency: Max parallel HTTP requests. Default is 3.
            Set to 1 for sequential execution (no thread pool).
            Must be >= 1.

    Returns:
        An iterator of ManyResponse. With concurrency=1 results arrive in
        input order. With concurrency > 1 results arrive in completion order.
    """
    if concurrency <= 0:
        raise ValueError(f"concurrency must be >= 1, got {concurrency}")
    return _execute_many_gen(session, operations, concurrency)


def _execute_many_gen(
    session: Session,
    operations: Iterable[Operation],
    concurrency: int,
) -> Iterator[ManyResponse]:
    default_compression = session._usecase._compression
    default_expiration = session._usecase._expiration_policy

    # Step 1: Consume the iterable once, preparing and classifying as we go.
    batchable: list[tuple[int, Operation, _PreparedPut | None, int]] = []
    individual: list[_ClassifiedOp] = []

    for idx, op in enumerate(operations):
        if isinstance(op, Put):
            if isinstance(op.contents, bytes):
                prepared = _prepare_put(op, default_compression, default_expiration)
                kind, size = _classify(op, body_size=len(prepared.body))
                if kind == "individual":
                    individual.append((idx, op, prepared))
                else:
                    batchable.append((idx, op, prepared, size))
            else:
                # IO[bytes] bodies are always sent individually to avoid eager reading.
                individual.append((idx, op, None))
        else:
            batchable.append((idx, op, None, 0))

    # Step 2: Partition batchable ops into batch chunks.
    batch_chunks = list(_iter_batches(batchable))

    def run_batch(chunk: list[_ClassifiedOp]) -> list[tuple[int, ManyResponse]]:
        return list(_send_batch(session, chunk))

    def run_individual(entry: _ClassifiedOp) -> list[tuple[int, ManyResponse]]:
        idx, op, prepared = entry
        return [_execute_individual(session, idx, op, prepared)]

    # Step 3: Execute and yield results as they arrive.
    if concurrency == 1:
        for chunk in batch_chunks:
            for _, result in _send_batch(session, chunk):
                yield result
        for entry in individual:
            for _, result in run_individual(entry):
                yield result
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_batch, chunk) for chunk in batch_chunks]
            futures += [executor.submit(run_individual, entry) for entry in individual]
            for future in as_completed(futures):
                for _, result in future.result():
                    yield result
