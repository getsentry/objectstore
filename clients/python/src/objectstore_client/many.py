"""
Batch operations API for executing multiple operations.

:meth:`Session.many <objectstore_client.client.Session.many>` takes any iterable
of :class:`Get`, :class:`Put`, :class:`Delete`, and :class:`Head` operations and
executes them with as few requests as possible. Operations within the batch
protocol's per-part size limit are grouped into multipart requests to the
``objects:batch`` endpoint. Oversized and unsized inserts fall back to individual
requests.

Everything is streamed, both on the send and receive side.
"""

from __future__ import annotations

import atexit
import queue
import threading
from collections import Counter
from collections.abc import Generator, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from io import BytesIO
from typing import IO, TYPE_CHECKING, cast

import urllib3
import zstandard
from zstandard import ZstdCompressionReader

from objectstore_client.client import GetResponse
from objectstore_client.errors import RequestError, raise_for_status
from objectstore_client.formdata import (
    CHUNK_SIZE,
    MultipartBody,
    PartBody,
    RequestPart,
    ResponsePart,
    iter_multipart,
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
)
from objectstore_client.metrics import (
    batch_size_bucket,
    count_batch_operations,
    measure_storage_operation,
)
from objectstore_client.utils import decode_header_value, encode_header_value

if TYPE_CHECKING:
    from objectstore_client.client import Session

# Maximum number of operations to send in a single batch request.
MAX_BATCH_OPS = 1000

# Maximum (post-compression, estimated) size of a single part's body in a batch
# request. Inserts above this are sent as individual requests instead.
MAX_BATCH_PART_SIZE = 1024 * 1024  # 1 MB

# Maximum total (post-compression, estimated) body size of a single batch request.
MAX_BATCH_BODY_SIZE = 100 * 1024 * 1024  # 100 MB

# Default maximum number of concurrent requests.
DEFAULT_CONCURRENCY = 3

# How long a worker waits on a full result queue before re-checking whether the
# caller has abandoned the results.
_CANCEL_POLL_INTERVAL = 0.2

# Reported as the key of an operation that failed before it had one, which can
# only happen for a keyless insert.
_UNKNOWN_KEY = "<unknown>"

# Set while the interpreter shuts down, to release a worker waiting to hand over
# a result. Results abandoned without being closed would otherwise leave the
# thread pool's join waiting on workers that nothing is going to drain.
#
# `ThreadPoolExecutor` joins its threads from a `threading._register_atexit`
# callback rather than an `atexit` one, because `atexit` runs after those joins.
# Those callbacks run in reverse registration order, so registering ours here
# (below the import that registers the pool's) releases the workers just before
# the join.
_SHUTTING_DOWN = threading.Event()
_register_before_join = getattr(threading, "_register_atexit", atexit.register)
_register_before_join(_SHUTTING_DOWN.set)

HEADER_BATCH_OPERATION_KIND = "x-sn-batch-operation-kind"
HEADER_BATCH_OPERATION_KEY = "x-sn-batch-operation-key"
HEADER_BATCH_OPERATION_INDEX = "x-sn-batch-operation-index"
HEADER_BATCH_OPERATION_STATUS = "x-sn-batch-operation-status"


@dataclass(frozen=True)
class Get:
    """
    Fetches an object. See :meth:`Session.get <objectstore_client.client.Session.get>`.
    """

    key: str

    decompress: bool = True
    """Whether to transparently decompress the payload."""

    accept_encoding: Sequence[str] | None = None
    """Compression algorithms to pass through compressed instead of decompressing."""


@dataclass(frozen=True)
class Put:
    """
    Uploads an object. See :meth:`Session.put <objectstore_client.client.Session.put>`.

    Stream ``contents`` must be seekable to be batched, as its size is otherwise
    unknown; unseekable streams are sent as individual requests.
    """

    contents: bytes | IO[bytes]
    key: str | None = None
    compress: Compression | None = None
    precompressed: Compression | None = None
    content_type: str | None = None
    metadata: dict[str, str] | None = None
    expiration_policy: ExpirationPolicy | None = None
    origin: str | None = None
    filename: str | None = None

    def __post_init__(self) -> None:
        if self.compress is not None and self.precompressed is not None:
            raise ValueError("Cannot pass both `compress` and `precompressed`")
        if self.compress is not None and self.compress not in ("none", "zstd"):
            raise ValueError(f"Invalid compression: {self.compress}")
        if self.precompressed is not None and self.precompressed != "zstd":
            raise ValueError(f"Invalid compression: {self.precompressed}")


@dataclass(frozen=True)
class Delete:
    """
    Deletes an object.
    See :meth:`Session.delete <objectstore_client.client.Session.delete>`.
    """

    key: str


@dataclass(frozen=True)
class Head:
    """
    Fetches an object's metadata.
    See :meth:`Session.head <objectstore_client.client.Session.head>`.
    """

    key: str


Operation = Get | Put | Delete | Head
"""An operation that can be passed to :meth:`Session.many`."""


@dataclass
class GetResult:
    """The result of a :class:`Get` operation."""

    index: int
    """Position of the operation in the sequence given to :meth:`Session.many`."""

    key: str

    response: GetResponse | None
    """The fetched object, or ``None`` if it does not exist."""

    error: Exception | None
    """The error that occurred, or ``None`` if the operation succeeded."""


@dataclass
class PutResult:
    """The result of a :class:`Put` operation."""

    index: int
    """Position of the operation in the sequence given to :meth:`Session.many`."""

    key: str
    """The object key, as assigned by the server for a keyless :class:`Put`."""

    error: Exception | None
    """The error that occurred, or ``None`` if the operation succeeded."""


@dataclass
class DeleteResult:
    """The result of a :class:`Delete` operation."""

    index: int
    """Position of the operation in the sequence given to :meth:`Session.many`."""

    key: str

    error: Exception | None
    """The error that occurred, or ``None`` if the operation succeeded."""


@dataclass
class HeadResult:
    """The result of a :class:`Head` operation."""

    index: int
    """Position of the operation in the sequence given to :meth:`Session.many`."""

    key: str

    metadata: Metadata | None
    """The object's metadata, or ``None`` if it does not exist."""

    error: Exception | None
    """The error that occurred, or ``None`` if the operation succeeded."""


@dataclass
class ErrorResult:
    """
    An error that cannot be attributed to a specific operation.
    """

    index: int | None
    """Position of the operation, when the response says which one it was."""

    error: Exception


OperationResult = GetResult | PutResult | DeleteResult | HeadResult | ErrorResult
"""The result of a single operation in a :meth:`Session.many` call."""


class OperationResults:
    """
    A lazy iterator over the results of a :meth:`Session.many` call.
    """

    def __init__(self, results: Generator[OperationResult, None, None]):
        self._results = results

    def __iter__(self) -> Iterator[OperationResult]:
        return self

    def __next__(self) -> OperationResult:
        return next(self._results)

    def __enter__(self) -> OperationResults:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Abandons all operations that have not completed yet."""
        self._results.close()

    def failures(self) -> list[OperationResult]:
        """
        Drains the results and returns those that carry an error.
        """
        return [result for result in self if result.error is not None]

    def raise_for_failures(self) -> None:
        """
        Drains the results and raises an ``ExceptionGroup`` of all errors, if any.
        """
        failures = self.failures()
        errors = [f.error for f in failures if f.error is not None]
        if errors:
            raise ExceptionGroup("one or more batch operations failed", errors)


def execute_many(
    session: Session,
    operations: Iterable[Operation],
    *,
    concurrency: int | None = None,
) -> OperationResults:
    """
    Executes ``operations``, batching them where possible.

    This is the implementation of :meth:`Session.many
    <objectstore_client.client.Session.many>`; see there for documentation.
    """
    concurrency = DEFAULT_CONCURRENCY if concurrency is None else concurrency
    if concurrency < 1:
        raise ValueError(f"concurrency must be at least 1, got {concurrency}")
    return OperationResults(_execute(session, operations, concurrency))


def _classify(session: Session, op: Operation) -> int | None:
    """
    Returns the body size an operation contributes to a batch request.

    ``None`` marks it unbatchable.
    """
    if isinstance(op, (Get, Delete, Head)):
        return 0
    if not isinstance(op, Put):
        raise TypeError(f"not an objectstore operation: {op!r}")

    size = _body_size(op.contents)
    if size is None:
        return None

    # Compression happens while the body is streamed, so its size on the wire is
    # only known up to the worst case. Precompressed bodies are sent verbatim.
    if _compress_with(session, op) == "zstd":
        size = _zstd_compress_bound(size)

    return size if size <= MAX_BATCH_PART_SIZE else None


def _body_size(contents: bytes | IO[bytes]) -> int | None:
    if isinstance(contents, bytes):
        return len(contents)
    try:
        if not contents.seekable():
            return None
        position = contents.tell()
        end = contents.seek(0, 2)  # Seek to end
        contents.seek(position)
        return end - position
    except (OSError, ValueError):
        return None


def _zstd_compress_bound(size: int) -> int:
    """
    Returns the worst-case size of ``size`` bytes compressed as a single zstd frame.

    A port of the ``ZSTD_COMPRESSBOUND`` macro definition in ``zstd.h``.
    """
    margin = ((128 << 10) - size) >> 11 if size < (128 << 10) else 0
    return size + (size >> 8) + margin


def _encoding(session: Session, op: Put) -> Compression:
    """Returns the ``Content-Encoding`` the object is stored with."""
    return op.precompressed or op.compress or session._usecase._compression


def _compress_with(session: Session, op: Put) -> Compression:
    """Returns the compression the client applies to the body, if any."""
    if op.precompressed is not None:
        return "none"
    return _encoding(session, op)


@dataclass
class _Batch:
    """A group of operations sent as a single batch request."""

    ops: list[tuple[int, Operation]]


@dataclass
class _Individual:
    """An operation sent as an individual (non-batch) request."""

    index: int
    op: Operation


_Work = _Batch | _Individual


def _iter_work(
    classified: Iterable[tuple[int, Operation, int | None]],
) -> Iterator[_Work]:
    """
    Groups classified operations into work items, lazily.

    Each operation travels with the index it had in the sequence the caller gave,
    which is what its result reports back.
    """
    batch: list[tuple[int, Operation]] = []
    batch_size = 0

    for index, op, size in classified:
        # Ops with unknown size must be sent as individual ops
        if size is None:
            # If we've been accumulating a batch, cut if off here and yield it
            if batch:
                yield _Batch(batch)
                batch, batch_size = [], 0
            # Now yield this individual op
            yield _Individual(index, op)
            continue

        # If we've hit a batch size/length limit, cut the batch off here.
        if batch and (
            len(batch) >= MAX_BATCH_OPS or batch_size + size > MAX_BATCH_BODY_SIZE
        ):
            yield _Batch(batch)
            batch, batch_size = [], 0

        # Add to current batch
        batch.append((index, op))
        batch_size += size

    # Yield whatever is left over
    if batch:
        yield _Batch(batch)


def _op_key(op: Operation) -> str | None:
    if isinstance(op, Put):
        return op.key or None
    return op.key


def _execute(
    session: Session,
    operations: Iterable[Operation],
    concurrency: int,
) -> Generator[OperationResult, None, None]:
    concurrency = _fit_to_connection_pool(session, concurrency)
    work = _iter_work(
        (index, op, _classify(session, op)) for index, op in enumerate(operations)
    )

    if concurrency == 1:
        for item in work:
            yield from _run_work(session, item)
    else:
        yield from _execute_concurrent(session, work, concurrency)


def _fit_to_connection_pool(session: Session, concurrency: int) -> int:
    """
    Returns how many requests to keep in flight, sizing the pool to suit.

    This function reconciles the ``concurrency`` argument that the caller passed
    in to ``session.many()`` with the ``maxsize`` and ``block`` options on the
    ``urllib3`` connection pool.

    ``maxsize`` controls how many free connections are retained in the pool.
    ``block`` controls whether a request must wait for a free connection before
    being sent or if it may open a new connection.

    When ``block=False``(default), a connection pool may send more than
    ``maxsize`` concurrent requests, but they will not reuse connections
    effectively and ``urllib3`` emits warnings about it. So, this function
    overwrites ``maxsize`` if the caller's desired ``concurrency`` is higher.

    When ``block=True``, ``maxsize`` is meant to be a hard cap on the number of
    concurrent requests. This function leaves ``maxsize`` alone in this case
    and clamps the caller's desired ``concurrency`` to be no larger than
    ``maxsize``.
    """
    pool = getattr(session, "_pool", None)
    connections = getattr(pool, "pool", None)
    if connections is None:
        return concurrency

    if getattr(pool, "block", False):
        return max(1, min(concurrency, connections.maxsize))

    if connections.maxsize < concurrency:
        connections.maxsize = concurrency
    return concurrency


class _Done:
    """
    Sentinel value that workers push to the results queue when their work is
    finished.

    This signals when every submitted item is accounted for and when a new item
    may be submitted.
    """


_DONE = _Done()


def _execute_concurrent(
    session: Session,
    work: Iterator[_Work],
    concurrency: int,
) -> Iterator[OperationResult]:
    """
    Runs work items on a thread pool, yielding results in completion order.

    At most ``concurrency`` items are in flight. Workers hand results over
    through a bounded queue.
    """
    pool = ThreadPoolExecutor(
        max_workers=concurrency, thread_name_prefix="objectstore-many"
    )
    results: queue.Queue[OperationResult | _Done] = queue.Queue(maxsize=concurrency * 2)
    cancelled = threading.Event()

    def put(item: OperationResult | _Done) -> bool:
        """
        Passes a result through the results queue out to the caller. Returns
        ``False`` if we were told to exit.
        """
        while not (cancelled.is_set() or _SHUTTING_DOWN.is_set()):
            try:
                results.put(item, timeout=_CANCEL_POLL_INTERVAL)
                return True
            except queue.Full:
                continue
        return False

    def run(item: _Work) -> None:
        """Work item runner that runs on the thread pool."""
        if cancelled.is_set() or _SHUTTING_DOWN.is_set():
            # The caller abandoned the results between this item's submission
            # and a worker picking it up, so it must not be sent.
            return

        try:
            work_results = _run_work(session, item)
            try:
                for result in work_results:
                    if not put(result):
                        return
            finally:
                # Releases the connection if the consumer abandoned us
                # part-way through the batch response.
                work_results.close()
        except Exception as error:
            put(ErrorResult(None, error))
        finally:
            put(_DONE)

    submitted = 0
    finished = 0
    exhausted = False
    try:
        while True:
            # Submit work items until we finish or hit `concurrency`.
            while not exhausted and submitted - finished < concurrency:
                item = next(work, None)
                if item is None:
                    exhausted = True
                    break
                pool.submit(run, item)
                submitted += 1

            # Normal completion condition has triggered
            if exhausted and submitted == finished:
                return

            # Pop a result off the queue. If it's `_Done` then the whole work
            # item has completed. Otherwise it's an individual operation result.
            result = results.get()
            if isinstance(result, _Done):
                finished += 1
            else:
                yield result
    finally:
        # We've exited the runloop but we haven't hit the normal completion
        # case. Set `cancelled.set()` so `pool` workers will give up and we can
        # clean everything up.
        cancelled.set()
        pool.shutdown(wait=False, cancel_futures=True)


def _run_work(session: Session, item: _Work) -> Generator[OperationResult, None, None]:
    if isinstance(item, _Batch):
        yield from _stream_batch(session, item.ops)
    else:
        yield _execute_individual(session, item.index, item.op)


def _execute_individual(session: Session, index: int, op: Operation) -> OperationResult:
    """Executes a single operation through the ordinary (non-batch) endpoints."""
    if isinstance(op, Get):
        try:
            response = session.get(
                op.key,
                decompress=op.decompress,
                accept_encoding=op.accept_encoding,
            )
            return GetResult(index, op.key, response, None)
        except Exception as error:
            return GetResult(index, op.key, None, error)
    elif isinstance(op, Put):
        try:
            key = session.put(
                op.contents,
                key=op.key,
                compress=op.compress,
                precompressed=op.precompressed,
                content_type=op.content_type,
                metadata=op.metadata,
                expiration_policy=op.expiration_policy,
                origin=op.origin,
                filename=op.filename,
            )
            return PutResult(index, key, None)
        except Exception as error:
            return PutResult(index, op.key or _UNKNOWN_KEY, error)
    elif isinstance(op, Delete):
        try:
            session.delete(op.key)
            return DeleteResult(index, op.key, None)
        except Exception as error:
            return DeleteResult(index, op.key, error)
    else:
        try:
            return HeadResult(index, op.key, session.head(op.key), None)
        except Exception as error:
            return HeadResult(index, op.key, None, error)


def _stream_batch(
    session: Session, ops: list[tuple[int, Operation]]
) -> Iterator[OperationResult]:
    """
    Sends ``ops`` as one batch request, streaming out results as parts arrive.

    Whether the request fails outright or part-way through the response, the
    error goes to every operation that has not produced a result yet — so none is
    reported twice, and none is dropped.
    """
    # Actual parts to be sent in the batch request. Filters out malformed parts.
    parts: list[RequestPart] = []
    # Associates each operation with its original index in the batch.
    sent: list[tuple[int, Operation]] = []
    for index, op in ops:
        request_part = _build_part(session, op)
        try:
            request_part.validate()
        except ValueError as invalid:
            yield _error_result(index, op, invalid)
            continue
        parts.append(request_part)
        sent.append((index, op))

    if not sent:
        return

    body = MultipartBody(parts)
    headers = session._make_headers()
    headers["Content-Type"] = body.content_type

    usecase = session._usecase.name
    # Emit counters for each op we're about to send in this batch
    count_batch_operations(
        session._metrics_backend, usecase, Counter(_op_kind(op) for _, op in sent)
    )

    # The server sends each part as its operation completes, so a batch's latency
    # is that of the whole exchange — including time the caller spends taking
    # results off this generator, which is what paces reading the response.

    # Measures batch processing latency. Note that this includes the time the
    # caller spends consuming the results iterator and not just client/server
    # latency.
    with measure_storage_operation(
        session._metrics_backend,
        "batch",
        usecase,
        tags={"operations": batch_size_bucket(len(sent))},
    ):
        response = session._pool.request(
            "POST",
            session._make_batch_url(),
            body=body,
            headers=headers,
            preload_content=False,
            decode_content=True,
            # `None` means defer to the pool, not "no retries"
            retries=None if body.resendable else _single_send_retries(session),
        )
        completed = False
        seen: set[int] = set()
        try:
            raise_for_status(response)

            content_type = response.headers.get("content-type", "")
            for part in iter_multipart(content_type, response.stream(CHUNK_SIZE)):
                position, result = _result_from_part(sent, part)
                if position is not None:
                    seen.add(position)
                yield result
            completed = True

            # Yield an error for every operation that didn't have a
            # corresponding result part.
            yield from _unanswered(sent, seen, None)
        except Exception as error:
            # Yield an error for every operation that we haven't yielded a
            # response part for yet.
            yield from _unanswered(sent, seen, error)
        finally:
            if completed:
                response.drain_conn()
                response.release_conn()
            else:
                # Not read to the end, so the connection cannot be reused and
                # draining it could mean reading an unbounded body.
                response.close()


def _unanswered(
    sent: list[tuple[int, Operation]], seen: set[int], error: Exception | None
) -> Iterator[OperationResult]:
    """
    Reports every operation of a batch that no response part accounted for.
    """
    for position, (index, op) in enumerate(sent):
        if position in seen:
            continue
        yield _error_result(
            index,
            op,
            error
            or RequestError(
                f"server did not return a response for the operation at index {index}",
                status=0,
                response="",
            ),
        )


def _single_send_retries(session: Session) -> urllib3.Retry:
    """
    Returns a policy that lets urllib3 retry a request so long as the retry
    doesn't re-send the body.

    A body that reads from a stream can only be sent once, so the only retry we
    can try one that happens before the body is touched: a connection failure.
    """
    retries = urllib3.Retry.from_int(session._pool.retries)
    return retries.new(read=0, other=0, redirect=0)


def _op_kind(op: Operation) -> str:
    """Returns the batch protocol's name for this kind of operation."""
    if isinstance(op, Get):
        return "get"
    if isinstance(op, Put):
        return "insert"
    if isinstance(op, Delete):
        return "delete"
    return "head"


def _build_part(session: Session, op: Operation) -> RequestPart:
    if isinstance(op, Put):
        return _insert_part(session, op)
    return _keyed_part(_op_kind(op), op.key)


def _keyed_part(kind: str, key: str) -> RequestPart:
    return RequestPart(
        headers={
            HEADER_BATCH_OPERATION_KIND: kind,
            HEADER_BATCH_OPERATION_KEY: encode_header_value(key),
        }
    )


def _insert_part(session: Session, op: Put) -> RequestPart:
    headers = {HEADER_BATCH_OPERATION_KIND: _op_kind(op)}
    key = op.key or None
    if key is not None:
        headers[HEADER_BATCH_OPERATION_KEY] = encode_header_value(key)

    encoding = _encoding(session, op)
    if encoding != "none":
        headers["Content-Encoding"] = encoding
    if op.content_type:
        headers["Content-Type"] = op.content_type

    expiration_policy = op.expiration_policy or session._usecase._expiration_policy
    if expiration_policy:
        headers[HEADER_EXPIRATION] = format_expiration(expiration_policy)
    if op.origin:
        headers[HEADER_ORIGIN] = encode_header_value(op.origin)
    if op.filename is not None:
        headers[HEADER_FILENAME] = encode_header_value(op.filename)
    if op.metadata:
        for name, value in op.metadata.items():
            headers[f"{HEADER_META_PREFIX}{name}"] = encode_header_value(value)

    return RequestPart(headers, _part_body(op.contents, _compress_with(session, op)))


def _part_body(contents: bytes | IO[bytes], compress_with: Compression) -> PartBody:
    """
    Returns the part's payload, deferring reads and compression to send time.

    An uncompressed payload is handed over as it is. A compressing one becomes a
    :class:`_ZstdBody` that the writer reads as the request goes out.
    """
    if compress_with != "zstd":
        return contents

    source = BytesIO(contents) if isinstance(contents, bytes) else contents
    return cast(IO[bytes], _ZstdBody(source))


class _ZstdBody:
    """
    A part payload that compresses a source stream as it is read. File-like,
    non-rewindable stream.

    The zstd compressor is somewhat heavy so it is dropped as soon as the source
    stream is exhausted.
    """

    def __init__(self, source: IO[bytes]):
        compressor = zstandard.ZstdCompressor()
        self._reader: ZstdCompressionReader | None = compressor.stream_reader(
            source,
            closefd=False,  # Not our stream to close
        )
        self._closed = False

    def read(self, size: int = -1, /) -> bytes:
        if self._closed:
            raise ValueError("read of closed stream")

        # Not closed, but source is exhausted. EOF
        if self._reader is None:
            return b""

        chunk: bytes = self._reader.read(size)
        # When the stream is exhausted, drop the compressor
        if not chunk:
            self._reader = None

        return chunk

    def close(self) -> None:
        if self._reader:
            self._reader.close()
        self._closed = True
        self._reader = None

    @property
    def closed(self) -> bool:
        return self._closed


def _result_from_part(
    sent: list[tuple[int, Operation]], part: ResponsePart
) -> tuple[int | None, OperationResult]:
    """
    Turns one response part into a result for the operation it belongs to.

    Returns the operation's position within the batch (server-side, so not
    including malformed operations that were dropped before sending) or ``None``
    when the part can't be linked to an operation.
    """
    raw_position = part.headers.get(HEADER_BATCH_OPERATION_INDEX)
    if raw_position is None or not raw_position.isdigit():
        return None, ErrorResult(
            None,
            _malformed(f"missing or invalid {HEADER_BATCH_OPERATION_INDEX} header"),
        )
    position = int(raw_position)

    status = _parse_status(part.headers.get(HEADER_BATCH_OPERATION_STATUS))
    if status is None:
        return None, ErrorResult(
            None,
            _malformed(f"missing or invalid {HEADER_BATCH_OPERATION_STATUS} header"),
        )

    if position >= len(sent):
        return None, ErrorResult(
            None, _malformed(f"response references unknown operation index {position}")
        )
    index, op = sent[position]

    # Prefer the server's key as the source of truth.
    raw_key = part.headers.get(HEADER_BATCH_OPERATION_KEY)
    key = decode_header_value(raw_key) if raw_key else _op_key(op)

    # A missing object is a successful "not found" for reads, an error otherwise.
    is_error = status >= 400 and not (isinstance(op, (Get, Head)) and status == 404)

    if key is None:
        # Any successful operation must have a key. The condition below should
        # never occur.
        if not is_error:
            return position, ErrorResult(
                index, _malformed(f"missing {HEADER_BATCH_OPERATION_KEY} header")
            )
        key = _UNKNOWN_KEY

    if is_error:
        error = RequestError(
            f"batch operation failed with status {status}",
            status=status,
            response=part.body.decode("utf-8", "replace"),
        )
        return position, _error_result(index, op, error, key=key)

    if isinstance(op, Get):
        if status == 404:
            return position, GetResult(index, key, None, None)
        metadata = Metadata.from_headers(part.headers)
        payload = _maybe_decompress(part.body, metadata, op)
        return position, GetResult(index, key, GetResponse(metadata, payload), None)
    if isinstance(op, Put):
        return position, PutResult(index, key, None)
    if isinstance(op, Delete):
        return position, DeleteResult(index, key, None)
    if status == 404:
        return position, HeadResult(index, key, None, None)
    return position, HeadResult(index, key, Metadata.from_headers(part.headers), None)


def _maybe_decompress(payload: bytes, metadata: Metadata, op: Get) -> IO[bytes]:
    """Applies transparent decompression, mirroring :meth:`Session.get`."""
    accept_encoding = op.accept_encoding
    encoding_accepted = accept_encoding is not None and (
        "*" in accept_encoding or metadata.compression in accept_encoding
    )
    if metadata.compression and op.decompress and not encoding_accepted:
        if metadata.compression != "zstd":
            raise NotImplementedError(
                "Transparent decoding of anything but `zstd` is not implemented yet"
            )
        metadata.compression = None
        decompressor = zstandard.ZstdDecompressor()
        return decompressor.stream_reader(BytesIO(payload), read_across_frames=True)
    return BytesIO(payload)


def _parse_status(value: str | None) -> int | None:
    """Parses a status header like ``"200 OK"`` into its numeric code."""
    if not value:
        return None
    code = value.split(" ", 1)[0]
    try:
        return int(code)
    except ValueError:
        return None


def _malformed(message: str) -> RequestError:
    return RequestError(f"malformed batch response: {message}", status=0, response="")


def _error_result(
    index: int, op: Operation, error: Exception, *, key: str | None = None
) -> OperationResult:
    """Creates a result of the right kind for ``op``, carrying ``error``."""
    key = key or _op_key(op) or _UNKNOWN_KEY
    if isinstance(op, Get):
        return GetResult(index, key, None, error)
    if isinstance(op, Put):
        return PutResult(index, key, error)
    if isinstance(op, Delete):
        return DeleteResult(index, key, error)
    return HeadResult(index, key, None, error)
