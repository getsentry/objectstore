from __future__ import annotations

import io
import json
import pathlib
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

import pytest
import sentry_sdk
import urllib3
import zstandard
from objectstore_client import Client, Session, Usecase
from objectstore_client import many as many_module
from objectstore_client.formdata import (
    ResponsePart,
    iter_multipart,
)
from objectstore_client.many import (
    MAX_BATCH_BODY_SIZE,
    MAX_BATCH_OPS,
    MAX_BATCH_PART_SIZE,
    Delete,
    DeleteResult,
    ErrorResult,
    Get,
    GetResult,
    Head,
    HeadResult,
    Operation,
    OperationResult,
    Put,
    PutResult,
    _classify,
    _fit_to_connection_pool,
    _iter_work,
    _parse_status,
    _zstd_compress_bound,
    _ZstdBody,
)
from objectstore_client.metadata import TimeToLive
from objectstore_client.metrics import MetricsBackend, Tags, batch_size_bucket
from objectstore_client.utils import decode_header_value, encode_header_value

_STATUS_REASONS = {
    200: "OK",
    201: "Created",
    204: "No Content",
    403: "Forbidden",
    404: "Not Found",
}

# Headers of the batch protocol itself, which are not object metadata.
_PROTOCOL_HEADERS = {
    "x-sn-batch-operation-kind",
    "x-sn-batch-operation-key",
    "x-sn-batch-operation-index",
    "x-sn-batch-operation-status",
    "content-disposition",
}


def _status_line(code: int) -> str:
    return f"{code} {_STATUS_REASONS[code]}"


class Unseekable(io.BytesIO):
    """A stream whose size cannot be known without reading it."""

    def seekable(self) -> bool:
        return False


class FakeResponse:
    """A stand-in for a urllib3 response, streaming its body in small chunks."""

    def __init__(self, status: int, headers: dict[str, str], data: bytes = b""):
        self.status = status
        self.headers = headers
        self.data = data
        self.drained = False
        self.released = False
        self.closed = False
        self._buffer = io.BytesIO(data)

    def read(self, amt: int | None = None) -> bytes:
        return self._buffer.read(amt)

    def stream(self, _amt: int | None = None) -> Iterator[bytes]:
        # A deliberately tiny chunk size, so that parts and boundaries land
        # across chunks the way they do on a real socket.
        while chunk := self._buffer.read(13):
            yield chunk

    def json(self) -> Any:
        return json.loads(self.data)

    def drain_conn(self) -> None:
        self.drained = True

    def release_conn(self) -> None:
        self.released = True

    def close(self) -> None:
        self.closed = True


class FakeObjectstore:
    """
    An in-memory stand-in for the endpoints the many API talks to.

    Requests are parsed with the formdata codec's ``iter_multipart`` response
    parser as the grammar is the same for requests and responses.

    This class shouldn't be used to write integration tests that rely on real
    server behavior. Those tests should be written in ``test_e2e.py``. What it
    offers instead is more granular visibilty into request/connection details
    and, via subclasses, behavior that should be impossible from a real server.

    ``FakeObjectstore`` imitates the parts of a ``urllib3`` connection pool that
    the client uses, as that's how it's shimmed into our test Objectstore
    clients. Tests subclass it and override methods to do whatever is needed for
    the test.
    """

    retries = urllib3.Retry(connect=3, read=0)

    def __init__(self, *, deny_writes: bool = False, in_order: bool = False):
        self.headers: dict[str, str] = {}
        self.store: dict[str, tuple[dict[str, str], bytes]] = {}
        self.deny_writes = deny_writes
        self.in_order = in_order
        """Whether to respond in request order, rather than the reverse."""

        self.batches: list[list[ResponsePart]] = []
        """The parsed request parts of every batch request received."""

        self.bodies: list[Any] = []
        """The raw body object of every batch request received."""

        self.individual: list[str] = []
        """The keys of every insert received on the individual endpoint."""

        self.responses: list[FakeResponse] = []
        """Every response handed out, to inspect how connections were released."""

        self.retries_used: list[Any] = []
        """The retry policy of every request, ``None`` when the pool's applies."""

        self._generated = 0

    @property
    def requests(self) -> int:
        return len(self.batches) + len(self.individual)

    def request(
        self,
        method: str,
        url: str,
        *,
        body: Any = None,
        headers: dict[str, str] | None = None,
        **_kwargs: Any,
    ) -> FakeResponse:
        self.retries_used.append(_kwargs.get("retries"))
        if "objects:batch" in url:
            assert method == "POST"
            response = self._batch(body, headers or {})
        else:
            response = self._individual_insert(method, url, body, headers or {})
        self.responses.append(response)
        return response

    def _individual_insert(
        self, method: str, url: str, body: Any, headers: dict[str, str]
    ) -> FakeResponse:
        assert method in ("POST", "PUT")
        key = url.rsplit("/", 1)[-1] or self._next_key()
        contents = body.read() if hasattr(body, "read") else bytes(body or b"")
        self.store[key] = (_metadata_headers(headers), contents)
        self.individual.append(key)
        return FakeResponse(200, {}, json.dumps({"key": key}).encode())

    def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
        self.bodies.append(body)
        raw = body if isinstance(body, bytes) else b"".join(body)
        parts = list(iter_multipart(headers["Content-Type"], [raw]))
        self.batches.append(parts)

        responses = [self._execute(index, part) for index, part in enumerate(parts)]
        if not self.in_order:
            # The server executes the parts of a batch concurrently and responds
            # in completion order, which is not the order they were sent in.
            responses.reverse()

        boundary = "os-boundary-" + f"{len(self.batches):032x}"
        return FakeResponse(
            200,
            {"content-type": f'multipart/form-data; boundary="{boundary}"'},
            _serialize_response(responses, boundary),
        )

    def _execute(self, index: int, part: ResponsePart) -> ResponsePart:
        kind = part.headers["x-sn-batch-operation-kind"]
        raw_key = part.headers.get("x-sn-batch-operation-key")
        key = decode_header_value(raw_key) if raw_key else None

        headers = {
            "content-disposition": "form-data; name=part",
            "x-sn-batch-operation-index": str(index),
            "x-sn-batch-operation-kind": kind,
        }

        def respond(status: int, key: str | None, body: bytes = b"") -> ResponsePart:
            if key is not None:
                headers["x-sn-batch-operation-key"] = encode_header_value(key)
            headers["x-sn-batch-operation-status"] = _status_line(status)
            return ResponsePart(headers, body)

        if kind == "insert":
            assert len(part.body) <= MAX_BATCH_PART_SIZE, "part exceeds server limit"
            if self.deny_writes:
                return respond(403, key, b'{"detail":"forbidden"}')
            key = key or self._next_key()
            self.store[key] = (_metadata_headers(part.headers), part.body)
            return respond(201, key)

        assert key is not None, f"{kind} operation without a key"

        if kind == "delete":
            if self.deny_writes:
                return respond(403, key, b'{"detail":"forbidden"}')
            self.store.pop(key, None)
            return respond(204, key)

        if key not in self.store:
            return respond(404, key)
        metadata, contents = self.store[key]

        if kind == "get":
            headers.update(metadata)
            headers.setdefault("content-type", "application/octet-stream")
            headers["x-sn-time-created"] = "2024-01-01T00:00:00+00:00"
            return respond(200, key, contents)

        assert kind == "head", f"unknown operation kind {kind}"
        headers.update(metadata)
        headers["x-sn-time-created"] = "2024-01-01T00:00:00+00:00"
        headers["x-sn-size"] = str(len(contents))
        return respond(200, key)

    def _next_key(self) -> str:
        self._generated += 1
        return f"generated-{self._generated}"


def _metadata_headers(headers: dict[str, str]) -> dict[str, str]:
    """Extracts the headers the server would persist as object metadata."""
    return {
        name.lower(): value
        for name, value in headers.items()
        if name.lower() not in _PROTOCOL_HEADERS
        and (name.lower().startswith(("content-", "x-sn-", "x-snme-")))
    }


def _serialize_response(parts: list[ResponsePart], boundary: str) -> bytes:
    """Serializes parts exactly the way the server's multipart writer does."""
    out = bytearray()
    for part in parts:
        out += f"--{boundary}\r\n".encode()
        for name, value in part.headers.items():
            out += f"{name}: {value}\r\n".encode()
        out += b"\r\n" + part.body + b"\r\n"
    out += f"--{boundary}--".encode()
    return bytes(out)


def _respond_with(parts: list[ResponsePart]) -> FakeResponse:
    """A batch response carrying exactly ``parts``."""
    boundary = "os-boundary-" + "0" * 32
    return FakeResponse(
        200,
        {"content-type": f'multipart/form-data; boundary="{boundary}"'},
        _serialize_response(parts, boundary),
    )


class RecordingMetrics(MetricsBackend):
    """Records every metric the client emits."""

    def __init__(self) -> None:
        self.counters: list[tuple[str, int | float, Tags | None]] = []
        self.distributions: list[tuple[str, int | float, Tags | None]] = []

    def increment(
        self, name: str, value: int | float = 1, tags: Tags | None = None
    ) -> None:
        self.counters.append((name, value, tags))

    def gauge(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        pass

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
    ) -> None:
        self.distributions.append((name, value, tags))


def _session(
    pool: Any, compression: str = "none", metrics: MetricsBackend | None = None
) -> Session:
    client = Client("http://localhost:8888", metrics_backend=metrics)
    client._pool = pool
    usecase = Usecase(
        "testing",
        compression=compression,  # type: ignore[arg-type]
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    return client.session(usecase, org=42, project=1337)


def _by_key(results: list[OperationResult]) -> dict[str, OperationResult]:
    return {
        result.key: result for result in results if not isinstance(result, ErrorResult)
    }


@pytest.mark.parametrize("concurrency", [1, 3])
def test_many_round_trip(concurrency: int) -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    results = list(
        session.many(
            [
                Put(b"first", key="key-1", filename="report.pdf"),
                Put(b"second", key="key-2", compress="zstd"),
                Put(b"third", key="key-3", metadata={"foo": "bar"}),
                Put(b"fourth", key="key-4"),
            ],
            concurrency=concurrency,
        )
    )
    puts = [result for result in results if isinstance(result, PutResult)]
    assert len(puts) == len(results)
    assert sorted(put.key for put in puts) == ["key-1", "key-2", "key-3", "key-4"]
    assert pool.requests == 1

    results = list(
        session.many(
            [Get("key-1"), Get("key-2"), Head("key-3"), Delete("key-4"), Get("gone")],
            concurrency=concurrency,
        )
    )
    assert len(results) == 5
    by_key = _by_key(results)

    first = by_key["key-1"]
    assert isinstance(first, GetResult) and first.response is not None
    assert first.response.payload.read() == b"first"
    assert first.response.metadata.filename == "report.pdf"

    second = by_key["key-2"]
    assert isinstance(second, GetResult) and second.response is not None
    # Transparently decompressed, so the metadata no longer claims compression.
    assert second.response.metadata.compression is None
    assert second.response.payload.read() == b"second"

    third = by_key["key-3"]
    assert isinstance(third, HeadResult) and third.metadata is not None
    assert third.metadata.custom == {"foo": "bar"}

    assert isinstance(by_key["key-4"], DeleteResult)
    assert by_key["key-4"].error is None
    assert "key-4" not in pool.store

    gone = by_key["gone"]
    assert isinstance(gone, GetResult)
    assert gone.response is None and gone.error is None

    # A missing object is a successful "not found" for reads, not a failure.
    (missing,) = list(session.many([Head("gone")], concurrency=concurrency))
    assert isinstance(missing, HeadResult)
    assert missing.metadata is None and missing.error is None


@pytest.mark.parametrize(
    "get", [Get("k", decompress=False), Get("k", accept_encoding=["zstd"])]
)
def test_many_optionally_skips_decompression(get: Get) -> None:
    """Both ways of opting out leave the payload and its encoding as stored."""
    pool = FakeObjectstore()
    session = _session(pool)
    session.many([Put(b"payload", key="k", compress="zstd")]).raise_for_failures()

    (result,) = list(session.many([get]))

    assert isinstance(result, GetResult) and result.response is not None
    assert result.response.metadata.compression == "zstd"
    raw = result.response.payload.read()
    assert (
        zstandard.ZstdDecompressor().stream_reader(io.BytesIO(raw)).read() == b"payload"
    )


def test_many_sends_metadata_headers() -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    session.many(
        [
            Put(
                b"payload",
                key="k",
                content_type="text/plain",
                metadata={"foo": "bär", "with-newline": "a\r\nb"},
                origin="127.0.0.1",
                filename="rapport.pdf",
                expiration_policy=TimeToLive(timedelta(hours=2)),
            )
        ]
    ).raise_for_failures()

    (part,) = pool.batches[0]
    assert part.headers["content-type"] == "text/plain"
    assert part.headers["x-sn-origin"] == "127.0.0.1"
    assert part.headers["x-sn-expiration"] == "ttl:2h"
    # Values that a header cannot carry verbatim are percent-encoded, which also
    # keeps CR/LF from forging additional headers.
    assert part.headers["x-snme-with-newline"] == "a%0D%0Ab"
    assert part.headers["x-snme-foo"] == "b%C3%A4r"

    (result,) = list(session.many([Get("k")]))
    assert isinstance(result, GetResult) and result.response is not None
    assert result.response.metadata.custom == {"foo": "bär", "with-newline": "a\r\nb"}
    assert result.response.metadata.filename == "rapport.pdf"


def test_many_reports_failures() -> None:
    pool = FakeObjectstore(deny_writes=True)
    pool.store["exists"] = ({}, b"hi")
    session = _session(pool)
    ops: list[Operation] = [Put(b"x", key="k1"), Get("exists"), Delete("k2")]

    failures = session.many(ops).failures()
    assert sorted(failure.key for failure in failures) == ["k1", "k2"]  # type: ignore[union-attr]

    with pytest.raises(ExceptionGroup) as exc_info:
        session.many(ops).raise_for_failures()
    assert len(exc_info.value.exceptions) == 2


class _Rejects(FakeObjectstore):
    """Turns the whole batch away."""

    def request(self, *_args: Any, **_kwargs: Any) -> FakeResponse:
        return FakeResponse(500, {}, b"boom")


class _Unreachable(FakeObjectstore):
    """Never gets the request out at all."""

    def request(self, *_args: Any, **_kwargs: Any) -> FakeResponse:
        raise urllib3.exceptions.NewConnectionError(
            None,  # type: ignore[arg-type]
            "connection refused",
        )


class _SkipsLastPart(FakeObjectstore):
    """Answers every operation but the last."""

    def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
        response = super()._batch(body, headers)
        parts = list(iter_multipart(response.headers["content-type"], [response.data]))
        return _respond_with(parts[:-1])


class _Truncated(FakeObjectstore):
    """Cuts the response short, as a dropped connection would."""

    def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
        response = super()._batch(body, headers)
        return FakeResponse(200, response.headers, response.data[:-20])


class _BadDate(FakeObjectstore):
    """Gives the first part a header the client cannot parse."""

    def _execute(self, index: int, part: ResponsePart) -> ResponsePart:
        response = super()._execute(index, part)
        if index == 0:
            response.headers["x-sn-time-created"] = "not-a-date"
        return response


class _CannotSign(FakeObjectstore):
    """Fails while the request is prepared, before any of it is sent."""

    @property
    def headers(self) -> dict[str, str]:
        # `Session._make_headers` reads these to sign the request.
        raise RuntimeError("signing failed")

    @headers.setter
    def headers(self, value: dict[str, str]) -> None:
        pass  # `FakeObjectstore.__init__` assigns them; only reads should fail.


@pytest.mark.parametrize("concurrency", [1, 3])
@pytest.mark.parametrize(
    ("server", "failed"),
    [
        (_CannotSign, {"a", "b"}),
        (_Rejects, {"a", "b"}),
        (_Unreachable, {"a", "b"}),
        (_SkipsLastPart, {"b"}),
        (_Truncated, {"b"}),
        (_BadDate, {"a"}),
    ],
)
def test_many_broken_batch_has_result_for_every_op(
    server: type[FakeObjectstore], failed: set[str], concurrency: int
) -> None:
    """
    However a batch breaks, each operation gets exactly one result.

    Both concurrencies, because they deliver results by different routes:
    straight from the generator, or through a pool worker.
    """
    pool = server(in_order=True)
    pool.store["a"] = ({}, b"payload")
    pool.store["b"] = ({}, b"payload")
    session = _session(pool)

    results = list(session.many([Get("a"), Get("b")], concurrency=concurrency))

    assert len(results) == 2
    assert {result.key for result in results if result.error is not None} == failed  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        # No index, so nothing ties the part to an operation: it is reported on
        # its own, and the operation is left to the unanswered sweep.
        pytest.param({"x-sn-batch-operation-status": "200 OK"}, 2, id="unattributable"),
        # An index but no status. The part names its operation, so the failure
        # belongs to that operation alone and nothing sweeps it up again.
        pytest.param(
            {"x-sn-batch-operation-index": "0", "x-sn-batch-operation-key": "a"},
            1,
            id="attributable",
        ),
    ],
)
def test_many_reports_part_it_cannot_use(
    headers: dict[str, str], expected: int
) -> None:
    class BadPartPool(FakeObjectstore):
        def _batch(self, body: Any, _headers: dict[str, str]) -> FakeResponse:
            return _respond_with([ResponsePart(headers, b"")])

    results = list(_session(BadPartPool()).many([Get("a")]))

    assert len(results) == expected
    assert all(result.error is not None for result in results)
    # Either way the operation is accounted for exactly once, and by a result of
    # its own kind rather than a bare `ErrorResult`.
    (answered,) = [result for result in results if isinstance(result, GetResult)]
    assert answered.index == 0 and answered.key == "a"


@pytest.mark.parametrize(
    ("bad", "error"),
    [
        # A metadata name cannot be percent-escaped the way a value can.
        (Put(b"x", key="bad", metadata={"a\r\nb": "c"}), ValueError),
        (Put(b"x", key="bad", metadata={"not a header": "c"}), ValueError),
        # A value that is not a string cannot be escaped at all.
        (Put(b"x", key="bad", metadata={"m": 123}), TypeError),  # type: ignore[dict-item]
    ],
)
def test_many_fails_only_operations_it_cannot_encode(
    bad: Put, error: type[Exception]
) -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    results = list(session.many([bad, Put(b"y", key="good-1"), Delete("good-2")]))

    by_index = {result.index: result for result in results}
    assert isinstance(by_index[0].error, error)
    assert by_index[1].error is None and by_index[2].error is None
    # Dropping the first operation shifts the rest by one on the wire, so their
    # results have to report the index they had in the input.
    assert by_index[1].key == "good-1"  # type: ignore[union-attr]
    assert "good-1" in pool.store


def test_put_rejects_conflicting_compression() -> None:
    with pytest.raises(ValueError):
        Put(b"x", compress="zstd", precompressed="zstd")
    with pytest.raises(ValueError):
        Put(b"x", compress="gzip")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Put(b"x", precompressed="none")


def test_many_reports_index_of_each_operation() -> None:
    pool = FakeObjectstore()  # responds in reverse order
    session = _session(pool)
    keys = [f"key-{index}" for index in range(5)]

    results = list(session.many([Get(key) for key in keys], concurrency=1))

    assert [result.key for result in results] != keys  # type: ignore[union-attr]
    assert {result.index: result.key for result in results} == dict(  # type: ignore[union-attr]
        enumerate(keys)
    )


def test_many_correlates_keyless_puts_by_index() -> None:
    pool = FakeObjectstore()
    session = _session(pool)
    payloads = [f"payload-{index}".encode() for index in range(4)]

    results = list(session.many([Put(payload) for payload in payloads], concurrency=2))

    assert len(results) == len(payloads)
    for result in results:
        assert isinstance(result, PutResult) and result.error is None
        assert pool.store[result.key][1] == payloads[result.index]


def test_many_operation_count_limit() -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    ops: list[Operation] = [
        Delete(f"key-{index}") for index in range(MAX_BATCH_OPS + 1)
    ]
    results = list(session.many(ops, concurrency=1))

    assert len(results) == MAX_BATCH_OPS + 1
    assert [len(batch) for batch in pool.batches] == [MAX_BATCH_OPS, 1]


def test_iter_work_splits_on_body_size_limit() -> None:
    one_mb = 1024 * 1024
    classified = [(index, Delete(f"key-{index}"), one_mb) for index in range(150)]

    batches = list(_iter_work(classified))

    per_batch = MAX_BATCH_BODY_SIZE // one_mb
    assert [len(item.ops) for item in batches] == [per_batch, 150 - per_batch]  # type: ignore[union-attr]


def test_iter_work_oversized_operation() -> None:
    classified = [(0, Delete("k"), MAX_BATCH_BODY_SIZE + 1)]
    batches = list(_iter_work(classified))
    assert [len(item.ops) for item in batches] == [1]  # type: ignore[union-attr]


def test_iter_work_flushes_pending_batch_before_individual_request() -> None:
    classified: list[tuple[int, Operation, int | None]] = [
        (0, Get("a"), 0),
        (1, Put(b"big", key="b"), None),
        (2, Get("c"), 0),
    ]

    items = list(_iter_work(classified))

    assert [type(item).__name__ for item in items] == [
        "_Batch",
        "_Individual",
        "_Batch",
    ]


def test_classify_size_cutoff() -> None:
    session = _session(FakeObjectstore(), compression="zstd")

    # The largest payload whose worst-case compressed size still fits a part,
    # matching the boundary the Rust client tests.
    size = 1_044_496
    assert _zstd_compress_bound(size) == MAX_BATCH_PART_SIZE
    batchable = _classify(session, Put(b"a" * size, key="k"))
    assert batchable == MAX_BATCH_PART_SIZE

    unbatchable = _classify(session, Put(b"a" * (size + 1), key="k"))
    assert unbatchable is None


def test_classify_uses_exact_size_precompressed_body() -> None:
    session = _session(FakeObjectstore(), compression="zstd")

    # A size that the worst-case bound would push over the limit, but which is
    # sent verbatim because the payload is already compressed.
    payload = b"a" * MAX_BATCH_PART_SIZE
    size = _classify(session, Put(payload, key="k", precompressed="zstd"))
    assert size == MAX_BATCH_PART_SIZE


def test_classify_cannot_size_unseekable_stream() -> None:
    session = _session(FakeObjectstore())
    size = _classify(session, Put(Unseekable(b"data"), key="k"))
    assert size is None


def test_classify_rejects_foreign_operations() -> None:
    session = _session(FakeObjectstore())
    with pytest.raises(TypeError):
        _classify(session, "not an operation")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "contents",
    [
        pytest.param(b"x" * (2 * MAX_BATCH_PART_SIZE), id="oversized"),
        pytest.param(Unseekable(b"x" * 16), id="unsized"),
    ],
)
def test_many_sends_unbatchable_insert_individually(
    contents: bytes | io.BytesIO,
) -> None:
    """An insert too big to be a part, or of unknown size, gets its own request."""
    expected = contents if isinstance(contents, bytes) else contents.getvalue()
    pool = FakeObjectstore()
    session = _session(pool)

    results = list(
        session.many([Put(contents, key="alone"), Put(b"small", key="small")])
    )

    assert all(result.error is None for result in results)
    assert pool.individual == ["alone"]
    assert [len(batch) for batch in pool.batches] == [1]
    assert pool.store["alone"][1] == expected


def test_many_keeps_individual_insert_compressed() -> None:
    pool = FakeObjectstore()
    session = _session(pool, compression="zstd")
    big = b"x" * (4 * MAX_BATCH_PART_SIZE)

    (result,) = list(session.many([Put(big, key="big")]))

    assert result.error is None
    metadata, stored = pool.store["big"]
    assert metadata["content-encoding"] == "zstd"
    assert zstandard.ZstdDecompressor().stream_reader(io.BytesIO(stored)).read() == big


def test_many_streams_batched_stream_body() -> None:
    pool = FakeObjectstore()
    session = _session(pool, compression="zstd")
    payload = b"streamed payload " * 100

    (result,) = list(session.many([Put(io.BytesIO(payload), key="k")]))

    assert result.error is None
    metadata, stored = pool.store["k"]
    assert metadata["content-encoding"] == "zstd"
    assert (
        zstandard.ZstdDecompressor().stream_reader(io.BytesIO(stored)).read() == payload
    )


def test_many_leaves_caller_stream_open() -> None:
    pool = FakeObjectstore()
    session = _session(pool, compression="zstd")
    source = io.BytesIO(b"payload")

    session.many([Put(source, key="k")]).raise_for_failures()

    # The compressing reader wrapped around it is ours to close, the stream isn't.
    assert not source.closed


def test_zstd_body_holds_compressor_only_while_reading() -> None:
    """
    Zstd compression context costs ~1MB RAM. We want to defer creating them
    until reading and then drop it once the body is exhausted.
    """
    source = io.BytesIO(b"payload" * 100)
    body = _ZstdBody(source)
    assert body._compressed_stream is None

    compressed = body.read(8)
    started = body._compressed_stream is not None
    assert compressed and started
    while chunk := body.read(8):
        compressed += chunk

    assert (
        zstandard.ZstdDecompressor().stream_reader(io.BytesIO(compressed)).read()
        == b"payload" * 100
    )
    # The compressor is gone once the source is exhausted, but the source, which
    # belongs to the caller, is left open.
    assert body._compressed_stream is None
    assert not source.closed
    # A stream at the end of its input is at EOF, not closed, so reading on
    # returns empty bytes the way any file object does.
    assert not body.closed
    assert body.read(8) == b""


def test_many_does_not_resend_stream_bodies() -> None:
    pool = FakeObjectstore()
    session = _session(pool, compression="zstd")

    session.many([Put(io.BytesIO(b"streamed"), key="k")]).raise_for_failures()

    # Connect retries stay on: they happen before the body is read.
    (retries,) = pool.retries_used
    assert (retries.read, retries.other, retries.redirect) == (0, 0, 0)
    assert retries.connect == 3


def test_many_does_not_override_retry_policy_for_non_stream_bodies() -> None:
    pool = FakeObjectstore()
    session = _session(pool, compression="zstd")

    # Only inserts carry a body, and only a compressed one is a stream. Both of
    # these are re-sendable even though the usecase compresses by default.
    session.many([Get("a"), Delete("b"), Head("c")]).raise_for_failures()
    session.many([Put(b"payload", key="k", compress="none")]).raise_for_failures()

    assert pool.retries_used == [None, None]


def test_many_reads_operations_lazily() -> None:
    consumed = []

    def operations() -> Iterator[Operation]:
        for index in range(MAX_BATCH_OPS + 100):
            consumed.append(index)
            yield Delete(f"key-{index}")

    pool = FakeObjectstore()
    session = _session(pool)

    results = session.many(operations(), concurrency=1)
    assert consumed == []
    assert pool.requests == 0

    next(iter(results))
    assert len(consumed) <= MAX_BATCH_OPS + 1
    assert pool.requests == 1

    results.close()


def test_many_drained_batch_closes_connection() -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    list(session.many([Get("a"), Get("b")], concurrency=1))

    (response,) = pool.responses
    assert response.released and not response.closed


@pytest.mark.parametrize("concurrency", [1, 2])
def test_many_abandoned_batch_closes_connection(concurrency: int) -> None:
    pool = FakeObjectstore()
    for index in range(50):
        pool.store[f"key-{index}"] = ({}, b"payload")
    session = _session(pool)

    # One batch of fifty, so results are still coming when the caller walks away
    # after the first: on the calling thread at a concurrency of one, and from a
    # worker parked on the result queue above it.
    with session.many(
        [Get(f"key-{index}") for index in range(50)], concurrency=concurrency
    ) as results:
        next(iter(results))

    # The response was abandoned before its body was read to the end, so the
    # connection cannot be returned to the pool.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if all(response.closed for response in pool.responses):
            break
        time.sleep(0.05)
    assert all(response.closed and not response.released for response in pool.responses)


def test_many_runs_on_calling_thread_at_concurrency_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An effective concurrency of one means no thread pool, however it arose."""
    threads: list[int] = []

    class RecordingPool(FakeObjectstore):
        def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
            threads.append(threading.get_ident())
            return super()._batch(body, headers)

    # How the pool arrives at a concurrency of one is `_fit_to_connection_pool`'s
    # business, and is covered by its own tests.
    monkeypatch.setattr(many_module, "_fit_to_connection_pool", lambda *_args: 1)

    session = _session(RecordingPool())
    session.many([Get("a"), Delete("b")], concurrency=8).raise_for_failures()

    assert threads == [threading.get_ident()]


def test_many_runs_batches_concurrently() -> None:
    barrier = threading.Barrier(3, timeout=10)

    class BarrierPool(FakeObjectstore):
        def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
            # Only completes once three requests are in flight at the same time.
            barrier.wait()
            return super()._batch(body, headers)

    pool = BarrierPool()
    session = _session(pool)
    # Enough operations to fill two batches and start a third.
    ops: list[Operation] = [
        Delete(f"key-{index}") for index in range(2 * MAX_BATCH_OPS + 1)
    ]

    results = list(session.many(ops, concurrency=3))

    assert len(pool.batches) == 3
    assert len(results) == len(ops)
    assert all(result.error is None for result in results)


def test_many_drops_work_when_abandoned() -> None:
    pool = FakeObjectstore()
    for index in range(40):
        pool.store[f"key-{index}"] = ({}, b"payload")
    session = _session(pool)

    # An unsized insert cuts the batch it lands in, so this is four work items:
    # a batch, an insert, another batch, and the insert that must not be sent.
    ops: list[Operation] = [Get(f"key-{index}") for index in range(20)]
    ops.append(Put(Unseekable(b"payload"), key="cuts-the-batch"))
    ops += [Get(f"key-{index}") for index in range(20, 40)]
    ops.append(Put(Unseekable(b"payload"), key="never-sent"))

    results = session.many(ops, concurrency=2)
    next(iter(results))
    results.close()

    # The pool is internal, so there is nothing to join on: give a worker that
    # wrongly picked the insert up long enough to prove it.
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline and "never-sent" not in pool.store:
        time.sleep(0.01)

    # The first two items were dispatched, so this is work stopping rather than
    # work never having started.
    assert "cuts-the-batch" in pool.store
    assert "never-sent" not in pool.store


def _pool_queue(client: Client) -> Any:
    connections = client._pool.pool
    assert connections is not None
    return connections


def test_fit_to_connection_pool_block_is_false() -> None:
    """`concurrency` is the only knob; the pool is sized from it."""
    client = Client("http://localhost:8888")
    session = client.session(Usecase("testing", compression="none"), org=42)
    connections = _pool_queue(client)
    assert connections.maxsize == 1

    assert _fit_to_connection_pool(session, 8) == 8
    assert connections.maxsize == 8

    # Only ever grows, so a pooled connection is never left without a slot.
    assert _fit_to_connection_pool(session, 2) == 2
    assert connections.maxsize == 8


def test_fit_to_connection_pool_block_is_true() -> None:
    """`block=True` makes the pool size a deliberate cap on connections."""
    client = Client(
        "http://localhost:8888", connection_kwargs={"maxsize": 2, "block": True}
    )
    session = client.session(Usecase("testing", compression="none"), org=42)

    assert _fit_to_connection_pool(session, 8) == 2
    # Asking for less than the cap is still honoured, and the cap stays put.
    assert _fit_to_connection_pool(session, 1) == 1
    assert _pool_queue(client).maxsize == 2


@contextmanager
def _initialized_sentry() -> Iterator[None]:
    """Sentry's integrations are only installed for an initialized SDK."""
    previous = sentry_sdk.get_global_scope().client
    sentry_sdk.init(dsn=None, traces_sample_rate=1.0)
    try:
        yield
    finally:
        sentry_sdk.get_global_scope().set_client(previous)


def test_many_pool_thread_sentry_context() -> None:
    """
    Workers must run with the scope of the thread that submitted them.

    Nothing here does that by hand: `ThreadingIntegration` patches
    `ThreadPoolExecutor.submit` to carry the submitting scope into the task. We
    just want to make sure it continues working.
    """
    seen: list[tuple[int, str | None]] = []

    class RecordingPool(FakeObjectstore):
        def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
            seen.append(
                (
                    threading.get_ident(),
                    sentry_sdk.get_current_scope().get_traceparent(),
                )
            )
            return super().request(method, url, **kwargs)

    with _initialized_sentry():
        session = _session(RecordingPool())
        traceparent = sentry_sdk.get_current_scope().get_traceparent()

        # A batch request and an individual one, so both paths are covered.
        ops: list[Operation] = [Get("a"), Put(Unseekable(b"data"), key="b")]
        results = list(session.many(ops, concurrency=2))

    # No errors
    assert all(result.error is None for result in results)
    # Every thread had the same traceparent
    assert [scope for _, scope in seen] == [traceparent, traceparent]
    # Those threads actually weren't just the caller thread, proving the scopes
    # were inherited
    assert all(ident != threading.get_ident() for ident, _ in seen)


def test_many_does_not_hang_interpreter_exit_when_iterating() -> None:
    """A consumer waiting on results must not outlive the workers feeding it."""
    script = f"""
import sys, threading, time
sys.path.insert(0, {str(pathlib.Path(__file__).parent)!r})
from test_many import FakeObjectstore, _session

from objectstore_client.many import Get

class Slow(FakeObjectstore):
    def _batch(self, body, headers):
        time.sleep(0.5)
        return super()._batch(body, headers)

pool = Slow()
session = _session(pool)
started = threading.Event()

def consume():
    started.set()
    # Never abandoned, so the only thing that can release this is the shutdown.
    for _ in session.many([Get(f"key-{{i}}") for i in range(50)], concurrency=2):
        pass

# Not a daemon, so the interpreter joins it on the way out.
threading.Thread(target=consume, daemon=False).start()
started.wait()
time.sleep(0.1)  # let the request get in flight, then fall off the main thread
"""
    subprocess.run([sys.executable, "-c", script], timeout=60, check=True)


def test_many_does_not_hang_interpreter_exit_when_abandoned() -> None:
    """Workers park on a full queue; nothing may be left holding up the exit."""
    script = f"""
import sys
sys.path.insert(0, {str(pathlib.Path(__file__).parent)!r})
from test_many import FakeObjectstore, _session

from objectstore_client.many import Get

pool = FakeObjectstore()
session = _session(pool)
# One batch of many operations, so the worker has far more results to hand over
# than the queue holds and is still parked on it when the interpreter exits.
ops = [Get(f"key-{{i}}") for i in range(50)]

results = iter(session.many(ops, concurrency=2))
next(results)
# `results` stays referenced, so the generator is never closed and nothing
# releases the worker.
"""
    subprocess.run([sys.executable, "-c", script], timeout=60, check=True)


def test_many_batch_operations_metric() -> None:
    metrics = RecordingMetrics()
    pool = FakeObjectstore()
    session = _session(pool, metrics=metrics)

    session.many(
        [Get("a"), Get("b"), Put(b"x", key="c"), Delete("d"), Head("e")],
        concurrency=1,
    ).raise_for_failures()

    counts = {
        (tags or {}).get("operation"): value
        for name, value, tags in metrics.counters
        if name == "storage.batch.operations"
    }
    assert counts == {"get": 2, "insert": 1, "delete": 1, "head": 1}
    assert all(
        (tags or {}).get("usecase") == "testing" for _, _, tags in metrics.counters
    )


def test_many_batch_latency_metric() -> None:
    metrics = RecordingMetrics()
    session = _session(FakeObjectstore(), metrics=metrics)

    session.many([Get(f"key-{index}") for index in range(5)]).raise_for_failures()

    (latency,) = [m for m in metrics.distributions if m[0] == "storage.batch.latency"]
    assert (latency[2] or {})["operations"] == "4-7"


def test_batch_size_bucket() -> None:
    assert [batch_size_bucket(count) for count in (0, 1, 2, 3, 4, 7, 8, 1000)] == [
        "0",
        "1",
        "2-3",
        "2-3",
        "4-7",
        "4-7",
        "8-15",
        "512-1023",
    ]


def test_many_individual_insert_metrics() -> None:
    metrics = RecordingMetrics()
    pool = FakeObjectstore()
    session = _session(pool, metrics=metrics)

    session.many(
        [Put(b"x" * (2 * MAX_BATCH_PART_SIZE), key="big")]
    ).raise_for_failures()

    names = {name for name, _, _ in metrics.distributions}
    assert "storage.put.latency" in names
    # Nothing went through the batch endpoint, so it is not counted as a batch.
    assert "storage.batch.latency" not in names
    assert metrics.counters == []


def test_parse_status() -> None:
    assert _parse_status("200 OK") == 200
    assert _parse_status("404 Not Found") == 404
    assert _parse_status("204") == 204
    assert _parse_status(None) is None
    assert _parse_status("") is None
    assert _parse_status("garbage") is None
