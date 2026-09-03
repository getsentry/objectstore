"""
Tests for batch ("many") operations.

These tests exercise the full encode → send → parse → correlate path against an
in-memory stand-in for the server that speaks the same ``objects:batch``
protocol, so request serialization, response parsing, and result correlation are
covered without a live backend. The real thing is covered by ``test_e2e.py``.
"""

from __future__ import annotations

import io
import json
import pathlib
import queue
import resource
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
from objectstore_client.errors import RequestError
from objectstore_client.formdata import (
    MultipartBody,
    RequestPart,
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
    _part_body,
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
    """An in-memory stand-in for the endpoints the many API talks to."""

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


def test_many_get_passes_through_accepted_encoding() -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    session.many([Put(b"payload", key="k", compress="zstd")]).raise_for_failures()

    (result,) = list(session.many([Get("k", accept_encoding=["zstd"])]))
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


def test_many_applies_the_usecase_expiration_policy() -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    session.many([Put(b"payload", key="k")]).raise_for_failures()

    (part,) = pool.batches[0]
    assert part.headers["x-sn-expiration"] == "ttl:1d"


def test_many_reports_per_operation_failures() -> None:
    pool = FakeObjectstore(deny_writes=True)
    pool.store["exists"] = ({}, b"hi")
    session = _session(pool)

    results = _by_key(
        list(session.many([Put(b"denied", key="k1"), Delete("k2"), Get("exists")]))
    )

    put = results["k1"]
    assert isinstance(put, PutResult) and isinstance(put.error, RequestError)
    assert put.error.status == 403
    assert "forbidden" in put.error.response

    delete = results["k2"]
    assert isinstance(delete, DeleteResult) and isinstance(delete.error, RequestError)
    assert delete.error.status == 403

    get = results["exists"]
    assert isinstance(get, GetResult) and get.error is None


def test_many_raise_for_failures() -> None:
    pool = FakeObjectstore(deny_writes=True)
    session = _session(pool)

    results = session.many([Put(b"x", key="k1"), Put(b"y", key="k2")])
    with pytest.raises(ExceptionGroup) as exc_info:
        results.raise_for_failures()
    assert len(exc_info.value.exceptions) == 2


def test_many_failures_returns_only_failed_results() -> None:
    pool = FakeObjectstore(deny_writes=True)
    pool.store["exists"] = ({}, b"hi")
    session = _session(pool)

    failures = session.many(
        [Put(b"x", key="k1"), Get("exists"), Delete("k2")]
    ).failures()

    assert sorted(failure.key for failure in failures) == ["k1", "k2"]  # type: ignore[union-attr]
    assert all(failure.error is not None for failure in failures)


def test_many_fans_a_failed_batch_out_to_its_operations() -> None:
    class FailingPool(FakeObjectstore):
        def request(self, *_args: Any, **_kwargs: Any) -> FakeResponse:
            return FakeResponse(500, {}, b"boom")

    session = _session(FailingPool())

    results = list(session.many([Get("a"), Delete("b")]))
    assert len(results) == 2
    for result in results:
        assert isinstance(result.error, RequestError)
        assert result.error.status == 500
    assert {result.key for result in results} == {"a", "b"}  # type: ignore[union-attr]


def test_many_reports_operations_the_server_skipped() -> None:
    class ForgetfulPool(FakeObjectstore):
        def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
            response = super()._batch(body, headers)
            # Reply to the first operation only.
            parts = list(
                iter_multipart(response.headers["content-type"], [response.data])
            )
            boundary = "os-boundary-" + "0" * 32
            return FakeResponse(
                200,
                {"content-type": f'multipart/form-data; boundary="{boundary}"'},
                _serialize_response(parts[:1], boundary),
            )

    session = _session(ForgetfulPool())
    results = _by_key(list(session.many([Get("a"), Get("b")], concurrency=1)))

    assert results["b"].error is None
    assert isinstance(results["a"].error, RequestError)
    assert "did not return a response" in str(results["a"].error)


def test_many_reports_unattributable_response_parts() -> None:
    class AnonymousPool(FakeObjectstore):
        def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
            boundary = "os-boundary-" + "0" * 32
            part = ResponsePart({"x-sn-batch-operation-status": "200 OK"}, b"")
            return FakeResponse(
                200,
                {"content-type": f'multipart/form-data; boundary="{boundary}"'},
                _serialize_response([part], boundary),
            )

    session = _session(AnonymousPool())
    results = list(session.many([Get("a")]))

    # One error for the part that cannot be attributed, and one for the
    # operation that consequently never got a response.
    assert len(results) == 2
    assert any(isinstance(result, ErrorResult) for result in results)
    unanswered = next(result for result in results if isinstance(result, GetResult))
    assert isinstance(unanswered.error, RequestError)


class TruncatingPool(FakeObjectstore):
    """A server whose response ends part-way through, as a dropped connection would."""

    def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
        response = super()._batch(body, headers)
        data = response.data[: len(response.data) - 20]
        return FakeResponse(200, response.headers, data)


def test_many_keeps_the_results_of_a_broken_response() -> None:
    """Operations that answered keep their result; the rest inherit the error."""
    pool = TruncatingPool()  # responds in reverse order
    pool.store["a"] = ({}, b"payload")
    pool.store["b"] = ({}, b"payload")
    session = _session(pool)

    results = list(session.many([Get("a"), Get("b")], concurrency=1))

    by_key = _by_key(results)
    assert len(results) == 2
    assert by_key["b"].error is None
    assert by_key["a"].error is not None


@pytest.mark.parametrize("name", ["a\r\nb", "not a header"])
def test_many_rejects_unrepresentable_metadata(name: str) -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    # A metadata *name* cannot be escaped the way a value can, so this operation
    # fails on its own without taking the rest of the batch down.
    results = list(
        session.many(
            [
                Put(b"x", key="bad", metadata={name: "c"}),
                Put(b"y", key="good-1"),
                Put(b"z", key="good-2"),
            ],
            concurrency=1,
        )
    )

    failed = next(result for result in results if result.error is not None)
    assert isinstance(failed, PutResult) and isinstance(failed.error, ValueError)
    assert failed.key == "bad"
    assert "good-1" in pool.store and "good-2" in pool.store
    # Dropping the first operation shifts the rest by one on the wire, so their
    # results have to report the index they had in the input.
    assert {result.index: result.key for result in results} == {  # type: ignore[union-attr]
        0: "bad",
        1: "good-1",
        2: "good-2",
    }


def test_put_rejects_conflicting_compression() -> None:
    with pytest.raises(ValueError):
        Put(b"x", compress="zstd", precompressed="zstd")
    with pytest.raises(ValueError):
        Put(b"x", compress="gzip")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Put(b"x", precompressed="none")


def test_many_reports_the_index_of_each_operation() -> None:
    """The server answers in completion order, so results carry their index."""
    pool = FakeObjectstore()  # responds in reverse order
    session = _session(pool)
    keys = [f"key-{index}" for index in range(5)]

    results = list(session.many([Get(key) for key in keys], concurrency=1))

    assert [result.key for result in results] != keys  # type: ignore[union-attr]
    assert {result.index: result.key for result in results} == dict(  # type: ignore[union-attr]
        enumerate(keys)
    )


def test_many_correlates_keyless_puts_by_index() -> None:
    """A keyless put has no key of its own, so the index is the only handle."""
    pool = FakeObjectstore()
    session = _session(pool)
    payloads = [f"payload-{index}".encode() for index in range(4)]

    results = list(session.many([Put(payload) for payload in payloads], concurrency=2))

    assert len(results) == len(payloads)
    for result in results:
        assert isinstance(result, PutResult) and result.error is None
        assert pool.store[result.key][1] == payloads[result.index]


def test_many_batches_up_to_the_operation_count_limit() -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    ops: list[Operation] = [
        Delete(f"key-{index}") for index in range(MAX_BATCH_OPS + 1)
    ]
    results = list(session.many(ops, concurrency=1))

    assert len(results) == MAX_BATCH_OPS + 1
    assert [len(batch) for batch in pool.batches] == [MAX_BATCH_OPS, 1]


def test_iter_work_splits_on_the_body_size_limit() -> None:
    one_mb = 1024 * 1024
    classified = [(index, Delete(f"key-{index}"), one_mb) for index in range(150)]

    batches = list(_iter_work(classified))

    per_batch = MAX_BATCH_BODY_SIZE // one_mb
    assert [len(item.ops) for item in batches] == [per_batch, 150 - per_batch]  # type: ignore[union-attr]


def test_iter_work_keeps_a_single_batch_at_the_size_limit() -> None:
    one_mb = 1024 * 1024
    classified = [(index, Delete(f"key-{index}"), one_mb) for index in range(100)]
    assert len(list(_iter_work(classified))) == 1


def test_iter_work_never_drops_an_oversized_operation() -> None:
    """An operation larger than a whole batch still gets a batch of its own."""
    classified = [(0, Delete("k"), MAX_BATCH_BODY_SIZE + 1)]
    batches = list(_iter_work(classified))
    assert [len(item.ops) for item in batches] == [1]  # type: ignore[union-attr]


def test_iter_work_flushes_the_pending_batch_before_an_individual_request() -> None:
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


def test_classify_uses_the_worst_case_compressed_size() -> None:
    session = _session(FakeObjectstore(), compression="zstd")

    # The largest payload whose worst-case compressed size still fits a part,
    # matching the boundary the Rust client tests.
    size = 1_044_496
    assert _zstd_compress_bound(size) == MAX_BATCH_PART_SIZE
    batchable = _classify(session, Put(b"a" * size, key="k"))
    assert batchable == MAX_BATCH_PART_SIZE

    unbatchable = _classify(session, Put(b"a" * (size + 1), key="k"))
    assert unbatchable is None


def test_classify_uses_the_exact_size_of_a_precompressed_body() -> None:
    session = _session(FakeObjectstore(), compression="zstd")

    # A size that the worst-case bound would push over the limit, but which is
    # sent verbatim because the payload is already compressed.
    payload = b"a" * MAX_BATCH_PART_SIZE
    assert _zstd_compress_bound(len(payload)) > MAX_BATCH_PART_SIZE

    size = _classify(session, Put(payload, key="k", precompressed="zstd"))
    assert size == MAX_BATCH_PART_SIZE


def test_classify_uses_the_exact_size_when_not_compressing() -> None:
    session = _session(FakeObjectstore(), compression="zstd")
    size = _classify(session, Put(b"a" * 1000, key="k", compress="none"))
    assert size == 1000


def test_classify_measures_the_remainder_of_a_stream() -> None:
    session = _session(FakeObjectstore())
    stream = io.BytesIO(b"a" * 1000)
    stream.seek(400)

    size = _classify(session, Put(stream, key="k"))
    assert size == 600


def test_classify_cannot_size_an_unseekable_stream() -> None:
    class Unseekable(io.BytesIO):
        def seekable(self) -> bool:
            return False

    session = _session(FakeObjectstore())
    size = _classify(session, Put(Unseekable(b"data"), key="k"))
    assert size is None


def test_classify_rejects_foreign_operations() -> None:
    session = _session(FakeObjectstore())
    with pytest.raises(TypeError):
        _classify(session, "not an operation")  # type: ignore[arg-type]


def test_zstd_compress_bound_is_an_upper_bound() -> None:
    for size in (0, 1, 1000, 100_000, 5_000_000):
        payload = b"a" * size
        streamed = zstandard.ZstdCompressor().stream_reader(io.BytesIO(payload)).read()
        assert len(streamed) <= _zstd_compress_bound(size)
        assert len(
            zstandard.ZstdCompressor().compress(payload)
        ) <= _zstd_compress_bound(size)


def test_many_sends_an_oversized_insert_individually() -> None:
    pool = FakeObjectstore()
    session = _session(pool)
    big = b"x" * (2 * MAX_BATCH_PART_SIZE)

    results = list(session.many([Put(big, key="big"), Put(b"small", key="small")]))

    assert all(result.error is None for result in results)
    assert pool.individual == ["big"]
    assert [len(batch) for batch in pool.batches] == [1]
    assert pool.store["big"][1] == big


def test_many_sends_an_unseekable_stream_individually() -> None:
    class Unseekable(io.BytesIO):
        def seekable(self) -> bool:
            return False

    pool = FakeObjectstore()
    session = _session(pool)

    (result,) = list(session.many([Put(Unseekable(b"data"), key="stream")]))

    assert result.error is None
    assert pool.individual == ["stream"]
    assert pool.store["stream"][1] == b"data"


def test_many_keeps_an_individual_insert_compressed() -> None:
    pool = FakeObjectstore()
    session = _session(pool, compression="zstd")
    big = b"x" * (4 * MAX_BATCH_PART_SIZE)

    (result,) = list(session.many([Put(big, key="big")]))

    assert result.error is None
    metadata, stored = pool.store["big"]
    assert metadata["content-encoding"] == "zstd"
    assert zstandard.ZstdDecompressor().stream_reader(io.BytesIO(stored)).read() == big


def test_many_streams_a_batched_stream_body() -> None:
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


def test_many_leaves_the_callers_stream_open() -> None:
    pool = FakeObjectstore()
    session = _session(pool, compression="zstd")
    source = io.BytesIO(b"payload")

    session.many([Put(source, key="k")]).raise_for_failures()

    # The compressing reader wrapped around it is ours to close, the stream isn't.
    assert not source.closed


def test_many_holds_one_compressor_at_a_time() -> None:
    """Retaining a compressor per part would cost roughly a megabyte each."""
    parts = [
        RequestPart({"x-i": str(index)}, _part_body(b"x" * 1024, "zstd"))
        for index in range(200)
    ]

    before = _peak_rss()
    written = sum(len(chunk) for chunk in MultipartBody(parts))
    growth = _peak_rss() - before

    assert written > 0
    # Well under the ~200 MB that retaining every compressor would take, and
    # comfortably above one context, so the bound does not depend on the exact
    # size of a zstd context.
    assert growth < 50 * 1024 * 1024, f"grew by {growth} bytes"


def _peak_rss() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _RSS_UNIT


# `ru_maxrss` is bytes on macOS and kilobytes on Linux.
_RSS_UNIT = 1 if sys.platform == "darwin" else 1024


def test_zstd_body_releases_the_compressor_at_the_end_of_the_source() -> None:
    source = io.BytesIO(b"payload" * 100)
    body = _ZstdBody(source)

    compressed = b""
    while chunk := body.read(8):
        compressed += chunk

    assert (
        zstandard.ZstdDecompressor().stream_reader(io.BytesIO(compressed)).read()
        == b"payload" * 100
    )
    # The compressor is gone once the source is exhausted, but the source, which
    # belongs to the caller, is left open.
    assert body._reader is None
    assert not source.closed
    # A stream at the end of its input is at EOF, not closed, so reading on
    # returns empty bytes the way any file object does.
    assert not body.closed
    assert body.read(8) == b""


def test_zstd_body_can_be_released_before_the_source_is_exhausted() -> None:
    source = io.BytesIO(b"payload" * 100)
    body = _ZstdBody(source)
    body.read(8)

    body.close()

    assert body._reader is None
    assert body.closed
    assert not source.closed
    # Reading a closed stream is an error, as it is for any file object.
    with pytest.raises(ValueError):
        body.read(8)


def test_many_forbids_resending_a_streamed_request_body() -> None:
    pool = FakeObjectstore()
    session = _session(pool, compression="zstd")

    session.many([Put(io.BytesIO(b"streamed"), key="k")]).raise_for_failures()

    # Connect retries stay on: they happen before the body is read.
    (retries,) = pool.retries_used
    assert (retries.read, retries.other, retries.redirect) == (0, 0, 0)
    assert retries.connect == 3


def test_many_leaves_the_pool_policy_alone_for_a_bytes_request_body() -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    session.many([Put(b"payload", key="k")]).raise_for_failures()

    assert pool.retries_used == [None]


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


def test_many_returns_the_connection_of_a_drained_batch() -> None:
    pool = FakeObjectstore()
    session = _session(pool)

    list(session.many([Get("a"), Get("b")], concurrency=1))

    (response,) = pool.responses
    assert response.released and not response.closed


def test_many_closes_the_connection_of_an_abandoned_batch() -> None:
    pool = FakeObjectstore()
    pool.store["a"] = ({}, b"payload")
    session = _session(pool)

    with session.many([Get("a"), Get("b")], concurrency=1) as results:
        next(iter(results))

    # The response was abandoned before its body was read to the end, so the
    # connection cannot be returned to the pool.
    (response,) = pool.responses
    assert response.closed and not response.released


def test_many_can_skip_decompression() -> None:
    pool = FakeObjectstore()
    session = _session(pool)
    session.many([Put(b"payload", key="k", compress="zstd")]).raise_for_failures()

    (result,) = list(session.many([Get("k", decompress=False)]))

    assert isinstance(result, GetResult) and result.response is not None
    assert result.response.metadata.compression == "zstd"
    raw = result.response.payload.read()
    assert (
        zstandard.ZstdDecompressor().stream_reader(io.BytesIO(raw)).read() == b"payload"
    )


def test_many_accepts_no_operations() -> None:
    pool = FakeObjectstore()
    session = _session(pool)
    assert list(session.many([])) == []
    assert pool.requests == 0


def test_many_runs_on_the_calling_thread_at_concurrency_one() -> None:
    """`concurrency=1` opts out of threads altogether, not just out of overlap."""
    threads: list[int] = []

    class RecordingPool(FakeObjectstore):
        def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
            threads.append(threading.get_ident())
            return super()._batch(body, headers)

    session = _session(RecordingPool())
    session.many([Get("a"), Delete("b")], concurrency=1).raise_for_failures()

    assert threads == [threading.get_ident()]


def test_many_releases_workers_when_the_results_are_abandoned() -> None:
    """Abandoning the results must not leave a worker holding a response."""
    pool = FakeObjectstore()
    for index in range(50):
        pool.store[f"key-{index}"] = ({}, b"payload")
    session = _session(pool)

    # One batch of fifty, so the worker still has results to hand over — and is
    # parked on the queue — when the caller walks away after the first one.
    results = session.many([Get(f"key-{i}") for i in range(50)], concurrency=2)
    next(iter(results))
    results.close()

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if all(response.closed for response in pool.responses):
            break
        time.sleep(0.05)
    assert all(response.closed for response in pool.responses)


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


def test_many_stops_dispatching_work_when_abandoned() -> None:
    class Unseekable(io.BytesIO):
        def seekable(self) -> bool:
            return False

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


def test_fit_to_connection_pool_raises_the_idle_connection_cap() -> None:
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


def test_fit_to_connection_pool_clamps_to_a_blocking_pool() -> None:
    """`block=True` makes the pool size a deliberate cap on connections."""
    client = Client(
        "http://localhost:8888", connection_kwargs={"maxsize": 2, "block": True}
    )
    session = client.session(Usecase("testing", compression="none"), org=42)

    assert _fit_to_connection_pool(session, 8) == 2
    # Asking for less than the cap is still honoured, and the cap stays put.
    assert _fit_to_connection_pool(session, 1) == 1
    assert _pool_queue(client).maxsize == 2


def test_many_runs_on_the_calling_thread_for_a_blocking_pool_of_one() -> None:
    """Clamping to one request in flight takes the thread-free path."""
    threads: list[int] = []

    class RecordingPool(FakeObjectstore):
        block = True
        pool = queue.LifoQueue[Any](maxsize=1)

        def _batch(self, body: Any, headers: dict[str, str]) -> FakeResponse:
            threads.append(threading.get_ident())
            return super()._batch(body, headers)

    session = _session(RecordingPool())
    session.many([Get("a"), Delete("b")], concurrency=8).raise_for_failures()

    assert threads == [threading.get_ident()]


@contextmanager
def _initialized_sentry() -> Iterator[None]:
    """Sentry's integrations are only installed for an initialized SDK."""
    previous = sentry_sdk.get_global_scope().client
    sentry_sdk.init(dsn=None, traces_sample_rate=1.0)
    try:
        yield
    finally:
        sentry_sdk.get_global_scope().set_client(previous)


def test_many_propagates_the_callers_trace_to_pool_threads() -> None:
    """
    Trace headers come from a thread-local scope, which workers must inherit.

    Nothing here does that by hand: `ThreadingIntegration` patches
    `ThreadPoolExecutor.submit` to carry the submitting scope into the task.
    """

    class Unseekable(io.BytesIO):
        def seekable(self) -> bool:
            return False

    pool = FakeObjectstore()
    seen: list[str | None] = []

    def record(method: str, url: str, **kwargs: Any) -> FakeResponse:
        seen.append(kwargs["headers"].get("sentry-trace"))
        return FakeObjectstore.request(pool, method, url, **kwargs)

    pool.request = record  # type: ignore[method-assign]

    with _initialized_sentry():
        client = Client("http://localhost:8888", propagate_traces=True)
        client._pool = pool  # type: ignore[assignment]
        session = client.session(Usecase("testing", compression="none"), org=42)
        traceparent = sentry_sdk.get_current_scope().get_traceparent()

        # A batch request and an individual one, so both paths are covered.
        ops: list[Operation] = [Get("a"), Put(Unseekable(b"data"), key="b")]
        results = list(session.many(ops, concurrency=2))

    assert all(result.error is None for result in results)
    assert len(seen) == 2
    assert seen == [traceparent, traceparent]


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


def test_many_rejects_invalid_concurrency() -> None:
    session = _session(FakeObjectstore())
    for concurrency in (0, -1):
        with pytest.raises(ValueError):
            session.many([Get("a")], concurrency=concurrency)


def test_many_counts_the_operations_of_each_batch() -> None:
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


def test_many_labels_batch_latency_with_the_batch_size() -> None:
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


def test_many_reports_an_individual_insert_under_its_own_operation() -> None:
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
