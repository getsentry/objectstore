"""Unit tests for batch (``many``) operations.

Since these tests cannot spin up the real Objectstore server, they exercise the
full encode → parse → correlate path against an in-memory fake connection pool
that speaks the same ``objects:batch`` multipart protocol as the server. This
validates request serialization, response parsing, and result correlation
without a live backend (end-to-end behavior is covered by the Rust e2e suite).
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import unquote

import pytest
import zstandard
from objectstore_client import Client, Usecase
from objectstore_client.errors import RequestError
from objectstore_client.many import (
    DeleteResult,
    ErrorResult,
    GetResult,
    HeadResult,
    PutResult,
    _extract_boundary,
    _InsertOp,
    _iter_batches,
    _parse_multipart_response,
    _parse_status,
    _zstd_compress_bound,
)

BATCH_OP_HEADERS = {
    "x-sn-batch-operation-kind",
    "x-sn-batch-operation-key",
    "x-sn-batch-operation-index",
    "x-sn-batch-operation-status",
    "content-disposition",
}

_STATUS_REASONS = {
    200: "OK",
    201: "Created",
    204: "No Content",
    403: "Forbidden",
    404: "Not Found",
}


def _status_line(code: int) -> str:
    return f"{code} {_STATUS_REASONS.get(code, '')}".strip()


class FakeResponse:
    def __init__(self, status: int, headers: dict[str, str], data: bytes) -> None:
        self.status = status
        self.headers = headers
        self.data = data

    def read(self) -> bytes:
        return self.data


def _serialize_response(
    parts: list[tuple[dict[str, str], bytes]], boundary: str
) -> bytes:
    """Serializes response parts exactly as the server's multipart writer does."""
    delimiter = b"--" + boundary.encode()
    out = bytearray()
    for headers, body in parts:
        out += delimiter + b"\r\n"
        for name, value in headers.items():
            out += f"{name}: {value}".encode() + b"\r\n"
        out += b"\r\n"
        out += body + b"\r\n"
    out += delimiter + b"--"
    return bytes(out)


class FakeBatchPool:
    """A minimal in-memory stand-in for the server's ``objects:batch`` endpoint."""

    def __init__(self, *, deny_writes: bool = False) -> None:
        self.headers: dict[str, str] = {}
        self.store: dict[str, tuple[dict[str, str], bytes]] = {}
        self.deny_writes = deny_writes
        self._generated = 0

    def request(
        self,
        method: str,
        url: str,
        *,
        body: bytes,
        headers: dict[str, str],
        **_kwargs: Any,
    ) -> FakeResponse:
        assert method == "POST"
        assert "objects:batch" in url
        boundary = _extract_boundary(headers["Content-Type"])
        request_parts = _parse_multipart_response(body, boundary)

        response_parts: list[tuple[dict[str, str], bytes]] = []
        for index, (part_headers, part_body) in enumerate(request_parts):
            response_parts.append(self._handle(index, part_headers, part_body))

        response_boundary = "os-boundary-" + "0" * 32
        data = _serialize_response(response_parts, response_boundary)
        return FakeResponse(
            200,
            {"content-type": f'multipart/form-data; boundary="{response_boundary}"'},
            data,
        )

    def _handle(
        self, index: int, headers: dict[str, str], body: bytes
    ) -> tuple[dict[str, str], bytes]:
        kind = headers["x-sn-batch-operation-kind"]
        key_enc = headers.get("x-sn-batch-operation-key")
        key = unquote(key_enc) if key_enc else None

        base = {
            "x-sn-batch-operation-index": str(index),
            "x-sn-batch-operation-kind": kind,
        }

        def with_key(resp: dict[str, str], k: str) -> dict[str, str]:
            from urllib.parse import quote

            resp["x-sn-batch-operation-key"] = quote(k, safe="")
            return resp

        if kind == "insert":
            if self.deny_writes:
                resp = dict(base)
                resp["x-sn-batch-operation-status"] = _status_line(403)
                if key is not None:
                    with_key(resp, key)
                return resp, json.dumps({"detail": "forbidden"}).encode()
            if key is None:
                self._generated += 1
                key = f"generated-{self._generated}"
            meta = {k: v for k, v in headers.items() if k not in BATCH_OP_HEADERS}
            self.store[key] = (meta, body)
            resp = with_key(dict(base), key)
            resp["x-sn-batch-operation-status"] = _status_line(201)
            return resp, b""

        assert key is not None
        if kind == "delete":
            if self.deny_writes:
                resp = with_key(dict(base), key)
                resp["x-sn-batch-operation-status"] = _status_line(403)
                return resp, json.dumps({"detail": "forbidden"}).encode()
            self.store.pop(key, None)
            resp = with_key(dict(base), key)
            resp["x-sn-batch-operation-status"] = _status_line(204)
            return resp, b""

        if kind == "get":
            resp = with_key(dict(base), key)
            if key not in self.store:
                resp["x-sn-batch-operation-status"] = _status_line(404)
                return resp, b""
            meta, stored = self.store[key]
            resp["x-sn-batch-operation-status"] = _status_line(200)
            resp["x-sn-time-created"] = "2024-01-01T00:00:00+00:00"
            resp.update(meta)
            resp.setdefault("content-type", "application/octet-stream")
            return resp, stored

        # head
        resp = with_key(dict(base), key)
        if key not in self.store:
            resp["x-sn-batch-operation-status"] = _status_line(404)
            return resp, b""
        meta, _stored = self.store[key]
        resp["x-sn-batch-operation-status"] = _status_line(204)
        resp["x-sn-time-created"] = "2024-01-01T00:00:00+00:00"
        resp.update(meta)
        return resp, b""


def _session(pool: FakeBatchPool) -> Any:
    client = Client("http://localhost:8888")
    client._pool = pool  # type: ignore[assignment]
    return client.session(Usecase("testing", compression="none"), org=42, project=1337)


# --- Full round-trip tests -----------------------------------------------------


def test_batch_put_get_delete_head_roundtrip() -> None:
    pool = FakeBatchPool()
    session = _session(pool)

    results = (
        session.many()
        .put(b"first", key="key-1")
        .put(b"second", key="key-2", compression="zstd", content_type="text/plain")
        .put(b"third", key="key-3", filename="report.pdf", metadata={"foo": "bar"})
        .send()
    )
    assert len(results) == 3
    assert all(isinstance(r, PutResult) and r.error is None for r in results)
    assert {r.key for r in results} == {"key-1", "key-2", "key-3"}

    results = (
        session.many()
        .get("key-1")
        .get("key-2")  # stored zstd-compressed, transparently decompressed
        .head("key-3")
        .delete("key-1")
        .get("missing")
        .send()
    )
    get1 = next(r for r in results if isinstance(r, GetResult) and r.key == "key-1")
    assert get1.error is None and get1.response is not None
    assert get1.response.payload.read() == b"first"

    get2 = next(r for r in results if isinstance(r, GetResult) and r.key == "key-2")
    assert get2.response is not None
    assert get2.response.metadata.compression is None  # decompressed
    assert get2.response.payload.read() == b"second"

    head3 = next(r for r in results if isinstance(r, HeadResult))
    assert head3.metadata is not None
    assert head3.metadata.filename == "report.pdf"
    assert head3.metadata.custom.get("foo") == "bar"

    delete1 = next(r for r in results if isinstance(r, DeleteResult))
    assert delete1.error is None

    missing = next(
        r for r in results if isinstance(r, GetResult) and r.key == "missing"
    )
    assert missing.response is None and missing.error is None


def test_batch_get_accept_encoding_passthrough() -> None:
    pool = FakeBatchPool()
    session = _session(pool)

    session.many().put(b"payload", key="c", compression="zstd").send()

    results = session.many().get("c", accept_encoding=["zstd"]).send()
    get = results[0]
    assert isinstance(get, GetResult) and get.response is not None
    # Passthrough: still compressed, metadata preserved.
    assert get.response.metadata.compression == "zstd"
    raw = get.response.payload.read()
    assert zstandard.ZstdDecompressor().decompress(raw) == b"payload"


def test_batch_keyless_insert_gets_server_key() -> None:
    pool = FakeBatchPool()
    session = _session(pool)

    results = session.many().put(b"no key here").send()
    assert len(results) == 1
    put = results[0]
    assert isinstance(put, PutResult)
    assert put.error is None
    assert put.key.startswith("generated-")
    assert put.key in pool.store


def test_batch_partial_failures() -> None:
    pool = FakeBatchPool(deny_writes=True)
    session = _session(pool)
    # Seed an object directly so the read succeeds.
    pool.store["exists"] = ({"content-type": "application/octet-stream"}, b"hi")

    results = (
        session.many()
        .put(b"denied", key="write-key")
        .delete("delete-key")
        .get("exists")
        .send()
    )

    put = next(r for r in results if isinstance(r, PutResult))
    assert isinstance(put.error, RequestError)
    assert put.error.status == 403

    delete = next(r for r in results if isinstance(r, DeleteResult))
    assert isinstance(delete.error, RequestError)
    assert delete.error.status == 403

    get = next(r for r in results if isinstance(r, GetResult))
    assert get.error is None and get.response is not None
    assert get.response.payload.read() == b"hi"


def test_raise_for_failures() -> None:
    pool = FakeBatchPool(deny_writes=True)
    session = _session(pool)

    results = session.many().put(b"x", key="k1").put(b"y", key="k2").send()
    assert len(results.failures()) == 2

    with pytest.raises(ExceptionGroup) as exc_info:
        results.raise_for_failures()
    assert len(exc_info.value.exceptions) == 2


def test_batch_request_failure_produces_per_op_errors() -> None:
    """A whole-batch HTTP failure maps to one error per enqueued operation."""

    class FailingPool:
        headers: dict[str, str] = {}

        def request(self, *_args: Any, **_kwargs: Any) -> FakeResponse:
            return FakeResponse(500, {}, b"boom")

    session = _session(FakeBatchPool())
    session._pool = FailingPool()  # type: ignore[assignment]

    results = session.many().get("a").delete("b").send()
    assert len(results) == 2
    assert all(isinstance(r.error, RequestError) for r in results)  # type: ignore[union-attr]
    assert {r.error.status for r in results} == {500}  # type: ignore[union-attr]


# --- Classification / chunking -------------------------------------------------


def test_partition_oversized_insert_is_individual() -> None:
    pool = FakeBatchPool()
    session = _session(pool)

    builder = (
        session.many()
        .put(b"small", key="small")
        .put(b"x" * (2 * 1024 * 1024), key="big", compression="none")
        .get("g")
    )
    batchable, individual = builder._partition()

    assert len(individual) == 1
    assert isinstance(individual[0], _InsertOp)
    assert individual[0].key == "big"
    # small insert + get are batchable
    assert {op.key for op, _ in batchable} == {"small", "g"}  # type: ignore[union-attr]


def test_partition_unseekable_stream_is_individual() -> None:
    import io

    class Unseekable(io.BytesIO):
        def seekable(self) -> bool:
            return False

    pool = FakeBatchPool()
    session = _session(pool)
    builder = session.many().put(Unseekable(b"data"), key="stream")
    batchable, individual = builder._partition()
    assert not batchable
    assert len(individual) == 1


def test_iter_batches_count_limit() -> None:
    from objectstore_client.many import _DeleteOp

    ops: list[tuple[Any, int]] = [(_DeleteOp(f"k{i}"), 0) for i in range(1001)]
    batches = list(_iter_batches(ops))
    assert [len(b) for b in batches] == [1000, 1]


def test_iter_batches_size_limit() -> None:
    from objectstore_client.many import MAX_BATCH_BODY_SIZE, _DeleteOp

    one_mb = 1024 * 1024
    ops: list[tuple[Any, int]] = [(_DeleteOp(f"k{i}"), one_mb) for i in range(150)]
    batches = list(_iter_batches(ops))
    per_batch = MAX_BATCH_BODY_SIZE // one_mb
    assert len(batches) > 1
    for batch in batches[:-1]:
        assert len(batch) == per_batch


# --- Helper functions ----------------------------------------------------------


def test_zstd_compress_bound_is_upper_bound() -> None:
    for size in (0, 1, 1000, 100_000, 5_000_000):
        payload = b"a" * size
        actual = len(zstandard.ZstdCompressor().compress(payload))
        assert actual <= _zstd_compress_bound(size)


def test_parse_status() -> None:
    assert _parse_status("200 OK") == 200
    assert _parse_status("404 Not Found") == 404
    assert _parse_status("204") == 204
    assert _parse_status(None) is None
    assert _parse_status("garbage") is None


def test_extract_boundary() -> None:
    assert (
        _extract_boundary('multipart/form-data; boundary="os-boundary-abc"')
        == "os-boundary-abc"
    )
    assert _extract_boundary("multipart/form-data; boundary=plain") == "plain"
    with pytest.raises(RequestError):
        _extract_boundary("application/json")


def test_parse_multipart_response_roundtrip() -> None:
    boundary = "os-boundary-xyz"
    parts = [
        ({"x-sn-batch-operation-index": "0", "content-type": "text/plain"}, b"hello"),
        ({"x-sn-batch-operation-index": "1"}, b""),
        ({"x-sn-batch-operation-index": "2"}, b"\x00\x01\xff binary \r\n data"),
    ]
    body = _serialize_response(parts, boundary)
    parsed = _parse_multipart_response(body, boundary)

    assert len(parsed) == 3
    assert parsed[0][0]["x-sn-batch-operation-index"] == "0"
    assert parsed[0][0]["content-type"] == "text/plain"
    assert parsed[0][1] == b"hello"
    assert parsed[1][1] == b""
    assert parsed[2][1] == b"\x00\x01\xff binary \r\n data"


def test_unattributed_error_result_for_bad_index() -> None:
    boundary = "os-boundary-xyz"
    parts = [({"x-sn-batch-operation-status": "200 OK"}, b"")]  # no index header
    body = _serialize_response(parts, boundary)

    pool = FakeBatchPool()

    class BadIndexPool(FakeBatchPool):
        def request(self, *_args: Any, **_kwargs: Any) -> FakeResponse:
            return FakeResponse(
                200,
                {"content-type": f'multipart/form-data; boundary="{boundary}"'},
                body,
            )

    session = _session(pool)
    session._pool = BadIndexPool()  # type: ignore[assignment]

    results = session.many().get("k").send()
    # one unattributed error + one synthesized missing-response error
    assert any(isinstance(r, ErrorResult) for r in results)
    assert any(isinstance(r, GetResult) and r.error is not None for r in results)
