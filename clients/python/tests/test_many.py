import io
from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
import zstandard
from objectstore_client.client import RequestError
from objectstore_client.many import (
    HEADER_BATCH_OP_INDEX,
    HEADER_BATCH_OP_KEY,
    HEADER_BATCH_OP_KIND,
    HEADER_BATCH_OP_STATUS,
    MAX_BATCH_BODY_SIZE,
    MAX_BATCH_OPS,
    MAX_BATCH_PART_SIZE,
    Delete,
    Get,
    Put,
    _classify,
    _iter_batches,
    _parse_batch_response,
    _prepare_put,
    _PreparedPut,
    execute_many,
)
from objectstore_client.metadata import TimeToLive
from objectstore_client.multipart import ResponsePart

# Shorthand for test ops lists: (batch_index, operation, prepared_put_or_none)
_Ops = list[tuple[int, Put | Get | Delete, _PreparedPut | None]]

# ---------------------------------------------------------------------------
# _prepare_put
# ---------------------------------------------------------------------------


def test_prepare_put_bytes_no_compression() -> None:
    op = Put(contents=b"hello world")
    prepared = _prepare_put(op, default_compression="none", default_expiration=None)
    assert prepared.body == b"hello world"
    assert "Content-Encoding" not in prepared.headers


def test_prepare_put_bytes_with_zstd() -> None:
    op = Put(contents=b"hello world")
    prepared = _prepare_put(op, default_compression="zstd", default_expiration=None)
    assert prepared.body != b"hello world"  # compressed
    assert prepared.headers["Content-Encoding"] == "zstd"


def test_prepare_put_io_materialized() -> None:
    data = b"stream data"
    op = Put(contents=io.BytesIO(data))
    prepared = _prepare_put(op, default_compression="none", default_expiration=None)
    assert prepared.body == data


def test_prepare_put_explicit_compression_override() -> None:
    op = Put(contents=b"data", compression="none")
    prepared = _prepare_put(op, default_compression="zstd", default_expiration=None)
    assert prepared.body == b"data"
    assert "Content-Encoding" not in prepared.headers


def test_prepare_put_metadata_headers() -> None:
    from datetime import timedelta

    op = Put(
        contents=b"x",
        key="k",
        content_type="text/plain",
        metadata={"custom-key": "custom-val"},
        expiration_policy=TimeToLive(timedelta(days=1)),
        origin="127.0.0.1",
    )
    prepared = _prepare_put(op, default_compression="none", default_expiration=None)
    assert prepared.headers["Content-Type"] == "text/plain"
    assert prepared.headers["x-snme-custom-key"] == "custom-val"
    assert "x-sn-expiration" in prepared.headers
    assert prepared.headers["x-sn-origin"] == "127.0.0.1"


def test_prepare_put_default_expiration() -> None:
    from datetime import timedelta

    op = Put(contents=b"x")
    default_exp = TimeToLive(timedelta(hours=1))
    prepared = _prepare_put(
        op, default_compression="none", default_expiration=default_exp
    )
    assert "x-sn-expiration" in prepared.headers


def test_prepare_put_explicit_expiration_overrides_default() -> None:
    from datetime import timedelta

    explicit = TimeToLive(timedelta(days=7))
    default = TimeToLive(timedelta(hours=1))
    op = Put(contents=b"x", expiration_policy=explicit)
    prepared = _prepare_put(op, default_compression="none", default_expiration=default)
    assert "7 days" in prepared.headers["x-sn-expiration"]


# ---------------------------------------------------------------------------
# _classify
# ---------------------------------------------------------------------------


def test_classify_get_is_batchable() -> None:
    kind, size = _classify(Get("key"), body_size=0)
    assert kind == "batchable"
    assert size == 0


def test_classify_delete_is_batchable() -> None:
    kind, size = _classify(Delete("key"), body_size=0)
    assert kind == "batchable"
    assert size == 0


def test_classify_small_put_is_batchable() -> None:
    kind, size = _classify(Put(b"x"), body_size=100)
    assert kind == "batchable"
    assert size == 100


def test_classify_large_put_is_individual() -> None:
    kind, size = _classify(Put(b"x"), body_size=MAX_BATCH_PART_SIZE + 1)
    assert kind == "individual"
    assert size == MAX_BATCH_PART_SIZE + 1


def test_classify_put_at_exact_limit_is_batchable() -> None:
    kind, _ = _classify(Put(b"x"), body_size=MAX_BATCH_PART_SIZE)
    assert kind == "batchable"


# ---------------------------------------------------------------------------
# _iter_batches
# ---------------------------------------------------------------------------


def _batchable_op(i: int, size: int) -> tuple[int, Delete, None, int]:
    """Create a dummy batchable op 4-tuple for _iter_batches tests."""
    return (i, Delete("k"), None, size)


def test_iter_batches_empty() -> None:
    assert list(_iter_batches([])) == []


def test_iter_batches_single_batch_at_count_limit() -> None:
    ops = [_batchable_op(i, 1) for i in range(MAX_BATCH_OPS)]
    batches = list(_iter_batches(ops))
    assert len(batches) == 1
    assert len(batches[0]) == MAX_BATCH_OPS


def test_iter_batches_splits_on_count() -> None:
    ops = [_batchable_op(i, 1) for i in range(MAX_BATCH_OPS + 1)]
    batches = list(_iter_batches(ops))
    assert len(batches) == 2
    assert len(batches[0]) == MAX_BATCH_OPS
    assert len(batches[1]) == 1


def test_iter_batches_exactly_at_size_limit() -> None:
    op_size = 1024 * 1024  # 1 MB
    count = MAX_BATCH_BODY_SIZE // op_size  # 100
    ops = [_batchable_op(i, op_size) for i in range(count)]
    batches = list(_iter_batches(ops))
    assert len(batches) == 1
    assert len(batches[0]) == count


def test_iter_batches_splits_on_size() -> None:
    op_size = 1024 * 1024  # 1 MB
    count = MAX_BATCH_BODY_SIZE // op_size + 1  # 101
    ops = [_batchable_op(i, op_size) for i in range(count)]
    batches = list(_iter_batches(ops))
    assert len(batches) == 2
    assert len(batches[0]) == MAX_BATCH_BODY_SIZE // op_size
    assert len(batches[1]) == 1


def test_iter_batches_size_hits_before_count() -> None:
    op_size = 600 * 1024  # 600 KB
    ops = [_batchable_op(i, op_size) for i in range(200)]
    batches = list(_iter_batches(ops))
    per_batch = MAX_BATCH_BODY_SIZE // op_size
    assert len(batches) > 1
    for batch in batches[:-1]:
        assert len(batch) == per_batch


# ---------------------------------------------------------------------------
# _parse_batch_response
# ---------------------------------------------------------------------------


def _make_response_part(
    index: int,
    status: str,
    key: str,
    kind: str = "get",
    body: bytes = b"",
    extra_headers: dict[str, str] | None = None,
) -> ResponsePart:
    headers = {
        HEADER_BATCH_OP_INDEX: str(index),
        HEADER_BATCH_OP_STATUS: status,
        HEADER_BATCH_OP_KEY: key,
        HEADER_BATCH_OP_KIND: kind,
    }
    if extra_headers:
        headers.update(extra_headers)
    return ResponsePart(headers=headers, body=body)


def test_parse_successful_get() -> None:
    parts = [_make_response_part(0, "200 OK", "k1", body=b"payload")]
    ops: _Ops = [(0, Get("k1"), None)]
    results = list(_parse_batch_response(parts, ops))
    assert len(results) == 1
    idx, result = results[0]
    assert idx == 0
    assert result.key == "k1"
    # response should be a GetResponse
    from objectstore_client.client import GetResponse

    assert isinstance(result.response, GetResponse)
    assert result.response.payload.read() == b"payload"


def test_parse_successful_get_decompresses_streamed_zstd() -> None:
    raw = b"payload" * 100
    compressor = zstandard.ZstdCompressor()
    compressed = compressor.stream_reader(io.BytesIO(raw)).read()

    parts = [
        _make_response_part(
            0,
            "200 OK",
            "k1",
            body=compressed,
            extra_headers={"content-encoding": "zstd"},
        )
    ]
    ops: _Ops = [(0, Get("k1"), None)]
    results = list(_parse_batch_response(parts, ops))
    _, result = results[0]

    from objectstore_client.client import GetResponse

    assert isinstance(result.response, GetResponse)
    assert result.response.metadata.compression is None
    assert result.response.payload.read() == raw


def test_parse_get_not_found() -> None:
    parts = [_make_response_part(0, "404 Not Found", "k1")]
    ops: _Ops = [(0, Get("k1"), None)]
    results = list(_parse_batch_response(parts, ops))
    assert len(results) == 1
    _, result = results[0]
    assert result.response is None


def test_parse_successful_put() -> None:
    from objectstore_client.many import _PreparedPut

    prepared = _PreparedPut(key="k1", body=b"", headers={})
    parts = [_make_response_part(0, "200 OK", "k1", kind="insert")]
    ops = [(0, Put(b"data", key="k1"), prepared)]
    results = list(_parse_batch_response(parts, ops))
    assert len(results) == 1
    _, result = results[0]
    assert result.key == "k1"
    assert result.response is None


def test_parse_successful_delete() -> None:
    parts = [_make_response_part(0, "200 OK", "k1", kind="delete")]
    ops: _Ops = [(0, Delete("k1"), None)]
    results = list(_parse_batch_response(parts, ops))
    assert len(results) == 1
    _, result = results[0]
    assert result.key == "k1"
    assert result.response is None


def test_parse_per_operation_error() -> None:
    parts = [
        _make_response_part(
            0, "500 Internal Server Error", "k1", body=b"something broke"
        )
    ]
    ops: _Ops = [(0, Get("k1"), None)]
    results = list(_parse_batch_response(parts, ops))
    assert len(results) == 1
    _, result = results[0]
    assert isinstance(result.response, RequestError)
    assert result.response.status == 500


def test_parse_mixed_operations() -> None:
    from objectstore_client.many import _PreparedPut

    prepared = _PreparedPut(key=None, body=b"", headers={})
    parts = [
        _make_response_part(0, "200 OK", "assigned-key", kind="insert"),
        _make_response_part(1, "200 OK", "k2", kind="get", body=b"data"),
        _make_response_part(2, "200 OK", "k3", kind="delete"),
    ]
    ops: _Ops = [
        (5, Put(b"data"), prepared),
        (10, Get("k2"), None),
        (15, Delete("k3"), None),
    ]
    results = list(_parse_batch_response(parts, ops))
    assert len(results) == 3

    result_map = {idx: result for idx, result in results}
    assert result_map[5].key == "assigned-key"
    assert result_map[5].response is None
    assert result_map[10].key == "k2"
    assert result_map[15].key == "k3"


def test_parse_missing_response_part() -> None:
    # Server returns response for index 0 but not index 1
    parts = [_make_response_part(0, "200 OK", "k1")]
    ops: _Ops = [
        (0, Get("k1"), None),
        (1, Get("k2"), None),
    ]
    results = list(_parse_batch_response(parts, ops))
    assert len(results) == 2
    result_map = {idx: result for idx, result in results}
    assert result_map[0].response is not None  # success
    assert isinstance(result_map[1].response, RequestError)  # missing


# ---------------------------------------------------------------------------
# execute_many
# ---------------------------------------------------------------------------


def _make_mock_session(
    batch_response_parts: list[ResponsePart] | None = None,
) -> MagicMock:
    """Create a mock Session for testing execute_many."""
    session = MagicMock()
    session._usecase.name = "test-usecase"
    session._usecase._compression = "none"
    session._usecase._expiration_policy = None
    session._make_batch_url.return_value = "/v1/objects:batch/test-usecase/org=1/"
    session._make_headers.return_value = {}

    if batch_response_parts is not None:
        from objectstore_client.multipart import RequestPart, encode_multipart

        # Build a fake multipart response body
        fake_parts = [
            RequestPart(headers=p.headers, body=p.body) for p in batch_response_parts
        ]
        content_type, body_iter = encode_multipart(fake_parts)
        body = b"".join(body_iter)

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": content_type}
        # _send_batch uses preload_content=False and response.stream(chunk_size)
        mock_response.stream.return_value = iter([body])
        session._pool.request.return_value = mock_response

    return session


def test_execute_many_empty() -> None:
    session = _make_mock_session()
    results = list(execute_many(session, []))
    assert results == []


def test_execute_many_accepts_generator() -> None:
    """execute_many should accept a generator (consumed only once)."""
    response_parts = [_make_response_part(0, "200 OK", "k1", kind="delete")]
    session = _make_mock_session(response_parts)

    def ops_gen() -> Iterator[Delete]:
        yield Delete("k1")

    results = list(execute_many(session, ops_gen(), concurrency=1))
    assert len(results) == 1
    assert results[0].key == "k1"


def test_execute_many_returns_iterator() -> None:
    """execute_many should return an iterator, not a list."""
    response_parts = [_make_response_part(0, "200 OK", "k1", kind="delete")]
    session = _make_mock_session(response_parts)
    result = execute_many(session, [Delete("k1")], concurrency=1)
    assert hasattr(result, "__iter__") and hasattr(result, "__next__")


def test_execute_many_concurrency_zero_raises() -> None:
    session = _make_mock_session()
    with pytest.raises(ValueError, match="concurrency must be >= 1"):
        execute_many(session, [Get("k")], concurrency=0)


def test_execute_many_concurrency_negative_raises() -> None:
    session = _make_mock_session()
    with pytest.raises(ValueError, match="concurrency must be >= 1"):
        execute_many(session, [Get("k")], concurrency=-1)


def test_execute_many_single_batch_sequential() -> None:
    """Test a simple batch with concurrency=1."""
    response_parts = [
        _make_response_part(0, "200 OK", "k1", kind="get", body=b"hello"),
        _make_response_part(1, "200 OK", "k2", kind="delete"),
    ]
    session = _make_mock_session(response_parts)

    results = list(execute_many(session, [Get("k1"), Delete("k2")], concurrency=1))
    assert len(results) == 2
    assert results[0].key == "k1"
    assert results[1].key == "k2"
    assert results[1].response is None


def test_execute_many_preserves_order_sequential() -> None:
    """With concurrency=1, results arrive in input order."""
    response_parts = [
        _make_response_part(0, "200 OK", "k3", kind="delete"),
        _make_response_part(1, "200 OK", "k1", kind="get", body=b"data"),
        _make_response_part(2, "200 OK", "k2", kind="delete"),
    ]
    session = _make_mock_session(response_parts)

    ops: list[Put | Get | Delete] = [Delete("k3"), Get("k1"), Delete("k2")]
    results = list(execute_many(session, ops, concurrency=1))
    assert results[0].key == "k3"
    assert results[1].key == "k1"
    assert results[2].key == "k2"


def test_execute_many_individual_put() -> None:
    """Large puts should be routed to individual endpoints."""
    large_body = b"x" * (MAX_BATCH_PART_SIZE + 1)
    session = _make_mock_session()
    session.put.return_value = "assigned-key"

    results = list(
        execute_many(session, [Put(large_body, compression="none")], concurrency=1)
    )
    assert len(results) == 1
    assert results[0].key == "assigned-key"
    assert results[0].response is None
    # Should have called session.put, not session._pool.request for batch
    session.put.assert_called_once()


def test_execute_many_mixed_batch_and_individual() -> None:
    """Mix of small (batchable) and large (individual) operations."""
    large_body = b"x" * (MAX_BATCH_PART_SIZE + 1)

    # Response for the batch (index 0 = Get)
    response_parts = [_make_response_part(0, "200 OK", "k1", kind="get", body=b"hi")]
    session = _make_mock_session(response_parts)
    session.put.return_value = "big-key"

    ops: list[Put | Get | Delete] = [
        Get("k1"),
        Put(large_body, key="big-key", compression="none"),
    ]
    results = list(execute_many(session, ops, concurrency=1))
    assert len(results) == 2
    assert results[0].key == "k1"
    assert results[1].key == "big-key"


def test_execute_many_sequential_mixed_preserves_input_order() -> None:
    """concurrency=1 should not move later batch work ahead of individual ops."""
    large_body = b"x" * (MAX_BATCH_PART_SIZE + 1)
    response_parts = [
        _make_response_part(0, "200 OK", "after-large", kind="get", body=b"hi")
    ]
    session = _make_mock_session(response_parts)
    batch_response = session._pool.request.return_value
    calls: list[str] = []

    def put_side_effect(*_args: object, **_kwargs: object) -> str:
        calls.append("put")
        return "big-key"

    def request_side_effect(*_args: object, **_kwargs: object) -> object:
        calls.append("batch")
        return batch_response

    session.put.side_effect = put_side_effect
    session._pool.request.side_effect = request_side_effect

    ops: list[Put | Get] = [
        Put(large_body, key="big-key", compression="none"),
        Get("after-large"),
    ]
    results = list(execute_many(session, ops, concurrency=1))

    assert [result.key for result in results] == ["big-key", "after-large"]
    assert calls == ["put", "batch"]


def test_execute_many_io_put_goes_individual() -> None:
    """IO[bytes] puts should be routed individually without eager reading."""
    session = _make_mock_session()
    session.put.return_value = "assigned-key"

    results = list(
        execute_many(session, [Put(io.BytesIO(b"data"), key="my-key")], concurrency=1)
    )
    assert len(results) == 1
    assert results[0].key == "assigned-key"
    assert results[0].response is None
    session.put.assert_called_once()
    session._pool.request.assert_not_called()


def test_execute_many_io_put_passes_compression_to_session() -> None:
    """IO[bytes] puts should forward their compression setting to session.put()."""
    session = _make_mock_session()
    session.put.return_value = "assigned-key"

    list(
        execute_many(
            session,
            [Put(io.BytesIO(b"data"), key="k", compression="zstd")],
            concurrency=1,
        )
    )
    _, kwargs = session.put.call_args
    assert kwargs.get("compression") == "zstd"


def test_execute_many_io_put_mixed_with_batch() -> None:
    """IO[bytes] put goes individual while a concurrent Get is batched."""
    response_parts = [_make_response_part(0, "200 OK", "k1", kind="get", body=b"hi")]
    session = _make_mock_session(response_parts)
    session.put.return_value = "io-key"

    ops: list[Put | Get | Delete] = [Get("k1"), Put(io.BytesIO(b"data"), key="io-key")]
    results = list(execute_many(session, ops, concurrency=1))
    assert len(results) == 2
    assert results[0].key == "k1"
    assert results[1].key == "io-key"
    session.put.assert_called_once()
    session._pool.request.assert_called_once()
