"""Tests for the streaming ``multipart/form-data`` codec used by batch requests."""

from __future__ import annotations

import io
from collections.abc import Iterable

import pytest
from objectstore_client.formdata import (
    MalformedMultipart,
    MultipartBody,
    RequestPart,
    ResponsePart,
    _extract_boundary,
    iter_multipart,
)


def _serialize(parts: Iterable[RequestPart]) -> tuple[str, bytes]:
    body = MultipartBody(list(parts))
    return body.content_type, b"".join(body)


def _parse(content_type: str, body: bytes) -> list[ResponsePart]:
    return list(iter_multipart(content_type, [body]))


def test_encodes_part_headers_and_body() -> None:
    content_type, body = _serialize([RequestPart({"x-custom": "value"}, b"hello")])

    boundary = _extract_boundary(content_type)
    assert body.startswith(f"--{boundary}\r\n".encode())
    assert b"content-disposition: form-data; name=part\r\n" in body
    assert b"x-custom: value\r\n" in body
    assert b"\r\n\r\nhello\r\n" in body
    assert body.endswith(f"--{boundary}--\r\n".encode())


def test_reads_a_stream_body_only_while_encoding() -> None:
    class Recording(io.BytesIO):
        reads = 0

        def read(self, size: int | None = -1, /) -> bytes:
            Recording.reads += 1
            return super().read(size)

    body = MultipartBody([RequestPart({}, Recording(b"lazy body"))])
    assert Recording.reads == 0

    assert b"lazy body" in b"".join(body)
    assert Recording.reads > 0


def test_bytes_body_can_be_resent() -> None:
    """urllib3 re-iterates the body when it retries a connection."""
    body = MultipartBody([RequestPart({"x-kind": "insert"}, b"payload")])

    assert b"".join(body) == b"".join(body)


def test_leaves_a_stream_body_open() -> None:
    stream = io.BytesIO(b"payload")

    assert b"payload" in b"".join(MultipartBody([RequestPart({}, stream)]))

    # Releasing the stream is the caller's business, not the encoder's.
    assert not stream.closed


def test_reports_whether_a_body_can_be_resent() -> None:
    assert MultipartBody([RequestPart({}, b"payload")]).resendable
    assert not MultipartBody([RequestPart({}, io.BytesIO(b"payload"))]).resendable
    assert not MultipartBody(
        [RequestPart({}, b"payload"), RequestPart({}, io.BytesIO(b"payload"))]
    ).resendable


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("x-header", "value\r\nx-sn-batch-operation-kind: delete"),
        ("x-header", "value\nmore"),
        ("x-header", "control\x00char"),
        ("x-header", "nön-ascii"),
        ("x-head\r\ner", "value"),
        ("x-header:", "value"),
        ("x-head er", "value"),
        ("x-header", " leading space"),
        ("x-header", "trailing space "),
        ("x-header", "\t"),
        ("x-hea(d)er", "value"),
        ("nön-ascii", "value"),
        ("", "value"),
    ],
)
def test_rejects_unrepresentable_headers(name: str, value: str) -> None:
    with pytest.raises(ValueError):
        MultipartBody([RequestPart({name: value})])

    with pytest.raises(ValueError):
        RequestPart({name: value}).validate()


def test_accepts_ordinary_headers() -> None:
    # An empty value is a legal field-value, and metadata may well carry one.
    RequestPart({"x-snme-empty": ""}).validate()

    # A value carries what header values normally carry, punctuation included.
    RequestPart(
        {
            "content-disposition": 'form-data; name="part"',
            "x-snme-report_id": 'spaces, quotes ("like this") and tabs\tare fine',
        }
    ).validate()


def test_extract_boundary() -> None:
    assert _extract_boundary('multipart/form-data; boundary="abc"') == "abc"
    assert _extract_boundary("multipart/form-data; boundary=abc") == "abc"
    with pytest.raises(MalformedMultipart):
        _extract_boundary("application/json")


def test_parses_encoded_body() -> None:
    content_type, body = _serialize(
        [
            RequestPart({"x-index": "0"}, b"first"),
            RequestPart({"x-index": "1"}),
            RequestPart({"x-index": "2"}, b"\x00\xff binary \r\n body"),
        ]
    )

    parts = _parse(content_type, body)

    assert [part.headers["x-index"] for part in parts] == ["0", "1", "2"]
    assert [part.body for part in parts] == [
        b"first",
        b"",
        b"\x00\xff binary \r\n body",
    ]
    assert all(part.headers["content-disposition"] for part in parts)


def test_parses_headers_without_space_after_colon() -> None:
    body = b"--b\r\nx-index:0\r\n\r\nbody\r\n--b--\r\n"
    (part,) = _parse("multipart/form-data; boundary=b", body)
    assert part.headers == {"x-index": "0"}
    assert part.body == b"body"


def test_skips_preamble() -> None:
    body = b"ignored preamble\r\n--b\r\nx-index: 0\r\n\r\nbody\r\n--b--\r\n"
    (part,) = _parse("multipart/form-data; boundary=b", body)
    assert part.body == b"body"


def test_yields_parts_before_the_body_ends() -> None:
    content_type, body = _serialize(
        [RequestPart({"x-index": "0"}, b"first"), RequestPart({"x-index": "1"})]
    )
    # Feeding the body one byte at a time exercises boundary detection across
    # chunks, and shows that a part is yielded as soon as it is complete.
    parts = iter_multipart(content_type, (body[i : i + 1] for i in range(len(body))))

    first = next(parts)
    assert first.body == b"first"
    assert next(parts).headers["x-index"] == "1"
    with pytest.raises(StopIteration):
        next(parts)


def test_rejects_truncated_body() -> None:
    content_type, body = _serialize([RequestPart({"x-index": "0"}, b"first")])
    with pytest.raises(MalformedMultipart):
        _parse(content_type, body[: len(body) // 2])


def test_rejects_unexpected_bytes_after_delimiter() -> None:
    body = b"--b\r\nx-index: 0\r\n\r\nbody\r\n--bxx\r\n"
    with pytest.raises(MalformedMultipart):
        _parse("multipart/form-data; boundary=b", body)


def test_skips_a_header_line_it_cannot_split() -> None:
    body = b"--b\r\nx-index: 0\r\ngarbage\r\nx-status: 200 OK\r\n\r\nbody\r\n--b--\r\n"

    (part,) = _parse("multipart/form-data; boundary=b", body)

    # The line is dropped, the part it belongs to is not.
    assert part.headers == {"x-index": "0", "x-status": "200 OK"}
    assert part.body == b"body"


def test_rejects_part_without_headers() -> None:
    body = b"--b\r\nbroken\r\n--b--\r\n"
    with pytest.raises(MalformedMultipart):
        _parse("multipart/form-data; boundary=b", body)


def test_parses_empty_body() -> None:
    assert _parse("multipart/form-data; boundary=b", b"") == []
