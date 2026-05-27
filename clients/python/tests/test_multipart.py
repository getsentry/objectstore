import io

import pytest
from objectstore_client.multipart import (
    RequestPart,
    encode_multipart,
    iter_multipart_response,
    parse_multipart_response,
)

# ---------------------------------------------------------------------------
# encode_multipart
# ---------------------------------------------------------------------------


def test_encode_single_part() -> None:
    parts = [RequestPart(headers={"x-custom": "value"}, body=b"hello")]
    content_type, body_iter = encode_multipart(parts)
    body = b"".join(body_iter)

    assert content_type.startswith("multipart/form-data; boundary=")
    assert b"hello" in body
    assert b"x-custom: value" in body


def test_encode_multiple_parts() -> None:
    parts = [
        RequestPart(headers={"x-kind": "get"}, body=b""),
        RequestPart(headers={"x-kind": "insert"}, body=b"payload"),
    ]
    content_type, body_iter = encode_multipart(parts)
    body = b"".join(body_iter)

    assert body.count(b"x-kind") == 2
    assert b"payload" in body


def test_encode_empty_body() -> None:
    parts = [RequestPart(headers={"x-op": "delete"}, body=b"")]
    _, body_iter = encode_multipart(parts)
    body = b"".join(body_iter)
    assert b"x-op: delete" in body


def test_encode_binary_body() -> None:
    binary_data = bytes(range(256))
    parts = [RequestPart(headers={}, body=binary_data)]
    _, body_iter = encode_multipart(parts)
    body = b"".join(body_iter)
    assert binary_data in body


def test_encode_io_body() -> None:
    data = b"streamed content"
    parts = [RequestPart(headers={"x-kind": "insert"}, body=io.BytesIO(data))]
    _, body_iter = encode_multipart(parts)
    body = b"".join(body_iter)
    assert data in body
    assert b"x-kind: insert" in body


def test_encode_accepts_generator_of_parts() -> None:
    """encode_multipart should accept any Iterable, not just a list."""

    def parts_gen() -> object:
        yield RequestPart(headers={"x-i": "0"}, body=b"a")
        yield RequestPart(headers={"x-i": "1"}, body=b"b")

    _, body_iter = encode_multipart(parts_gen())  # type: ignore[arg-type]
    body = b"".join(body_iter)
    assert b"x-i: 0" in body
    assert b"x-i: 1" in body


def test_content_disposition_header_included() -> None:
    """Each encoded part should include content-disposition: form-data; name=part."""
    parts = [RequestPart(headers={"x-op": "get"}, body=b"")]
    _, body_iter = encode_multipart(parts)
    body = b"".join(body_iter)
    assert b"content-disposition: form-data; name=part" in body


def test_encode_rejects_multipart_header_name_control_chars() -> None:
    parts = [RequestPart(headers={"x-good\r\nx-injected": "value"}, body=b"")]
    _, body_iter = encode_multipart(parts)

    with pytest.raises(ValueError, match="Invalid multipart header name"):
        b"".join(body_iter)


def test_encode_rejects_multipart_header_value_control_chars() -> None:
    parts = [RequestPart(headers={"x-good": "value\r\nx-injected: yes"}, body=b"")]
    _, body_iter = encode_multipart(parts)

    with pytest.raises(ValueError, match="Invalid multipart header value"):
        b"".join(body_iter)


# ---------------------------------------------------------------------------
# round-trip: encode then parse
# ---------------------------------------------------------------------------


def test_round_trip() -> None:
    original_parts = [
        RequestPart(headers={"x-index": "0", "x-kind": "get"}, body=b""),
        RequestPart(headers={"x-index": "1", "x-kind": "insert"}, body=b"data here"),
        RequestPart(headers={"x-index": "2", "x-kind": "delete"}, body=b""),
    ]
    content_type, body_iter = encode_multipart(original_parts)
    body = b"".join(body_iter)

    parsed = parse_multipart_response(content_type, body)
    assert len(parsed) == 3

    for i, (original, parsed_part) in enumerate(zip(original_parts, parsed)):
        assert parsed_part.body == original.body, f"body mismatch at part {i}"
        for key, value in original.headers.items():
            assert parsed_part.headers[key] == value, (
                f"header {key} mismatch at part {i}"
            )


def test_round_trip_binary() -> None:
    binary_data = bytes(range(256)) + b"\r\n--boundary\r\n"
    original = [RequestPart(headers={"x-test": "bin"}, body=binary_data)]
    content_type, body_iter = encode_multipart(original)
    body = b"".join(body_iter)

    parsed = parse_multipart_response(content_type, body)
    assert len(parsed) == 1
    assert parsed[0].body == binary_data


def test_parse_extracts_boundary_from_content_type() -> None:
    parts = [RequestPart(headers={"x-a": "1"}, body=b"test")]
    content_type, body_iter = encode_multipart(parts)
    body = b"".join(body_iter)

    parsed = parse_multipart_response(content_type, body)
    assert len(parsed) == 1
    assert parsed[0].body == b"test"


def test_parse_empty_response() -> None:
    """A response with just the closing boundary and no parts."""
    boundary = "test-boundary"
    content_type = f'multipart/form-data; boundary="{boundary}"'
    body = f"--{boundary}--\r\n".encode()

    parsed = parse_multipart_response(content_type, body)
    assert len(parsed) == 0


# ---------------------------------------------------------------------------
# iter_multipart_response
# ---------------------------------------------------------------------------


def _make_multipart_body(
    boundary: str, parts: list[tuple[dict[str, str], bytes]]
) -> bytes:
    """Build a multipart body from boundary and (headers, body) pairs."""
    chunks: list[bytes] = []
    for headers, body in parts:
        chunks.append(f"--{boundary}\r\n".encode())
        chunks.append(b"content-disposition: form-data; name=part\r\n")
        for name, value in headers.items():
            chunks.append(f"{name}: {value}\r\n".encode())
        chunks.append(b"\r\n")
        chunks.append(body)
        chunks.append(b"\r\n")
    chunks.append(f"--{boundary}--".encode())
    return b"".join(chunks)


def test_iter_multipart_yields_parts_from_single_chunk() -> None:
    boundary = "bnd"
    body = _make_multipart_body(
        boundary,
        [
            ({"x-i": "0"}, b"hello"),
            ({"x-i": "1"}, b"world"),
        ],
    )
    content_type = f'multipart/form-data; boundary="{boundary}"'
    parts = list(iter_multipart_response(content_type, [body]))

    assert len(parts) == 2
    assert parts[0].body == b"hello"
    assert parts[1].body == b"world"


def test_iter_multipart_yields_parts_split_across_chunks() -> None:
    """Parts should be yielded correctly even when the boundary spans chunks."""
    boundary = "bnd"
    body = _make_multipart_body(
        boundary,
        [
            ({"x-i": "0"}, b"part-zero-data"),
            ({"x-i": "1"}, b"part-one-data"),
        ],
    )
    content_type = f'multipart/form-data; boundary="{boundary}"'

    # Split the body at every byte to maximally stress boundary detection.
    chunks = [bytes([b]) for b in body]
    parts = list(iter_multipart_response(content_type, chunks))

    assert len(parts) == 2
    assert parts[0].body == b"part-zero-data"
    assert parts[1].body == b"part-one-data"


def test_iter_multipart_yields_parts_split_at_boundary() -> None:
    """Split exactly at the boundary delimiter."""
    boundary = "myboundary"
    body = _make_multipart_body(
        boundary,
        [
            ({"x-n": "a"}, b"AAA"),
            ({"x-n": "b"}, b"BBB"),
        ],
    )
    content_type = f'multipart/form-data; boundary="{boundary}"'

    # Find the delimiter position and split there
    delim = f"\r\n--{boundary}".encode()
    split_pos = body.find(delim) + 3  # split mid-delimiter
    chunks = [body[:split_pos], body[split_pos:]]
    parts = list(iter_multipart_response(content_type, chunks))

    assert len(parts) == 2
    assert parts[0].body == b"AAA"
    assert parts[1].body == b"BBB"


def test_iter_multipart_empty_response() -> None:
    boundary = "bnd"
    body = f"--{boundary}--".encode()
    content_type = f'multipart/form-data; boundary="{boundary}"'
    parts = list(iter_multipart_response(content_type, [body]))
    assert parts == []


def test_iter_multipart_headers_parsed_correctly() -> None:
    boundary = "bnd"
    body = _make_multipart_body(
        boundary,
        [
            ({"x-status": "200 OK", "x-key": "my-key"}, b"payload"),
        ],
    )
    content_type = f'multipart/form-data; boundary="{boundary}"'
    parts = list(iter_multipart_response(content_type, [body]))

    assert len(parts) == 1
    assert parts[0].headers["x-status"] == "200 OK"
    assert parts[0].headers["x-key"] == "my-key"
    assert parts[0].body == b"payload"


def test_iter_multipart_raises_on_malformed_post_delimiter_bytes() -> None:
    """Bytes after the boundary delimiter that are neither \\r\\n nor -- raise."""
    boundary = "bnd"
    body = (
        f"--{boundary}\r\n".encode()
        + b"x-i: 0\r\n\r\nbody"
        + f"\r\n--{boundary}".encode()
        + b"XX"  # garbage instead of \r\n or --
    )
    content_type = f'multipart/form-data; boundary="{boundary}"'
    with pytest.raises(ValueError, match="Malformed multipart"):
        list(iter_multipart_response(content_type, [body]))


def test_iter_multipart_round_trip_matches_parse() -> None:
    """iter_multipart_response and parse_multipart_response must agree."""
    original_parts = [
        RequestPart(headers={"x-index": "0"}, body=b"alpha"),
        RequestPart(headers={"x-index": "1"}, body=b"beta"),
        RequestPart(headers={"x-index": "2"}, body=b""),
    ]
    content_type, body_iter = encode_multipart(original_parts)
    body = b"".join(body_iter)

    from_list = parse_multipart_response(content_type, body)
    from_iter = list(iter_multipart_response(content_type, [body]))

    assert len(from_list) == len(from_iter)
    for a, b_part in zip(from_list, from_iter):
        assert a.headers == b_part.headers
        assert a.body == b_part.body
