"""Custom multipart encoder/decoder for batch requests.

urllib3's encode_multipart_formdata doesn't support per-part custom headers,
which we need for the batch protocol's operation-kind/key/index headers.
"""

from __future__ import annotations

import re
import secrets
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import IO

# 64kB matches urllib3's default chunk size
_CHUNK_SIZE = 64 * 1024
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")


@dataclass
class RequestPart:
    """A single part in a multipart request."""

    headers: dict[str, str]
    body: bytes | IO[bytes]


@dataclass
class ResponsePart:
    """A single part parsed from a multipart response."""

    headers: dict[str, str]
    body: bytes


def _validate_part_header(name: str, value: str) -> None:
    if not name or ":" in name or not name.isascii() or _CONTROL_CHAR_RE.search(name):
        raise ValueError(f"Invalid multipart header name: {name!r}")
    if _CONTROL_CHAR_RE.search(value):
        raise ValueError(f"Invalid multipart header value for {name!r}")


def encode_multipart(parts: Iterable[RequestPart]) -> tuple[str, Iterator[bytes]]:
    """Encode parts into a multipart/form-data body.

    Returns (content_type_header, body_iterator). The iterator yields chunks
    lazily; urllib3 will send them via chunked transfer encoding.
    """
    boundary = f"os-boundary-{secrets.token_hex(16)}"

    def _generate() -> Iterator[bytes]:
        for part in parts:
            yield f"--{boundary}\r\n".encode()
            yield b"content-disposition: form-data; name=part\r\n"
            for name, value in part.headers.items():
                _validate_part_header(name, value)
                yield f"{name}: {value}\r\n".encode()
            yield b"\r\n"
            if isinstance(part.body, bytes):
                yield part.body
            else:
                while True:
                    chunk = part.body.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    yield chunk
            yield b"\r\n"
        yield f"--{boundary}--".encode()

    content_type = f'multipart/form-data; boundary="{boundary}"'
    return content_type, _generate()


def _extract_boundary(content_type: str) -> str:
    """Extract the boundary string from a Content-Type header."""
    match = re.search(r'boundary="?([^";]+)"?', content_type)
    if not match:
        raise ValueError(f"No boundary found in Content-Type: {content_type}")
    return match.group(1)


def _parse_part(data: bytes) -> ResponsePart:
    """Parse a single multipart part from its raw bytes (headers + body)."""
    header_end = data.find(b"\r\n\r\n")
    if header_end == -1:
        header_bytes = data
        part_body = b""
    else:
        header_bytes = data[:header_end]
        part_body = data[header_end + 4 :]

    headers: dict[str, str] = {}
    for line in header_bytes.split(b"\r\n"):
        if not line:
            continue
        colon_idx = line.find(b": ")
        if colon_idx == -1:
            continue
        name = line[:colon_idx].decode("ascii").lower()
        value = line[colon_idx + 2 :].decode("utf-8")
        headers[name] = value

    return ResponsePart(headers=headers, body=part_body)


def iter_multipart_response(
    content_type: str, chunks: Iterable[bytes]
) -> Iterator[ResponsePart]:
    """Parse a streaming multipart response, yielding one ResponsePart at a time.

    Accepts an iterable of byte chunks (e.g. from urllib3's response.stream()).
    Parts are yielded as soon as their full content has been received; the caller
    does not need to wait for the entire response body.
    """
    boundary = _extract_boundary(content_type)
    opening = f"--{boundary}\r\n".encode()
    # Between consecutive parts the delimiter is \r\n--boundary; the \r\n
    # belongs to the trailer of the preceding part, not the next part's header.
    delimiter = f"\r\n--{boundary}".encode()

    buf = bytearray()
    started = False

    for chunk in chunks:
        buf.extend(chunk)

        if not started:
            pos = buf.find(opening)
            if pos == -1:
                # Discard everything except the last len(opening)-1 bytes —
                # only that suffix could form a partial match across the next chunk.
                del buf[: -(len(opening) - 1)]
                continue
            del buf[: pos + len(opening)]
            started = True

        while True:
            pos = buf.find(delimiter)
            if pos == -1:
                break
            after_delim = pos + len(delimiter)
            # Need at least 2 bytes after the delimiter to distinguish
            # \r\n (next part follows) from -- (closing boundary).
            if len(buf) < after_delim + 2:
                break
            yield _parse_part(bytes(buf[:pos]))
            suffix = bytes(buf[after_delim : after_delim + 2])
            if suffix == b"--":
                return
            if suffix != b"\r\n":
                raise ValueError(
                    f"Malformed multipart body: unexpected bytes {suffix!r}"
                )
            del buf[: after_delim + 2]  # consume delimiter + \r\n


def parse_multipart_response(content_type: str, body: bytes) -> list[ResponsePart]:
    """Parse a multipart/form-data response body into parts."""
    return list(iter_multipart_response(content_type, [body]))
