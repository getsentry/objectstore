"""
Streaming ``multipart/form-data`` codec for batch requests.

`urllib3`'s `encode_multipart_formdata` doesn't support per-part custom headers,
but we need them for operation-kind/key/index headers.
"""

from __future__ import annotations

import re
import secrets
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import IO

# 64 KiB matches urllib3's default chunk size.
CHUNK_SIZE = 64 * 1024

# Header name/value regular expressions adapted from `h11`'s `_abnf.py` (MIT).
_VALID_HEADER_NAME = re.compile(r"[-!#$%&'*+.^_`|~0-9a-zA-Z]+")
_VISIBLE_CHARS = r"[\x21-\x7e]"
_VALID_HEADER_VALUE = re.compile(rf"({_VISIBLE_CHARS}+(?:[ \t]+{_VISIBLE_CHARS}+)*)?")

_BOUNDARY_RE = re.compile(r'boundary="?([^";]+)"?')

PartBody = bytes | IO[bytes]


@dataclass
class RequestPart:
    """A single part of a multipart request."""

    headers: dict[str, str]
    body: PartBody = b""

    def validate(self) -> None:
        """
        Raises ``ValueError`` if a header cannot be represented on the wire.
        """
        for name, value in self.headers.items():
            _validate_header(name, value)


@dataclass
class ResponsePart:
    """A single part parsed from a multipart response."""

    headers: dict[str, str]
    """Part headers, with names lowercased."""

    body: bytes = field(repr=False)


class MalformedMultipart(ValueError):
    """Raised when a multipart body cannot be parsed."""


class MultipartBody:
    """
    A lazily encoded ``multipart/form-data`` request body. Pass the instance as
    the ``body`` param in a urllib3 request and :attr:`content_type` as the
    request's ``Content-Type``.

    :attr:`resendable` indicates whether the instance can be iterated over
    multiple times and, consequently, whether urllib3 can retry requests. True
    when every part's body is ``bytes``.
    """

    def __init__(self, parts: Sequence[RequestPart]):
        for part in parts:
            part.validate()
        self._parts = parts
        self._boundary = f"os-boundary-{secrets.token_hex(16)}"
        self.resendable = all(isinstance(part.body, bytes) for part in parts)
        self.content_type = f'multipart/form-data; boundary="{self._boundary}"'

    def __iter__(self) -> Iterator[bytes]:
        for part in self._parts:
            yield self._preamble(part)
            if isinstance(part.body, bytes):
                if part.body:
                    yield part.body
            else:
                while chunk := part.body.read(CHUNK_SIZE):
                    yield chunk
            yield b"\r\n"
        yield f"--{self._boundary}--\r\n".encode()

    def _preamble(self, part: RequestPart) -> bytes:
        lines = [
            f"--{self._boundary}",
            "content-disposition: form-data; name=part",
        ]
        lines += [f"{name}: {value}" for name, value in part.headers.items()]
        return ("\r\n".join(lines) + "\r\n\r\n").encode()


def _validate_header(name: str, value: str) -> None:
    if not _VALID_HEADER_NAME.fullmatch(name):
        raise ValueError(f"invalid multipart header name: {name!r}")
    if not _VALID_HEADER_VALUE.fullmatch(value):
        raise ValueError(f"invalid multipart header value for {name!r}")


def _extract_boundary(content_type: str) -> str:
    """
    Returns the ``boundary`` parameter of a ``multipart/*`` content type.

    Raises :class:`MalformedMultipart` if the content type carries no boundary.
    """
    match = _BOUNDARY_RE.search(content_type)
    if not match:
        raise MalformedMultipart(
            f"no multipart boundary in Content-Type: {content_type!r}"
        )
    return match.group(1)


def iter_multipart(
    content_type: str, chunks: Iterable[bytes]
) -> Iterator[ResponsePart]:
    """
    Parses a multipart response body, yielding parts as they come in.

    Accepts an iterable of byte chunks (e.g. from urllib's `response.stream()`).
    Parts are yielded as soon as they arrive in full.

    Raises :class:`MalformedMultipart` if the body's ``boundary`` isn't valid
    and consistent.
    """
    boundary = _extract_boundary(content_type)
    opening = f"--{boundary}\r\n".encode()
    delimiter = f"\r\n--{boundary}".encode()

    buffer = bytearray()
    started = False

    for chunk in chunks:
        buffer.extend(chunk)

        if not started:
            position = buffer.find(opening)
            if position == -1:
                # Discard everything except the last len(opening)-1 bytes -
                # only that suffix could form a partial match across the next chunk.
                del buffer[: -(len(opening) - 1)]
                continue
            del buffer[: position + len(opening)]
            started = True

        while True:
            position = buffer.find(delimiter)
            if position == -1:
                break
            trailer = position + len(delimiter)
            # Need at least 2 bytes after the delimiter to distinguish
            # \r\n (next part follows) from -- (closing boundary).
            if len(buffer) < trailer + 2:
                break
            yield _parse_part(bytes(buffer[:position]))
            suffix = bytes(buffer[trailer : trailer + 2])
            if suffix == b"--":
                return
            if suffix != b"\r\n":
                raise MalformedMultipart(
                    f"unexpected bytes {suffix!r} after multipart delimiter"
                )
            del buffer[: trailer + 2]  # consume delimiter + \r\n

    if started:
        raise MalformedMultipart("multipart body ended without a closing delimiter")


def _parse_part(data: bytes) -> ResponsePart:
    """
    Parses a single part from its raw bytes (headers, blank line, body).
    """
    header_blob, separator, body = data.partition(b"\r\n\r\n")
    if not separator:
        raise MalformedMultipart("multipart part has no header separator")

    headers: dict[str, str] = {}
    for line in header_blob.split(b"\r\n"):
        name, separator, value = line.partition(b":")
        if not separator:
            continue
        utf8_name = name.decode("utf-8", "replace").strip().lower()
        utf8_value = value.decode("utf-8", "replace").strip()
        headers[utf8_name] = utf8_value

    return ResponsePart(headers=headers, body=body)
