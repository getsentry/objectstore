from __future__ import annotations

import io
from typing import IO, Any
from urllib.parse import quote

import filetype  # type: ignore[import-untyped]
from zstandard import ZstdCompressionReader

# Characters left unescaped when percent-encoding a URL path or query, expressed
# in RFC 3986 terms. `urllib.parse.quote` already leaves the *unreserved* set
# (ALPHA / DIGIT / "-._~") untouched, so `safe` only lists the extra characters
# permitted without escaping:
#   - sub-delims: "!$&'()*+,;="
#   - path extras: ":" "@" "/"  (the pchar set plus the "/" segment separator)
#   - query also allows "?"
# This is the same set urllib3 leaves unescaped, so the encoding is a fixed point
# of the HTTP client that ultimately sends the URL and survives on the wire
# verbatim. It is also byte-for-byte identical to the Rust client's
# `percent_encoding` set (see `objectstore-types/src/presign.rs`), so the two
# clients produce the same bytes for any key.
_PATH_SAFE = "/:@!$&'()*+,;="
_QUERY_SAFE = _PATH_SAFE + "?"


def encode_path(path: str) -> str:
    """Percent-encodes a request path as it will appear on the wire.

    Leaves the RFC 3986 unreserved set, sub-delims, and ``:`` ``@`` ``/``
    unescaped (see :data:`_PATH_SAFE`); everything else is percent-encoded. This
    treats the input as a literal string: a literal ``%`` becomes ``%25``.
    """
    return quote(path, safe=_PATH_SAFE)


def encode_query(query: str) -> str:
    """Percent-encodes a query string as it will appear on the wire.

    Same set as :func:`encode_path`, additionally leaving ``?`` unescaped
    (see :data:`_QUERY_SAFE`).
    """
    return quote(query, safe=_QUERY_SAFE)


def parse_accept_encoding(header: str) -> list[str]:
    """Parse an Accept-Encoding header value for use in objectstore GET requests.

    Returns a list of encoding names, normalized to lowercase and stripped of q-values,
    per RFC 7231. Encodings explicitly rejected via ``q=0`` are excluded.

    Note: ``identity;q=0`` is not supported and will be silently ignored (i.e., treated
    as acceptable), since objectstore always serves an identity fallback.
    """
    result = []
    for part in header.split(","):
        segments = part.split(";")
        name = segments[0].strip().lower()
        if not name:
            continue
        if any(_is_q_zero(s) for s in segments[1:]):
            continue
        result.append(name)
    return result


def _is_q_zero(segment: str) -> bool:
    s = segment.strip().lower()
    if not s.startswith("q="):
        return False
    try:
        return float(s[2:]) == 0.0
    except ValueError:
        return False


def guess_mime_type(contents: bytes | IO[bytes]) -> str | None:
    """
    Guesses the MIME type from the given contents.

    Reads up to 261 bytes from the beginning of the content to determine
    the MIME type using file header signatures.

    To guess the MIME type from a filename, use `mimetypes.guess_type`,
    which is part of the standard library.
    """

    if isinstance(contents, bytes):
        header = contents[:261]
    else:
        if not contents.seekable():
            return None
        pos = contents.tell()
        header = contents.read(261)
        contents.seek(pos)

    kind = filetype.guess(header)
    return kind.mime if kind else None


class _ZstdCompressionReaderWrapper:
    """
    Wraps a `ZstdCompressionReader`, allowing `seek(0)` as long as no data has been read
    yet.
    """

    def __init__(self, inner: ZstdCompressionReader) -> None:
        self._inner = inner

    def seek(self, offset: int, whence: int = io.SEEK_SET, /) -> int:
        current = self._inner.tell()
        if current == offset == 0 and whence == io.SEEK_SET:
            return 0
        raise OSError("Cannot seek in a compressed stream after reading has started")

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._inner, attr)

    def __setattr__(self, name: str, value: object) -> None:
        if name == "_inner":
            object.__setattr__(self, name, value)
        else:
            setattr(self._inner, name, value)
