from __future__ import annotations

import io
from typing import IO, Any
from urllib.parse import quote, unquote

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

# Characters left unescaped in a metadata header value: every visible ASCII
# character except "%". Non-ASCII, the C0 controls, and DEL are escaped because a
# header value cannot carry them; "%" is escaped so that a literal percent sign in
# a value can never be confused with an escape sequence when decoding.
#
# This is byte-for-byte identical to the Rust `HEADER_ESCAPE` set (see
# `objectstore-types/src/headers.rs`), so both clients and the server agree on the
# wire form. Values that are already plain ASCII are left untouched.
_HEADER_VALUE_SAFE = "".join(chr(b) for b in range(0x20, 0x7F) if b != ord("%"))


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


def encode_header_value(value: str) -> str:
    """Percent-encodes a free-form metadata string for an HTTP header value.

    HTTP header values carry only visible ASCII, so anything outside that range has
    to be escaped to survive the transport (see :data:`_HEADER_VALUE_SAFE`). This is
    the inverse of :func:`decode_header_value`; only the transport representation is
    escaped, never the logical string held in :class:`Metadata`.
    """
    return quote(value, safe=_HEADER_VALUE_SAFE)


def decode_header_value(value: str) -> str:
    """Decodes a percent-encoded HTTP header value into its logical string.

    Inverse of :func:`encode_header_value`. Values without escape sequences are
    returned unchanged, so headers written before the encoding existed still read
    back as-is.
    """
    return unquote(value, errors="strict")


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
