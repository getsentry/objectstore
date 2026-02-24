from __future__ import annotations

import io
from typing import IO, Any

import filetype  # type: ignore[import-untyped]
from zstandard import ZstdCompressionReader


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
