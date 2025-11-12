from __future__ import annotations

from typing import IO

import filetype  # type: ignore


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
