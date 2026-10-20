"""Streaming compression and decompression for the supported algorithms."""

from __future__ import annotations

import gzip
import io
import zlib
from typing import IO, cast

import zstandard

from objectstore_client.metadata import Compression
from objectstore_client.utils import _ZstdCompressionReaderWrapper

SUPPORTED_COMPRESSIONS: tuple[Compression, ...] = ("zstd", "gzip")
"""Compression algorithms that the client can apply and transparently decode."""

_CHUNK_SIZE = 64 * 1024
# `wbits` for `zlib` that produces and consumes the gzip container format.
_GZIP_WBITS = 16 + zlib.MAX_WBITS


def validate(compression: str | None, *, allow_none: bool = True) -> None:
    """Raise `ValueError` if `compression` is not a supported algorithm.

    `None` is always accepted. `"none"` is accepted only if `allow_none` is set.
    """
    if compression is None or (allow_none and compression == "none"):
        return
    if compression not in SUPPORTED_COMPRESSIONS:
        raise ValueError(f"Invalid compression: {compression}")


def compressing_reader(source: IO[bytes], compression: Compression) -> IO[bytes]:
    """Return a readable stream yielding `source` compressed with `compression`.

    The returned stream compresses lazily as it is read, reports the number of
    compressed bytes produced via `tell()`, and supports `seek(0)` only until
    reading starts. Closing it does not close `source`.
    """
    if compression == "zstd":
        reader = zstandard.ZstdCompressor().stream_reader(source, closefd=False)
        return cast(IO[bytes], _ZstdCompressionReaderWrapper(reader))
    if compression == "gzip":
        return cast(IO[bytes], _GzipCompressionReader(source))
    raise ValueError(f"Invalid compression: {compression}")


def compress_into(
    source: IO[bytes], dest: IO[bytes], compression: Compression
) -> tuple[int, int]:
    """Compress `source` into `dest`, returning the bytes read and written."""
    if compression == "zstd":
        read, written = zstandard.ZstdCompressor().copy_stream(source, dest)
        return read, written
    if compression == "gzip":
        compressor = zlib.compressobj(wbits=_GZIP_WBITS)
        read = written = 0
        while chunk := source.read(_CHUNK_SIZE):
            read += len(chunk)
            written += dest.write(compressor.compress(chunk))
        written += dest.write(compressor.flush())
        return read, written
    raise ValueError(f"Invalid compression: {compression}")


def decompressing_reader(stream: IO[bytes], compression: str) -> IO[bytes]:
    """Return a readable stream decoding `stream` compressed with `compression`.

    Concatenated zstd frames and gzip members, as produced by multipart uploads
    with individually compressed parts, are decoded as a single payload.
    """
    if compression == "zstd":
        dctx = zstandard.ZstdDecompressor()
        return cast(IO[bytes], dctx.stream_reader(stream, read_across_frames=True))
    if compression == "gzip":
        return cast(IO[bytes], gzip.GzipFile(fileobj=stream, mode="rb"))
    raise NotImplementedError(
        f"Transparent decoding of `{compression}` is not implemented"
    )


class _GzipCompressionReader(io.RawIOBase):
    """A readable stream that gzip-compresses `source` as it is read.

    Like :class:`_ZstdCompressionReaderWrapper`, it allows `seek(0)` as long as
    no data has been read yet, so that urllib3 can record the body position.
    """

    def __init__(self, source: IO[bytes]) -> None:
        super().__init__()
        self._source = source
        self._compressor = zlib.compressobj(wbits=_GZIP_WBITS)
        self._buffer = bytearray()
        self._finished = False
        self._position = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1, /) -> bytes:
        if size is None or size < 0:
            size = -1
        while not self._finished and (size < 0 or len(self._buffer) < size):
            chunk = self._source.read(_CHUNK_SIZE)
            if chunk:
                self._buffer += self._compressor.compress(chunk)
            else:
                self._buffer += self._compressor.flush()
                self._finished = True

        if size < 0:
            size = len(self._buffer)
        out = bytes(self._buffer[:size])
        del self._buffer[:size]
        self._position += len(out)
        return out

    def readall(self) -> bytes:
        return self.read(-1)

    def readinto(self, buffer: memoryview | bytearray) -> int:  # type: ignore[override]
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = io.SEEK_SET, /) -> int:
        if offset == 0 and whence == io.SEEK_SET and self._position == 0:
            return 0
        raise OSError("Cannot seek in a compressed stream after reading has started")
