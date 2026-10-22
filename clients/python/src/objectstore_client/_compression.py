"""Streaming compression and decompression for the supported algorithms."""

from __future__ import annotations

import gzip
import io
import zlib
from typing import IO, cast

import zstandard

from objectstore_client.metadata import Compression
from objectstore_client.utils import _CompressionReader, _Compressor

SUPPORTED_COMPRESSIONS: tuple[Compression, ...] = ("zstd", "gzip")
"""Compression algorithms that the client can apply and transparently decode."""

_CHUNK_SIZE = 64 * 1024


class _GzipReader(gzip.GzipFile):
    """Decode gzip from a stream, closing the source with the reader."""

    def __init__(self, source: IO[bytes]) -> None:
        super().__init__(fileobj=source, mode="rb")
        self._source = source

    def seekable(self) -> bool:
        return False

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        raise io.UnsupportedOperation("gzip download stream is not seekable")

    def close(self) -> None:
        if self.closed:
            return
        try:
            super().close()
        finally:
            self._source.close()


def _gzip_compressor() -> _Compressor:
    """Return a zlib compressor that emits the gzip format.

    By default, zlib wraps deflate data in the zlib format. Adding 16 to the window
    size (`wbits`) selects the gzip header and trailer instead, which is what
    `Content-Encoding: gzip` requires. See `zlib.compressobj`.
    """
    return zlib.compressobj(wbits=16 + zlib.MAX_WBITS)


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
        compressor = zstandard.ZstdCompressor().compressobj()
        return cast(IO[bytes], _CompressionReader(source, compressor))
    if compression == "gzip":
        return cast(IO[bytes], _CompressionReader(source, _gzip_compressor()))
    raise ValueError(f"Invalid compression: {compression}")


def compress_into(
    source: IO[bytes], dest: IO[bytes], compression: Compression
) -> tuple[int, int]:
    """Compress `source` into `dest`, returning the bytes read and written."""
    if compression == "zstd":
        read, written = zstandard.ZstdCompressor().copy_stream(source, dest)
        return read, written
    if compression == "gzip":
        compressor = _gzip_compressor()
        read = written = 0
        while chunk := source.read(_CHUNK_SIZE):
            read += len(chunk)
            written += dest.write(compressor.compress(chunk))
        written += dest.write(compressor.flush())
        return read, written
    raise ValueError(f"Invalid compression: {compression}")


def decompressing_reader(stream: IO[bytes], compression: str) -> IO[bytes]:
    """Return a readable stream decoding `stream` compressed with `compression`.

    Concatenated zstd frames and gzip members are decoded as a single payload.
    The returned reader is non-seekable and closes the source when closed.
    """
    if compression == "zstd":
        dctx = zstandard.ZstdDecompressor()
        return cast(IO[bytes], dctx.stream_reader(stream, read_across_frames=True))
    if compression == "gzip":
        return cast(IO[bytes], _GzipReader(stream))
    raise NotImplementedError(
        f"Transparent decoding of `{compression}` is not implemented"
    )
