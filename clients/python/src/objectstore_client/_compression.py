"""Streaming compression and decompression for the supported algorithms."""

from __future__ import annotations

import gzip
import io
import shutil
import zlib
from typing import IO, Protocol, cast

import zstandard

from objectstore_client.metadata import Compression

SUPPORTED_COMPRESSIONS: tuple[Compression, ...] = ("zstd", "gzip")
"""Compression algorithms that the client can apply and transparently decode."""


class _Compressor(Protocol):
    """A streaming compression object, such as `zlib.compressobj()`."""

    def compress(self, data: bytes, /) -> bytes: ...

    def flush(self) -> bytes: ...


class _CompressionReader(io.RawIOBase):
    """
    A readable stream that compresses `source` with `compressor` as it is read.

    `tell()` reports the number of compressed bytes produced so far. `seek(0)` is
    allowed as long as no data has been read yet, so that urllib3 can record the body
    position. Closing the reader does not close `source`.
    """

    _CHUNK_SIZE = 64 * 1024

    def __init__(self, source: IO[bytes], compressor: _Compressor) -> None:
        super().__init__()
        self._source = source
        self._compressor = compressor
        self._buffer = bytearray()
        self._finished = False
        self._position = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1, /) -> bytes:
        if size is None or size < 0:
            size = -1
        while not self._finished and (size < 0 or len(self._buffer) < size):
            if chunk := self._source.read(self._CHUNK_SIZE):
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
    """Compress `source` into `dest`, returning the bytes read and written.

    For gzip, `dest` must support `tell()` to measure the compressed size.
    """
    if compression == "zstd":
        read, written = zstandard.ZstdCompressor().copy_stream(source, dest)
        return read, written
    if compression == "gzip":
        start = dest.tell()
        with gzip.GzipFile(
            filename="", fileobj=dest, mode="wb", compresslevel=6, mtime=0
        ) as writer:
            shutil.copyfileobj(source, writer)
            read = writer.tell()
        return read, dest.tell() - start
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
