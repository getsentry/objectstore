import gzip
import io
import random
from typing import IO, cast
from unittest.mock import Mock

import pytest
import urllib3
import zstandard
from objectstore_client import _compression
from objectstore_client.metadata import Compression

# Incompressible, so the output spans many internal chunks.
PAYLOAD = random.Random(0).randbytes(300_000) + b"tail" * 1000


def _make_wrapper(data: bytes = b"hello world") -> _compression._CompressionReader:
    compressor = zstandard.ZstdCompressor().compressobj()
    return _compression._CompressionReader(io.BytesIO(data), compressor)


def test_seek_zero_before_read() -> None:
    wrapper = _make_wrapper()
    assert wrapper.seek(0) == 0
    assert wrapper.seek(0) == 0


def test_seek_nonzero_before_read() -> None:
    wrapper = _make_wrapper()
    with pytest.raises(OSError, match="Cannot seek"):
        wrapper.seek(1)


def test_seek_zero_after_read() -> None:
    wrapper = _make_wrapper()
    wrapper.read(1)
    with pytest.raises(OSError, match="Cannot seek"):
        wrapper.seek(0)


def test_read_and_tell_forwarded() -> None:
    wrapper = _make_wrapper()
    assert wrapper.tell() == 0
    data = wrapper.read(4)
    assert len(data) > 0
    assert wrapper.tell() > 0


@pytest.mark.parametrize("compression", ["zstd", "gzip"])
@pytest.mark.parametrize("read_size", [-1, 1, 4096, 100_000])
def test_compressing_reader_roundtrip(compression: Compression, read_size: int) -> None:
    source = io.BytesIO(PAYLOAD)
    reader = _compression.compressing_reader(source, compression)

    chunks = []
    while chunk := reader.read(read_size):
        chunks.append(chunk)
    compressed = b"".join(chunks)

    assert reader.tell() == len(compressed)
    assert source.tell() == len(PAYLOAD)
    decoded = _compression.decompressing_reader(io.BytesIO(compressed), compression)
    assert decoded.read() == PAYLOAD

    reader.close()
    assert not source.closed


@pytest.mark.parametrize("compression", ["zstd", "gzip"])
def test_compressing_reader_seek(
    compression: Compression,
) -> None:
    reader = _compression.compressing_reader(io.BytesIO(PAYLOAD), compression)
    assert reader.seek(0) == 0
    reader.read(10)
    with pytest.raises(OSError):
        reader.seek(0)


@pytest.mark.parametrize("compression", ["zstd", "gzip"])
def test_compress_into(compression: Compression) -> None:
    dest = io.BytesIO()
    read, written = _compression.compress_into(io.BytesIO(PAYLOAD), dest, compression)

    assert read == len(PAYLOAD)
    assert written == len(dest.getvalue())
    dest.seek(0)
    assert _compression.decompressing_reader(dest, compression).read() == PAYLOAD


def test_gzip_format() -> None:
    reader = _compression.compressing_reader(io.BytesIO(PAYLOAD), "gzip")
    assert gzip.decompress(reader.read()) == PAYLOAD


@pytest.mark.parametrize(
    "compression,compress",
    [("zstd", zstandard.ZstdCompressor().compress), ("gzip", gzip.compress)],
)
def test_concatenated_members(compression: str, compress: object) -> None:
    assert callable(compress)
    stream = io.BytesIO(compress(b"hello ") + compress(b"world"))
    assert _compression.decompressing_reader(stream, compression).read() == (
        b"hello world"
    )


def test_gzip_partial_read_close() -> None:
    connection = Mock()
    body = io.BytesIO(gzip.compress(PAYLOAD))
    response = urllib3.HTTPResponse(
        body=body,
        preload_content=False,
        connection=connection,
    )
    reader = _compression.decompressing_reader(cast(IO[bytes], response), "gzip")

    assert reader.read(10) == PAYLOAD[:10]
    assert not response.closed
    assert not reader.seekable()
    with pytest.raises(io.UnsupportedOperation):
        reader.seek(0)

    reader.close()
    assert body.closed
    connection.close.assert_called_once_with()

    reader.close()
    connection.close.assert_called_once_with()


def test_unknown_compression() -> None:
    with pytest.raises(NotImplementedError):
        _compression.decompressing_reader(io.BytesIO(b""), "lz4")


def test_validate() -> None:
    for compression in ["zstd", "gzip", "none", None]:
        _compression.validate(compression)
    with pytest.raises(ValueError):
        _compression.validate("none", allow_none=False)
    with pytest.raises(ValueError):
        _compression.validate("lz4")
