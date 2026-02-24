import io

import pytest
import zstandard
from objectstore_client.utils import _ZstdCompressionReaderWrapper


def _make_wrapper(data: bytes = b"hello world") -> _ZstdCompressionReaderWrapper:
    cctx = zstandard.ZstdCompressor()
    reader = cctx.stream_reader(io.BytesIO(data))
    return _ZstdCompressionReaderWrapper(reader)


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
