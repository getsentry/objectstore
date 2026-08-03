import io

import pytest
import zstandard
from objectstore_client.utils import (
    _ZstdCompressionReaderWrapper,
    decode_header_value,
    encode_header_value,
    parse_accept_encoding,
)


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


def test_parse_accept_encoding_single() -> None:
    assert parse_accept_encoding("zstd") == ["zstd"]


def test_parse_accept_encoding_multiple() -> None:
    assert parse_accept_encoding("zstd, gzip, br") == ["zstd", "gzip", "br"]


def test_parse_accept_encoding_normalizes_to_lowercase() -> None:
    assert parse_accept_encoding("ZSTD, Gzip") == ["zstd", "gzip"]


def test_parse_accept_encoding_strips_whitespace() -> None:
    assert parse_accept_encoding("  zstd ,  gzip  ") == ["zstd", "gzip"]


def test_parse_accept_encoding_wildcard() -> None:
    assert parse_accept_encoding("*") == ["*"]


def test_parse_accept_encoding_positive_q_values() -> None:
    assert parse_accept_encoding("gzip;q=1, zstd;q=0.5") == ["gzip", "zstd"]


def test_parse_accept_encoding_q_zero_excluded() -> None:
    assert parse_accept_encoding("gzip;q=0") == []
    assert parse_accept_encoding("gzip;q=0.0") == []


def test_parse_accept_encoding_mixed_q() -> None:
    assert parse_accept_encoding("zstd, gzip;q=0, br;q=0.5") == ["zstd", "br"]


def test_parse_accept_encoding_empty() -> None:
    assert parse_accept_encoding("") == []


def test_parse_accept_encoding_q_spacing() -> None:
    assert parse_accept_encoding("gzip ; q=0") == []
    assert parse_accept_encoding("gzip ; q=1") == ["gzip"]


def test_encode_header_value_escapes_unicode() -> None:
    assert encode_header_value("réport-📄.pdf") == "r%C3%A9port-%F0%9F%93%84.pdf"


def test_encode_header_value_escapes_percent() -> None:
    assert encode_header_value("100% done") == "100%25 done"


def test_encode_header_value_leaves_visible_ascii_alone() -> None:
    # Must stay byte-identical so values that predate the encoding are unaffected.
    value = "has\"quote path/to.txt!$&'()*+,;=:@?<>[]{}|^`~#"
    assert encode_header_value(value) == value


def test_encode_header_value_escapes_control_characters() -> None:
    assert encode_header_value("a\r\nb\tc\x7f") == "a%0D%0Ab%09c%7F"


@pytest.mark.parametrize(
    "value",
    [
        "réport-📄.pdf",
        "100% done",
        "50%.pdf",
        "plain.txt",
        "a\r\nb",
        "",
    ],
)
def test_header_value_roundtrip(value: str) -> None:
    assert decode_header_value(encode_header_value(value)) == value


def test_decode_header_value_rejects_invalid_utf8() -> None:
    with pytest.raises(UnicodeDecodeError):
        decode_header_value("%FF.pdf")
