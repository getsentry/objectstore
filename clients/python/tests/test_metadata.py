import re
from datetime import timedelta

import pytest
from objectstore_client.metadata import (
    Metadata,
    TimeToIdle,
    TimeToLive,
    encode_metadata_key,
    format_expiration,
    format_timedelta,
    normalize_metadata_key,
    parse_expiration,
    parse_timedelta,
)
from objectstore_client.utils import decode_header_value

# Durations and their wire format, used in both directions.
ROUND_TRIP_CASES = [
    (timedelta(0), "0s"),
    (timedelta(seconds=30), "30s"),
    (timedelta(minutes=1), "1m"),
    (timedelta(hours=1), "1h"),
    (timedelta(days=1), "1d"),
    (timedelta(days=2, hours=3, seconds=4), "2d 3h 4s"),
    # Durations beyond a day stay in days, they never roll over to larger units.
    (timedelta(days=7), "7d"),
    (timedelta(days=400, hours=1), "400d 1h"),
]


@pytest.mark.parametrize(("delta", "formatted"), ROUND_TRIP_CASES)
def test_format_timedelta(delta: timedelta, formatted: str) -> None:
    assert format_timedelta(delta) == formatted


@pytest.mark.parametrize(("delta", "formatted"), ROUND_TRIP_CASES)
def test_parse_timedelta(delta: timedelta, formatted: str) -> None:
    assert parse_timedelta(formatted) == delta


def test_format_timedelta_truncates_sub_second_remainder() -> None:
    assert format_timedelta(timedelta(milliseconds=1500)) == "1s"
    assert format_timedelta(timedelta(milliseconds=500)) == "0s"


@pytest.mark.parametrize(
    "value",
    [
        "2weeks",
        "1year",
        "500ms",
        # Units that merely start with one of the wire format units must not be
        # mistaken for it: `13months` is not 13 minutes.
        "13months",
        "1month",
        "1minute",
        "30sec",
        "1day",
        "2hours",
    ],
)
def test_parse_timedelta_rejects_unit_outside_wire_format(value: str) -> None:
    with pytest.raises(ValueError, match="unknown time unit"):
        parse_timedelta(value)


def test_format_expiration() -> None:
    assert format_expiration(TimeToLive(timedelta(days=400))) == "ttl:400d"
    assert format_expiration(TimeToIdle(timedelta(hours=1))) == "tti:1h"


def test_parse_expiration() -> None:
    assert parse_expiration("ttl:400d") == TimeToLive(timedelta(days=400))
    assert parse_expiration("tti:1h") == TimeToIdle(timedelta(hours=1))
    assert parse_expiration("manual") is None


ALLOWED_KEY_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789_-|~%$#+.:"


@pytest.mark.parametrize("char", ALLOWED_KEY_CHARS)
def test_normalize_metadata_key_allows_each_character(char: str) -> None:
    assert normalize_metadata_key(f"a{char}b") == f"a{char}b"


def test_normalize_metadata_key_lowercases_ascii_letters() -> None:
    assert normalize_metadata_key("Source-ID") == "source-id"


@pytest.mark.parametrize(
    "key",
    ["", "a b", "a/b", "a@b", "a=b", "a,b", "a;b", "a*b", "a\tb", "a\nb", "ä", "kéy"],
)
def test_normalize_metadata_key_rejects_invalid_keys(key: str) -> None:
    with pytest.raises(ValueError, match="metadata key"):
        normalize_metadata_key(key)


def test_normalize_metadata_key_error_names_the_key() -> None:
    with pytest.raises(ValueError, match=re.escape(repr("bad key"))):
        normalize_metadata_key("bad key")


@pytest.mark.parametrize(
    ("key", "encoded"),
    [
        ("plain", "plain"),
        ("a%b", "a%25b"),
        ("a:b", "a%3ab"),
        ("a%3ab", "a%253ab"),
        ("%:", "%25%3a"),
    ],
)
def test_metadata_key_header_name_round_trip(key: str, encoded: str) -> None:
    assert encode_metadata_key(key) == encoded
    assert decode_header_value(encoded) == key


@pytest.mark.parametrize(
    ("name", "key"),
    [
        ("%61", "a"),
        ("%3a", ":"),
        ("%3A", ":"),
        ("%253a", "%3a"),
        ("%zz", "%zz"),
        ("a%", "a%"),
    ],
)
def test_key_header_name_decoding_accepts_any_escape(name: str, key: str) -> None:
    assert decode_header_value(name) == key


def test_key_header_name_decoding_rejects_invalid_utf8() -> None:
    with pytest.raises(UnicodeDecodeError):
        decode_header_value("%ff")


def test_from_headers_decodes_custom_key_names() -> None:
    metadata = Metadata.from_headers(
        {"x-snme-a%3ab": "1", "x-snme-c%25d": "2", "x-snme-e%41": "3"}
    )
    assert metadata.custom == {"a:b": "1", "c%d": "2", "eA": "3"}
