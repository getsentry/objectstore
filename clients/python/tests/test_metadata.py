from datetime import timedelta

import pytest
from objectstore_client.metadata import (
    TimeToIdle,
    TimeToLive,
    format_expiration,
    format_timedelta,
    parse_expiration,
    parse_timedelta,
)

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
