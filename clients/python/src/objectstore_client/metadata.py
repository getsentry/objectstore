from __future__ import annotations

import itertools
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, TypeVar, cast

from objectstore_client.utils import decode_header_value

Compression = Literal["zstd"] | Literal["none"]

HEADER_EXPIRATION = "x-sn-expiration"
HEADER_TIME_CREATED = "x-sn-time-created"
HEADER_TIME_EXPIRES = "x-sn-time-expires"
HEADER_ORIGIN = "x-sn-origin"
HEADER_FILENAME = "x-sn-filename"
HEADER_SIZE = "x-sn-size"
HEADER_META_PREFIX = "x-snme-"


@dataclass
class TimeToIdle:
    delta: timedelta


@dataclass
class TimeToLive:
    delta: timedelta


ExpirationPolicy = TimeToIdle | TimeToLive


@dataclass
class Metadata:
    content_type: str | None
    compression: Compression | None
    expiration_policy: ExpirationPolicy | None
    time_created: datetime | None
    """
    Timestamp indicating when the object was created or the last time it was replaced.

    This means that a PUT request to an existing object causes this value to be bumped.
    This field is computed by the server, it cannot be set by clients.
    """

    time_expires: datetime | None
    """
    Timestamp indicating when the object will expire.

    When using a Time To Idle expiration policy, this value will reflect the expiration
    timestamp present prior to the current access to the object.

    This field is computed by the server, it cannot be set by clients.
    Use `expiration_policy` to set an expiration policy instead.
    """

    origin: str | None
    """
    The origin of the object, typically the IP address of the original source.

    This tracks where the payload was originally obtained from
    (e.g., the IP of a Sentry SDK or CLI).
    """

    filename: str | None
    """
    An optional filename associated with this object.

    When present, the server includes a Content-Disposition header in GET responses,
    prompting browsers and download tools to save the file under this name.
    """

    size: int | None
    """
    The size of the complete stored object in bytes.

    This is always the size of the whole object, even when only a range of it was
    requested. For a compressed object it is the compressed size, matching the bytes on
    the wire.

    This field is computed by the server, it cannot be set by clients.
    """

    custom: dict[str, str]

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> Metadata:
        content_type = "application/octet-stream"
        compression = None
        expiration_policy = None
        time_created = None
        time_expires = None
        origin = None
        filename = None
        size = None
        custom_metadata = {}

        for k, v in headers.items():
            if k == "content-type":
                content_type = v
            elif k == "content-encoding":
                compression = cast(Compression | None, v)
            elif k == HEADER_EXPIRATION:
                expiration_policy = parse_expiration(v)
            elif k == HEADER_TIME_CREATED:
                time_created = datetime.fromisoformat(v)
            elif k == HEADER_TIME_EXPIRES:
                time_expires = datetime.fromisoformat(v)
            elif k == HEADER_ORIGIN:
                origin = decode_header_value(v)
            elif k == HEADER_FILENAME:
                filename = decode_header_value(v)
            elif k == HEADER_SIZE:
                size = int(v)
            elif k.startswith(HEADER_META_PREFIX):
                custom_metadata[k[len(HEADER_META_PREFIX) :]] = decode_header_value(v)

        return Metadata(
            content_type=content_type,
            compression=compression,
            expiration_policy=expiration_policy,
            time_created=time_created,
            time_expires=time_expires,
            origin=origin,
            filename=filename,
            size=size,
            custom=custom_metadata,
        )


def format_expiration(expiration_policy: ExpirationPolicy) -> str:
    if isinstance(expiration_policy, TimeToIdle):
        return f"tti:{format_timedelta(expiration_policy.delta)}"
    elif isinstance(expiration_policy, TimeToLive):
        return f"ttl:{format_timedelta(expiration_policy.delta)}"


def parse_expiration(value: str) -> ExpirationPolicy | None:
    if value.startswith("tti:"):
        return TimeToIdle(parse_timedelta(value[4:]))
    elif value.startswith("ttl:"):
        return TimeToLive(parse_timedelta(value[4:]))

    return None


def format_timedelta(delta: timedelta) -> str:
    """
    Formats a duration in the wire format, such as `400d 1m 30s`.

    Days are the largest unit, components that are zero are omitted, and a zero duration
    is written as `0s`. Any sub-second remainder is truncated.
    """
    minutes, seconds = divmod(delta.seconds, 60)
    hours, minutes = divmod(minutes, 60)

    components = ((delta.days, "d"), (hours, "h"), (minutes, "m"), (seconds, "s"))
    output = " ".join(f"{value}{unit}" for value, unit in components if value)

    return output or "0s"


TIME_SPLIT = re.compile(r"[^\W\d_]+|\d+")


def parse_timedelta(delta: str) -> timedelta:
    """
    Parses a duration in the wire format, such as `400d 1m 30s`.

    Units are matched exactly, so anything outside the wire format raises a
    `ValueError` rather than being mistaken for a unit it merely starts with.
    """
    words = TIME_SPLIT.findall(delta)
    seconds = 0

    for num, unit in itertools_batched(words, n=2, strict=True):
        num = int(num)

        if unit == "d":
            multiplier = 86400
        elif unit == "h":
            multiplier = 3600
        elif unit == "m":
            multiplier = 60
        elif unit == "s":
            multiplier = 1
        else:
            raise ValueError(f"unknown time unit {unit!r} in duration {delta!r}")

        seconds += num * multiplier

    return timedelta(seconds=seconds)


T = TypeVar("T")


def itertools_batched(
    iterable: Iterable[T], n: int, strict: bool = False
) -> Iterator[tuple[T, ...]]:
    """
    Vendored version of `itertools.batched`, not available in Python 3.11.
    Batch data from the iterable into tuples of length n.
    The last batch may be shorter than n.
    If strict is true, will raise a ValueError if the final batch is shorter than n.
    Loops over the input iterable and accumulates data into tuples up to size n.
    The input is consumed lazily, just enough to fill a batch.
    The result is yielded as soon as the batch is full
    or when the input iterable is exhausted:
    """
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        if strict and len(batch) < n:
            raise ValueError("final batch is shorter than n")
        yield batch
