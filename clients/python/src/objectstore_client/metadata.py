from __future__ import annotations

import itertools
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, cast

Compression = Literal["zstd"]

HEADER_EXPIRATION = "x-sn-expiration"
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
    custom: dict[str, str]

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> Metadata:
        content_type = "application/octet-stream"
        compression = None
        expiration_policy = None
        custom_metadata = {}

        for k, v in headers.items():
            if k == "content-type":
                content_type = v
            elif k == "content-encoding":
                compression = cast(Compression | None, v)
            elif k == HEADER_EXPIRATION:
                expiration_policy = parse_expiration(v)
            elif k.startswith(HEADER_META_PREFIX):
                custom_metadata[k[len(HEADER_META_PREFIX) :]] = v

        return Metadata(
            content_type=content_type,
            compression=compression,
            expiration_policy=expiration_policy,
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
    days = delta.days
    output = f"{days} days" if days else ""
    if seconds := delta.seconds:
        if output:
            output += " "
        output += f"{seconds} seconds"

    return output


TIME_SPLIT = re.compile(r"[^\W\d_]+|\d+")


def parse_timedelta(delta: str) -> timedelta:
    words = TIME_SPLIT.findall(delta)
    seconds = 0

    for num, unit in itertools.batched(words, n=2, strict=True):
        num = int(num)
        multiplier = 0

        if unit.startswith("w"):
            multiplier = 86400 * 7
        elif unit.startswith("d"):
            multiplier = 86400
        elif unit.startswith("h"):
            multiplier = 3600
        elif unit.startswith("m") and not unit.startswith("ms"):
            multiplier = 60
        elif unit.startswith("s"):
            multiplier = 1

        seconds += num * multiplier

    return timedelta(seconds=seconds)
