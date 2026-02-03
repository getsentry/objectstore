from __future__ import annotations

from urllib.parse import quote

MAX_KEY_LENGTH = 128


class InvalidKeyError(Exception):
    """Exception raised when an object key is invalid."""

    pass


def encode_key(raw: str) -> str:
    if not raw:
        raise InvalidKeyError("key must not be empty")

    for c in raw:
        if ord(c) > 127:
            raise InvalidKeyError(f"key contains non-ASCII character '{c}'")

    if len(raw) > MAX_KEY_LENGTH:
        raise InvalidKeyError(
            f"key exceeds maximum length of {MAX_KEY_LENGTH} characters "
            f"(got {len(raw)})"
        )

    return quote(raw, safe="")
