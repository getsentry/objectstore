"""Object key validation and encoding.

This module provides functions for validating and encoding object keys
according to RFC 3986 and the objectstore specification.
"""

from __future__ import annotations

from urllib.parse import quote, unquote

# Maximum length of an object key in ASCII characters (including percent encoding).
MAX_KEY_LENGTH = 128

# RFC 3986 unreserved characters: ALPHA / DIGIT / "-" / "." / "_" / "~"
# These are the only characters that don't need percent-encoding.
UNRESERVED_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)


class InvalidKeyError(Exception):
    """Exception raised when an object key is invalid."""

    pass


def encode_key(raw: str) -> str:
    """
    Encode a raw (unencoded) key string for use in API requests.

    This is intended for client-side use where users provide human-readable keys.
    Reserved characters will be automatically percent-encoded using urllib.parse.quote.

    Args:
        raw: The raw key string to encode.

    Returns:
        The encoded key string.

    Raises:
        InvalidKeyError: If the key is empty, too long, or contains
            non-ASCII characters.

    Examples:
        >>> encode_key("my-object")
        'my-object'
        >>> encode_key("path/to/object")
        'path%2Fto%2Fobject'
        >>> encode_key("hello world")
        'hello%20world'
    """
    if not raw:
        raise InvalidKeyError("key must not be empty")

    # Check for non-ASCII characters (our spec requires ASCII only)
    for c in raw:
        if ord(c) > 127:
            raise InvalidKeyError(f"key contains non-ASCII character '{c}'")

    # Use urllib.parse.quote with safe='' to encode all reserved characters.
    # By default, quote() leaves unreserved chars (letters, digits, -._~) unencoded,
    # which matches RFC 3986.
    encoded = quote(raw, safe="")

    if len(encoded) > MAX_KEY_LENGTH:
        raise InvalidKeyError(
            f"key exceeds maximum length of {MAX_KEY_LENGTH} characters "
            f"(got {len(encoded)})"
        )

    return encoded


def validate_encoded_key(encoded: str) -> str:
    """
    Validate and normalize an already percent-encoded key string.

    This is intended for server-side use where keys come from HTTP requests
    and are already URL-encoded. The key will be validated and normalized.

    Normalization:
    - Percent-encoded unreserved characters are decoded (%41 -> A)
    - Percent-encoded reserved characters remain encoded (%2F stays %2F)
    - Percent hex digits are uppercased (%2f -> %2F)

    Args:
        encoded: The encoded key string to validate.

    Returns:
        The normalized key string.

    Raises:
        InvalidKeyError: If the key is invalid.

    Examples:
        >>> validate_encoded_key("path%2Fto%2Fobject")
        'path%2Fto%2Fobject'
        >>> validate_encoded_key("hello%41world")
        'helloAworld'
        >>> validate_encoded_key("path/to/object")  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
            ...
        InvalidKeyError: key contains invalid character '/'
    """
    if not encoded:
        raise InvalidKeyError("key must not be empty")

    if len(encoded) > MAX_KEY_LENGTH:
        raise InvalidKeyError(
            f"key exceeds maximum length of {MAX_KEY_LENGTH} characters "
            f"(got {len(encoded)})"
        )

    # Validate: check for literal reserved characters and non-ASCII
    i = 0
    while i < len(encoded):
        c = encoded[i]

        if ord(c) > 127:
            raise InvalidKeyError(f"key contains non-ASCII character '{c}'")

        if c == "%":
            # Validate percent-encoded sequence
            if i + 2 >= len(encoded):
                raise InvalidKeyError("key contains invalid percent encoding")

            hex_chars = encoded[i + 1 : i + 3]
            if not all(h in "0123456789ABCDEFabcdef" for h in hex_chars):
                raise InvalidKeyError("key contains invalid percent encoding")

            i += 3
        elif c not in UNRESERVED_CHARS:
            # Literal reserved/special character is not allowed
            raise InvalidKeyError(
                f"key contains invalid character '{c}' (must be percent-encoded)"
            )
        else:
            i += 1

    # Normalize: decode to raw string, then re-encode.
    # This normalizes %41 -> A (unreserved decoded) and %2f -> %2F (uppercase hex).
    raw = unquote(encoded)
    return quote(raw, safe="")
