"""Object key validation and encoding.

This module provides functions for validating and encoding object keys
according to RFC 3986 and the objectstore specification.
"""

from __future__ import annotations

import re
import string

# Maximum length of an object key in ASCII characters (including percent encoding).
MAX_KEY_LENGTH = 128

# RFC 3986 unreserved characters: ALPHA / DIGIT / "-" / "." / "_" / "~"
UNRESERVED_CHARS = frozenset(string.ascii_letters + string.digits + "-._~")

# RFC 3986 reserved characters (gen-delims / sub-delims)
# gen-delims: ":" / "/" / "?" / "#" / "[" / "]" / "@"
# sub-delims: "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
RESERVED_CHARS = frozenset(":/?#[]@!$&'()*+,;=")

# Pattern for matching percent-encoded sequences
PERCENT_PATTERN = re.compile(r"%([0-9A-Fa-f]{2})")


class InvalidKeyError(Exception):
    """Exception raised when an object key is invalid."""

    pass


def encode_key(raw: str) -> str:
    """
    Encode a raw (unencoded) key string for use in API requests.

    This is intended for client-side use where users provide human-readable keys.
    Reserved characters will be automatically percent-encoded.

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

    encoded_parts = []
    for c in raw:
        # Check ASCII
        if ord(c) > 127:
            raise InvalidKeyError(f"key contains non-ASCII character '{c}'")

        if c in UNRESERVED_CHARS:
            encoded_parts.append(c)
        elif c == "%":
            # Percent sign itself needs to be encoded
            encoded_parts.append("%25")
        elif c in RESERVED_CHARS or not c.isprintable():
            # Encode reserved chars and non-printable ASCII
            encoded_parts.append(f"%{ord(c):02X}")
        else:
            # Other printable ASCII that's not reserved or unreserved
            # These should also be encoded for safety
            encoded_parts.append(f"%{ord(c):02X}")

    encoded = "".join(encoded_parts)

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

    normalized_parts = []
    i = 0
    while i < len(encoded):
        c = encoded[i]

        # Check ASCII
        if ord(c) > 127:
            raise InvalidKeyError(f"key contains non-ASCII character '{c}'")

        if c == "%":
            # Parse percent-encoded sequence
            if i + 2 >= len(encoded):
                raise InvalidKeyError("key contains invalid percent encoding")

            hex_chars = encoded[i + 1 : i + 3]
            if not all(c in "0123456789ABCDEFabcdef" for c in hex_chars):
                raise InvalidKeyError("key contains invalid percent encoding")

            byte_val = int(hex_chars, 16)
            decoded_char = chr(byte_val)

            if decoded_char in UNRESERVED_CHARS:
                # Decode unreserved characters
                normalized_parts.append(decoded_char)
            else:
                # Keep reserved/other characters encoded, with uppercase hex
                normalized_parts.append(f"%{hex_chars.upper()}")

            i += 3
        elif c in RESERVED_CHARS:
            # Literal reserved character is not allowed
            raise InvalidKeyError(
                f"key contains invalid character '{c}' (must be percent-encoded)"
            )
        elif c in UNRESERVED_CHARS:
            normalized_parts.append(c)
            i += 1
        elif not c.isprintable() or c == " ":
            # Non-printable characters and spaces must be encoded
            raise InvalidKeyError(
                f"key contains invalid character '{c!r}' (must be percent-encoded)"
            )
        else:
            # Other ASCII characters - allow them through
            normalized_parts.append(c)
            i += 1

    return "".join(normalized_parts)
