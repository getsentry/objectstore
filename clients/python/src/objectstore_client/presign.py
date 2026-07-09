"""Utilities for pre-signed URLs.

A pre-signed URL lets a client that owns an Ed25519 keypair hand out a
time-limited URL that authorizes a specific request, without the recipient
needing an auth token.

See ``objectstore-types/src/presign.rs`` for details on the scheme.
"""

from datetime import timedelta

# urllib3 internals that produce the on-the-wire request target. These are the
# exact functions urllib3 applies in `HTTPConnectionPool.urlopen`, so signing
# their output guarantees a byte-for-byte match with what the client sends.
from urllib3.util.url import (  # type: ignore[attr-defined]
    _PATH_CHARS,
    _QUERY_CHARS,
    _encode_invalid_chars,
)

# Query parameter carrying the RFC 3339 time at which the request was signed.
PARAM_TIMESTAMP = "os_timestamp"

# Query parameter carrying the validity duration, in seconds.
PARAM_DURATION = "os_duration"

# Query parameter naming the key ID used to sign the request.
PARAM_KID = "os_kid"

# Query parameter carrying the base64url-encoded signature.
PARAM_SIG = "os_sig"

# Maximum validity accepted by the server for a pre-signed URL.
MAX_PRESIGN_DURATION = timedelta(days=7)

# HTTP methods that may be pre-signed. The server rejects all others.
SUPPORTED_METHODS = ("GET", "HEAD")


def encode_path(path: str) -> str:
    """Percent-encodes a request path the same way urllib3 sends it on the wire."""
    return _encode_invalid_chars(path, _PATH_CHARS)


def encode_query(query: str) -> str:
    """Percent-encodes a query string the same way urllib3 sends it on the wire."""
    return _encode_invalid_chars(query, _QUERY_CHARS)


def build_canonical_form(method: str, encoded_path: str, encoded_query: str) -> str:
    """Builds the canonical form of a request to be signed.

    ``encoded_path`` and ``encoded_query`` must already be percent-encoded as
    they will appear on the wire (see :func:`encode_path` / :func:`encode_query`).
    """
    normalized_method = "GET" if method.upper() == "HEAD" else method.upper()

    pairs = []
    for pair in encoded_query.split("&"):
        if not pair:
            continue
        key, sep, value = pair.partition("=")
        key = key.lower()
        if key == PARAM_SIG:
            continue
        pairs.append(f"{key}={value}" if sep else key)
    pairs.sort()
    canonical_query = "&".join(pairs)

    return f"{normalized_method}\n{encoded_path}\n{canonical_query}"
