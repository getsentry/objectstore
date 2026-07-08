"""Utilities for signing pre-signed URLs.

A pre-signed URL lets a client that owns an Ed25519 keypair hand out a
time-limited URL that authorizes a specific request, without the recipient
needing an auth token.

This mirrors the server-side scheme defined in ``objectstore-types/src/presign.rs``.
The signature covers a canonical form of the request:

    <normalized method>\\n<path>\\n<canonical query string>

- normalized method: the uppercase HTTP method, with ``HEAD`` mapped to ``GET``;
- path: the request path, exactly as transmitted on the wire;
- canonical query string: every query parameter except ``os_sig``, with keys
  lowercased, sorted lexicographically, joined with ``&``.

The signature is Ed25519 over the canonical form's UTF-8 bytes, encoded as
base64url without padding, and carried in the ``os_sig`` query parameter.

Encoding note: the server verifies the signature against the raw (still
percent-encoded) path and query it receives, without normalizing them. The
client must therefore sign the *exact bytes* its HTTP client puts on the wire.
The Python client uses urllib3, whose ``HTTPConnectionPool.urlopen`` encodes
request targets with ``urllib3.util.url._encode_target``. We reuse those same
internal helpers here so the encoding we sign matches the wire encoding by
construction; a parity test guards against upstream changes.
"""

import base64
from datetime import timedelta

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

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
SUPPORTED_METHODS = ("GET", "HEAD", "DELETE")


def encode_path(path: str) -> str:
    """Percent-encodes a request path the same way urllib3 sends it on the wire.

    Unlike urllib3's full request-target encoder, this also encodes ``?`` and
    ``#`` (which are not in the path character set), so that those characters
    appearing inside an object key cannot be mistaken for the query or fragment
    delimiter.
    """
    return _encode_invalid_chars(path, _PATH_CHARS)


def encode_query(query: str) -> str:
    """Percent-encodes a query string the same way urllib3 sends it on the wire."""
    return _encode_invalid_chars(query, _QUERY_CHARS)


def build_canonical(method: str, encoded_path: str, encoded_query: str) -> str:
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


def sign_canonical(secret_key_pem: str, canonical: str) -> str:
    """Signs a canonical request with an Ed25519 private key.

    ``secret_key_pem`` is a PEM-encoded Ed25519 private key (the same key a
    :class:`~objectstore_client.auth.TokenGenerator` uses to sign JWTs). Returns
    the base64url-encoded (no padding) signature, suitable as the value of the
    ``os_sig`` query parameter.
    """
    key = load_pem_private_key(secret_key_pem.encode(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("pre-signed URLs require an Ed25519 private key")
    signature = key.sign(canonical.encode())
    return base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
