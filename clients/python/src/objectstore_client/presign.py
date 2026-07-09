"""Utilities for pre-signed URLs.

A pre-signed URL lets a client that owns an Ed25519 keypair hand out a
time-limited URL that authorizes a specific request, without the recipient
needing an auth token.

See ``objectstore-types/src/presign.rs`` for details on the scheme.
"""

from datetime import timedelta
from urllib.parse import quote

# Characters left unescaped when percent-encoding, expressed in RFC 3986 terms.
# `urllib.parse.quote` already leaves the *unreserved* set (ALPHA / DIGIT /
# "-._~") untouched, so `safe` only lists the extra characters permitted in a
# path or query without escaping:
#   - sub-delims: "!$&'()*+,;="
#   - path extras: ":" "@" "/"  (the pchar set plus the "/" segment separator)
#   - query also allows "?"
# This is the same set urllib3 leaves unescaped, so the encoding is a fixed
# point of the HTTP client that ultimately sends the URL and survives on the
# wire verbatim. It is also byte-for-byte identical to the Rust client's
# `percent_encoding` set (see `objectstore-types/src/presign.rs`), so the two
# clients produce the same signed bytes for any key.
_PATH_SAFE = "/:@!$&'()*+,;="
_QUERY_SAFE = _PATH_SAFE + "?"

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
    """Percent-encodes a request path as it will appear on the wire.

    Leaves the RFC 3986 unreserved set, sub-delims, and ``:`` ``@`` ``/``
    unescaped (see :data:`_PATH_SAFE`); everything else is percent-encoded.
    """
    return quote(path, safe=_PATH_SAFE)


def encode_query(query: str) -> str:
    """Percent-encodes a query string as it will appear on the wire.

    Same set as :func:`encode_path`, additionally leaving ``?`` unescaped
    (see :data:`_QUERY_SAFE`).
    """
    return quote(query, safe=_QUERY_SAFE)


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
