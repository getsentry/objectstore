"""Pre-signed URL generation for Objectstore."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

PARAM_EXPIRES = "X-Os-Expires"
PARAM_KEY_ID = "X-Os-KeyId"
PARAM_SIGNATURE = "X-Os-Signature"

DEFAULT_PRESIGNED_EXPIRY = 300  # 5 minutes

# RFC 3986 unreserved characters (minus letters and digits which are always safe).
# Same character set as AWS Signature V4's ``UriEncode``.
_UNRESERVED_SAFE = "-_.~"


def presign_url(
    kid: str,
    secret_key: str,
    method: str,
    url: str,
    expires_in: int = DEFAULT_PRESIGNED_EXPIRY,
) -> str:
    """
    Generate a pre-signed URL for reading an object.

    The returned URL is valid for both GET and HEAD requests.

    Args:
        kid: The key ID that the Objectstore server uses to look up the public key.
        secret_key: An Ed25519 private key in PEM format.
        method: The HTTP method (``"GET"`` or ``"HEAD"``). Both produce the same
            signature since HEAD is semantically equivalent to GET.
        url: The object URL to sign (as returned by ``Session.object_url``).
        expires_in: How long the URL should remain valid, in seconds.
            Defaults to 300 (5 minutes).

    Returns:
        The pre-signed URL string including signature query parameters.

    Raises:
        ValueError: If the method is not GET or HEAD.
    """
    method_upper = method.upper()
    if method_upper not in ("GET", "HEAD"):
        raise ValueError(
            f"Unsupported method for pre-signed URL: {method}. "
            "Only GET and HEAD are supported."
        )

    private_key = load_pem_private_key(secret_key.encode("utf-8"), password=None)
    if not isinstance(private_key, Ed25519PrivateKey):
        raise ValueError("secret_key must be an Ed25519 private key")

    expires_at = datetime.now(tz=UTC) + timedelta(seconds=expires_in)
    expires_ts = int(expires_at.timestamp())

    # Parse URL and add query params
    parsed = urlparse(url)
    existing_params = parse_qsl(parsed.query, keep_blank_values=True)
    existing_params.append((PARAM_EXPIRES, str(expires_ts)))
    existing_params.append((PARAM_KEY_ID, kid))

    # Build URL without signature for canonical form computation
    url_without_sig = urlunparse(parsed._replace(query=urlencode(existing_params)))
    parsed_without_sig = urlparse(url_without_sig)

    # Build canonical form and sign
    canonical = _canonical_presigned_request(
        parsed_without_sig.path, parsed_without_sig.query
    )
    signature = private_key.sign(canonical.encode("utf-8"))
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")

    # Append signature
    existing_params.append((PARAM_SIGNATURE, sig_b64))
    return urlunparse(parsed._replace(query=urlencode(existing_params)))


def _canonical_presigned_request(path: str, query: str) -> str:
    """
    Build the canonical request string for pre-signed URL signing/verification.

    Uses a "decode then re-encode" approach: percent-decode the raw
    values, then re-encode with a strict canonical set (only ``A-Z a-z 0-9 - _ . ~``
    left unencoded, uppercase hex). This normalizes to a single deterministic
    representation regardless of how the original URL was encoded.

    The canonical form is::

        GET\\n{canonical_path}\\n{canonical_query}

    - Method is always ``GET`` (HEAD maps to GET).
    - Path is decoded then re-encoded.
    - Query params are decoded then re-encoded, sorted by encoded key,
      excluding ``X-Os-Signature``.
    """
    canonical_path = _canonical_encode(unquote(path))

    params = []
    for pair in query.split("&"):
        if not pair:
            continue
        if "=" not in pair:
            continue
        k, _, v = pair.partition("=")
        k = unquote(k)
        if k == PARAM_SIGNATURE:
            continue
        v = unquote(v)
        params.append((_canonical_encode(k), _canonical_encode(v)))

    params.sort(key=lambda x: x[0])

    query_str = "&".join(f"{k}={v}" for k, v in params)
    return f"GET\n{canonical_path}\n{query_str}"


def _canonical_encode(value: str) -> str:
    """Canonically encode a string: only ``A-Z a-z 0-9 - _ . ~`` left unencoded."""
    return quote(value, safe=_UNRESERVED_SAFE)
