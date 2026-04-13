"""Pre-signed URL generation for Objectstore."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

PARAM_EXPIRES = "X-Os-Expires"
PARAM_KEY_ID = "X-Os-KeyId"
PARAM_SIGNATURE = "X-Os-Signature"

DEFAULT_PRESIGNED_EXPIRY = 300  # 5 minutes


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

    The canonical form is::

        GET\\n{canonical_path}\\n{canonical_query}

    - Method is always ``GET`` (HEAD maps to GET).
    - Path uses the encoded URI path as received by the service.
    - Percent-encoded octets are normalized to uppercase hex digits.
    - Query params use the encoded keys and optional values from the URI, excluding
      ``X-Os-Signature``, sorted by encoded key.
    """
    canonical_path = _normalize_percent_encoding(path)

    params = []
    for pair in query.split("&"):
        if not pair:
            continue
        if "=" in pair:
            k, _, v = pair.partition("=")
            value = _normalize_percent_encoding(v)
        else:
            k = pair
            value = None
        k = _normalize_percent_encoding(k)
        if k == PARAM_SIGNATURE:
            continue
        params.append((k, value))

    params.sort(key=lambda x: (x[0], x[1]))

    query_str = "&".join(k if v is None else f"{k}={v}" for k, v in params)
    return f"GET\n{canonical_path}\n{query_str}"


def _normalize_percent_encoding(value: str) -> str:
    """Normalize percent-encoded octets to use uppercase hex digits."""
    chars: list[str] = []
    i = 0
    while i < len(value):
        if (
            value[i] == "%"
            and i + 2 < len(value)
            and value[i + 1].isalnum()
            and value[i + 1].lower() in "0123456789abcdef"
            and value[i + 2].isalnum()
            and value[i + 2].lower() in "0123456789abcdef"
        ):
            chars.append("%")
            chars.append(value[i + 1].upper())
            chars.append(value[i + 2].upper())
            i += 3
            continue

        chars.append(value[i])
        i += 1

    return "".join(chars)
