"""Unit tests for the pre-signed URL signing utilities.

These pin the canonical form against the Rust reference vectors
(``objectstore-types/src/presign.rs``) and, crucially, assert that our path/query
encoding matches byte-for-byte what urllib3 puts on the wire.
"""

import base64
import os

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from objectstore_client import presign
from objectstore_client.auth import SecretKey

# urllib3's real request-target encoder: the ground truth for wire encoding.
from urllib3.util.url import _encode_target  # type: ignore[attr-defined]

TEST_EDDSA_PRIVKEY_PATH = os.path.join(os.path.dirname(__file__), "ed25519.private.pem")
TEST_EDDSA_PUBKEY_PATH = os.path.join(os.path.dirname(__file__), "ed25519.public.pem")

# Raw query string as it would appear on the wire: unsorted, includes the
# os_sig that must be excluded. Mirrors `sample_query()` in presign.rs.
SAMPLE_QUERY = (
    "os_timestamp=1985-04-12T23:20:50.52Z"
    "&os_kid=relay"
    "&os_duration=3600"
    "&os_sig=should-be-excluded"
)


def test_canonical_form_full_query() -> None:
    # Base case: pins path (slashes, `=`, `;` unencoded), os_sig exclusion,
    # key lowercasing, and query sort order.
    path = "/v1/objects/testing/org=17;project=42/foo/bar"
    canonical = presign.build_canonical_form(
        "GET", presign.encode_path(path), presign.encode_query(SAMPLE_QUERY)
    )
    assert canonical == (
        "GET\n"
        "/v1/objects/testing/org=17;project=42/foo/bar\n"
        "os_duration=3600&os_kid=relay&os_timestamp=1985-04-12T23:20:50.52Z"
    )


def test_canonical_form_empty_query() -> None:
    canonical = presign.build_canonical_form(
        "GET", presign.encode_path("/v1/objects/testing/_/key"), ""
    )
    assert canonical == "GET\n/v1/objects/testing/_/key\n"


def test_canonical_form_duplicate_keys_preserved_and_sorted() -> None:
    canonical = presign.build_canonical_form(
        "GET",
        presign.encode_path("/v1/objects/testing/_/key"),
        presign.encode_query("dup=b&dup=a&x=1"),
    )
    assert canonical == "GET\n/v1/objects/testing/_/key\ndup=a&dup=b&x=1"


def test_head_is_normalized_to_get() -> None:
    path = presign.encode_path("/v1/objects/testing/_/key")
    query = presign.encode_query(SAMPLE_QUERY)
    assert presign.build_canonical_form(
        "HEAD", path, query
    ) == presign.build_canonical_form("GET", path, query)


# Keys that round-trip through urllib3's request-target encoder (no `?`/`#`,
# which urllib3 would treat as the query/fragment delimiter).
WIRE_SAFE_KEYS = [
    "simple",
    "hello world",
    "café",
    "emoji-😀-key",
    "plus+plus",
    "sub!$&'()*+,;=delims",
    "tilde~and.dot",
    "100%done",
    "already%20encoded",
    "nested/a/b/c",
    "trailing space ",
    "quote'apostrophe",
    "at@sign",
    "colon:and/slash",
]


@pytest.mark.parametrize("key", WIRE_SAFE_KEYS)
def test_encode_path_matches_urllib3_wire_encoding(key: str) -> None:
    # Our signed path encoding must equal exactly what urllib3 sends on the wire.
    path = f"/v1/objects/test/org=1/{key}"
    assert presign.encode_path(path) == _encode_target(path)


@pytest.mark.parametrize(
    ("key", "encoded"),
    [
        ("has?question", "has%3Fquestion"),
        ("has#hash", "has%23hash"),
    ],
)
def test_encode_path_encodes_query_and_fragment_delimiters(
    key: str, encoded: str
) -> None:
    # Unlike urllib3's target encoder (which would split on `?`/`#`), we encode
    # them so they can appear literally inside an object key.
    path = f"/v1/objects/test/org=1/{key}"
    assert presign.encode_path(path) == f"/v1/objects/test/org=1/{encoded}"


def test_sign_canonical_roundtrips_with_public_key() -> None:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        secret_key_pem = f.read()
    with open(TEST_EDDSA_PUBKEY_PATH, "rb") as f:
        public_key = load_pem_public_key(f.read())
    assert isinstance(public_key, Ed25519PublicKey)

    key = SecretKey("test_kid", secret_key_pem)

    canonical = presign.build_canonical_form(
        "GET",
        presign.encode_path("/v1/objects/testing/_/key"),
        presign.encode_query(SAMPLE_QUERY),
    )
    signature_b64 = key.sign_canonical(canonical)

    # Decode the base64url-no-pad signature and verify with the public key.
    padding = "=" * (-len(signature_b64) % 4)
    signature = base64.urlsafe_b64decode(signature_b64 + padding)
    assert len(signature) == 64
    public_key.verify(signature, canonical.encode())  # raises on mismatch


def test_sign_canonical_rejects_non_ed25519_key() -> None:
    # An RSA key is a valid PEM private key but not Ed25519.
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
    )

    rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = rsa_key.private_bytes(
        Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
    ).decode()

    key = SecretKey("test_kid", pem)
    with pytest.raises(ValueError, match="Ed25519"):
        key.sign_canonical("GET\n/x\n")
