"""Unit tests for the pre-signed URL signing utilities."""

import base64
import os

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from objectstore_client import presign, utils
from objectstore_client.auth import SecretKey

TEST_EDDSA_PRIVKEY_PATH = os.path.join(os.path.dirname(__file__), "ed25519.private.pem")
TEST_EDDSA_PUBKEY_PATH = os.path.join(os.path.dirname(__file__), "ed25519.public.pem")

# Raw query string as it would appear on the wire: unsorted, includes the
# os_sig that must be excluded.
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
        "GET", utils._encode_path(path), utils._encode_query(SAMPLE_QUERY)
    )
    assert canonical == (
        "GET\n"
        "/v1/objects/testing/org=17;project=42/foo/bar\n"
        "os_duration=3600&os_kid=relay&os_timestamp=1985-04-12T23:20:50.52Z"
    )


def test_canonical_form_duplicate_keys_preserved_and_sorted() -> None:
    canonical = presign.build_canonical_form(
        "GET",
        utils._encode_path("/v1/objects/testing/_/key"),
        utils._encode_query("dup=b&dup=a&x=1"),
    )
    assert canonical == "GET\n/v1/objects/testing/_/key\ndup=a&dup=b&x=1"


def test_head_is_normalized_to_get() -> None:
    path = utils._encode_path("/v1/objects/testing/_/key")
    query = utils._encode_query(SAMPLE_QUERY)
    assert presign.build_canonical_form(
        "HEAD", path, query
    ) == presign.build_canonical_form("GET", path, query)


def test_sign_canonical_form_roundtrips_with_public_key() -> None:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        secret_key_pem = f.read()
    with open(TEST_EDDSA_PUBKEY_PATH, "rb") as f:
        public_key = load_pem_public_key(f.read())
    assert isinstance(public_key, Ed25519PublicKey)

    key = SecretKey("test_kid", secret_key_pem)

    canonical = presign.build_canonical_form(
        "GET",
        utils._encode_path("/v1/objects/testing/_/key"),
        utils._encode_query(SAMPLE_QUERY),
    )
    signature_b64 = key.signature_for_canonical_form(canonical)

    # Decode the base64url-no-pad signature and verify with the public key.
    padding = "=" * (-len(signature_b64) % 4)
    signature = base64.urlsafe_b64decode(signature_b64 + padding)
    assert len(signature) == 64
    public_key.verify(signature, canonical.encode())  # raises on mismatch
