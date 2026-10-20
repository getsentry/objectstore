"""Unit tests for the pre-signed URL signing utilities."""

import base64
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from objectstore_client import Client, Usecase, presign, utils
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
        "GET", utils.encode_path(path), utils.encode_query(SAMPLE_QUERY)
    )
    assert canonical == (
        "GET\n"
        "/v1/objects/testing/org=17;project=42/foo/bar\n"
        "os_duration=3600&os_kid=relay&os_timestamp=1985-04-12T23:20:50.52Z"
    )


def test_canonical_form_duplicate_keys_preserved_and_sorted() -> None:
    canonical = presign.build_canonical_form(
        "GET",
        utils.encode_path("/v1/objects/testing/_/key"),
        utils.encode_query("dup=b&dup=a&x=1"),
    )
    assert canonical == "GET\n/v1/objects/testing/_/key\ndup=a&dup=b&x=1"


def test_head_is_normalized_to_get() -> None:
    path = utils.encode_path("/v1/objects/testing/_/key")
    query = utils.encode_query(SAMPLE_QUERY)
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
        utils.encode_path("/v1/objects/testing/_/key"),
        utils.encode_query(SAMPLE_QUERY),
    )
    signature_b64 = key.signature_for_canonical_form(canonical)

    # Decode the base64url-no-pad signature and verify with the public key.
    padding = "=" * (-len(signature_b64) % 4)
    signature = base64.urlsafe_b64decode(signature_b64 + padding)
    assert len(signature) == 64
    public_key.verify(signature, canonical.encode())  # raises on mismatch


@pytest.mark.parametrize("key", ["café/looks%20encoded&+", "nested/space key"])
def test_response_options_are_encoded_and_signed(key: str) -> None:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        secret = SecretKey("test_kid", f.read())
    session = Client("https://example.com/prefix", token=secret).session(
        Usecase("testing"), org=1
    )
    cache_control = 'private, max-age=3600, x="a&b=+%?#"'
    with patch("objectstore_client.client.datetime") as clock:
        clock.now.return_value = datetime(2026, 4, 20, 13, 37, tzinfo=UTC)
        legacy = session.presigned_object_url("GET", key)
        explicit_none = session.presigned_object_url(
            "GET", key, response_content_disposition=None, response_cache_control=None
        )
        url = session.presigned_object_url(
            "GET",
            key,
            response_content_disposition="inline",
            response_cache_control=cache_control,
        )
    assert legacy == explicit_none
    legacy_path = utils.encode_path(f"/prefix/v1/objects/testing/org=1/{key}")
    legacy_query = "os_kid=test_kid&os_timestamp=2026-04-20T13:37:00Z&os_duration=3600"
    legacy_signature = secret.signature_for_canonical_form(
        presign.build_canonical_form("GET", legacy_path, legacy_query)
    )
    assert (
        legacy
        == f"https://example.com:443{legacy_path}?{legacy_query}&os_sig={legacy_signature}"
    )
    parsed = urlsplit(url)
    assert parsed.path == urlsplit(legacy).path
    query = parse_qs(parsed.query)
    assert query["response-content-disposition"] == ["inline"]
    assert query["response-cache-control"] == [cache_control]
    assert len(query) == 6
    assert "%26" in parsed.query and "%2B" in parsed.query
    canonical = presign.build_canonical_form("GET", parsed.path, parsed.query)
    assert secret.signature_for_canonical_form(canonical) == query["os_sig"][0]
    altered = parsed.query.replace("=inline", "=attachment")
    assert (
        secret.signature_for_canonical_form(
            presign.build_canonical_form("GET", parsed.path, altered)
        )
        != query["os_sig"][0]
    )


def test_legacy_canonical_vector() -> None:
    path = "/prefix/v1/objects/testing/org=1;project=2/caf%C3%A9/looks%2520encoded&+"
    query = (
        "unknown=b&os_sig=ignored&dup=b&&OS_KID=test&dup=a&flag"
        "&encoded=a%26b%3D%2B&unknown=a"
    )
    expected = (
        f"GET\n{path}\n"
        "dup=a&dup=b&encoded=a%26b%3D%2B&flag&os_kid=test&unknown=a&unknown=b"
    )
    assert presign.build_canonical_form("HEAD", path, query) == expected
    signature = Ed25519PrivateKey.from_private_bytes(bytes([7]) * 32).sign(
        expected.encode()
    )
    assert base64.urlsafe_b64encode(signature).decode().rstrip("=") == (
        "2_8Qv-7PKMDMd-ku1kF33Xh8aPXjLj3QaIE_nLJuaDUGxtWUDQzCiU6Uz0Mb1zgH"
        "tfsCTB6CcQoZitooeJgmBw"
    )


def test_jwt_url_options_roundtrip_without_binding_token() -> None:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        secret = SecretKey("test_kid", f.read())
    session = Client("https://example.com/prefix", token=secret).session(
        Usecase("testing")
    )
    with patch.object(session, "mint_token", return_value="same.jwt.token"):
        legacy = session.object_url("key", token_validity=timedelta(minutes=5))
        url = session.object_url(
            "key",
            token_validity=timedelta(minutes=5),
            response_content_disposition="inline",
            response_cache_control='private, x="&=+"',
        )
    assert parse_qs(urlsplit(url).query) == {
        "os_auth": ["same.jwt.token"],
        "response-content-disposition": ["inline"],
        "response-cache-control": ['private, x="&=+"'],
    }
    assert legacy.endswith("?os_auth=same.jwt.token")
    assert session.object_url(
        "key", response_content_disposition=None
    ) == session.object_url("key")
