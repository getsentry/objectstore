import base64
import os

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from objectstore_client import presign, utils
from objectstore_client.auth import Permission, SecretKey, TokenGenerator
from objectstore_client.scope import Scope

TEST_EDDSA_PRIVKEY_PATH = os.path.join(os.path.dirname(__file__), "ed25519.private.pem")
TEST_EDDSA_PUBKEY_PATH = os.path.join(os.path.dirname(__file__), "ed25519.public.pem")


def _key() -> SecretKey:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        return SecretKey("test_kid", f.read())


def test_token_generator_signs_with_key_and_defaults() -> None:
    generator = TokenGenerator(_key(), permissions=[Permission.OBJECT_READ])
    token = generator.token_for_scope("testing", Scope(org=1))

    assert jwt.get_unverified_header(token)["kid"] == "test_kid"
    with open(TEST_EDDSA_PUBKEY_PATH) as f:
        claims = jwt.decode(token, f.read(), algorithms=["EdDSA"])
    assert claims["res"]["os:usecase"] == "testing"
    assert claims["permissions"] == [Permission.OBJECT_READ]


def test_token_generator_rejects_escalated_permissions() -> None:
    generator = TokenGenerator(_key(), permissions=[Permission.OBJECT_READ])
    with pytest.raises(ValueError, match="not granted to this token generator"):
        generator.token_for_scope(
            "testing", Scope(org=1), permissions=[Permission.OBJECT_WRITE]
        )


def test_sign_canonical_form_roundtrips_with_public_key() -> None:
    with open(TEST_EDDSA_PUBKEY_PATH, "rb") as f:
        public_key = load_pem_public_key(f.read())
    assert isinstance(public_key, Ed25519PublicKey)

    canonical = presign.build_canonical_form(
        "GET",
        utils.encode_path("/v1/objects/testing/_/key"),
        utils.encode_query("os_kid=test_kid&os_duration=3600"),
    )
    signature_b64 = _key().signature_for_canonical_form(canonical)

    # Decode the base64url-no-pad signature and verify with the public key.
    padding = "=" * (-len(signature_b64) % 4)
    signature = base64.urlsafe_b64decode(signature_b64 + padding)
    assert len(signature) == 64
    public_key.verify(signature, canonical.encode())  # raises on mismatch
