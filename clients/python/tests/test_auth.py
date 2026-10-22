import os

import jwt
import pytest
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
