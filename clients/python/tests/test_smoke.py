import base64
import os
from datetime import timedelta

import jwt
from objectstore_client import Client, Usecase
from objectstore_client.auth import SecretKey

TEST_EDDSA_KID = "test_kid"
TEST_EDDSA_PRIVKEY_PATH = (
    os.path.dirname(os.path.realpath(__file__)) + "/ed25519.private.pem"
)


def test_imports() -> None:
    import objectstore_client  # noqa: F401
    from objectstore_client import client, metadata, metrics  # noqa: F401


def test_object_url() -> None:
    client = Client("http://127.0.0.1:8888/")
    session = client.session(
        Usecase("testing"), org=12345, project=1337, app_slug="email_app"
    )

    assert (
        session.object_url("foo/bar")
        == "http://127.0.0.1:8888/v1/objects/testing/org=12345;project=1337;app_slug=email_app/foo/bar"
    )


def test_object_url_with_base_path() -> None:
    client = Client("http://127.0.0.1:8888/api/prefix")
    session = client.session(Usecase("testing"), org=12345, project=1337)

    assert (
        session.object_url("foo/bar")
        == "http://127.0.0.1:8888/api/prefix/v1/objects/testing/org=12345;project=1337/foo/bar"
    )


def test_object_url_read_only_token() -> None:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        secret_key = SecretKey(TEST_EDDSA_KID, f.read())
    client = Client("http://127.0.0.1:8888/", token=secret_key)
    session = client.session(Usecase("testing"), org=12345, project=1337)

    url = session.object_url("foo/bar", read_only_token_validity=timedelta(minutes=5))

    base, _, query = url.partition("?")
    assert base == (
        "http://127.0.0.1:8888/v1/objects/testing/org=12345;project=1337/foo/bar"
    )
    param, _, encoded = query.partition("=")
    assert param == "os_auth"

    # The value is a base64url-encoded (unpadded) JWT scoped to this session
    # with a read-only permission.
    token = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode()
    claims = jwt.decode(token, options={"verify_signature": False})
    assert claims["res"] == {
        "os:usecase": "testing",
        "org": "12345",
        "project": "1337",
    }
    assert claims["permissions"] == ["object.read"]
