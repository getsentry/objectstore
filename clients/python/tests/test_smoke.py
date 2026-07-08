from datetime import timedelta

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from objectstore_client import Client, Usecase
from objectstore_client.auth import TokenGenerator


def _token_generator() -> TokenGenerator:
    pem = (
        Ed25519PrivateKey.generate()
        .private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
        .decode()
    )
    return TokenGenerator(kid="test-kid", secret_key=pem)


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


def test_presigned_url_brackets_ipv6_host() -> None:
    client = Client("http://[::1]:8888", token=_token_generator())
    session = client.session(Usecase("testing"), org=1)

    url = session.presigned_url("GET", "foo", duration=timedelta(hours=1))

    assert url.startswith("http://[::1]:8888/v1/objects/testing/org=1/foo?")


@pytest.mark.parametrize(
    "duration",
    [
        timedelta(0),
        timedelta(seconds=-1),
        timedelta(milliseconds=500),
    ],
)
def test_presigned_url_rejects_nonpositive_duration(duration: timedelta) -> None:
    client = Client("http://127.0.0.1:8888", token=_token_generator())
    session = client.session(Usecase("testing"), org=1)

    with pytest.raises(ValueError, match="too short"):
        session.presigned_url("GET", "foo", duration=duration)
