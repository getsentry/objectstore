from objectstore_client import Client, Usecase
from objectstore_client.client import USER_AGENT


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


def test_object_url_https() -> None:
    client = Client("https://127.0.0.1:8888/")
    session = client.session(Usecase("testing"), org=12345, project=1337)

    assert (
        session.object_url("foo/bar")
        == "https://127.0.0.1:8888/v1/objects/testing/org=12345;project=1337/foo/bar"
    )


def test_object_url_ipv6() -> None:
    client = Client("http://[::1]:8888/")
    session = client.session(Usecase("testing"), org=12345, project=1337)

    assert (
        session.object_url("foo/bar")
        == "http://[::1]:8888/v1/objects/testing/org=12345;project=1337/foo/bar"
    )


def test_object_url_with_base_path() -> None:
    client = Client("http://127.0.0.1:8888/api/prefix")
    session = client.session(Usecase("testing"), org=12345, project=1337)

    assert (
        session.object_url("foo/bar")
        == "http://127.0.0.1:8888/api/prefix/v1/objects/testing/org=12345;project=1337/foo/bar"
    )


def test_object_url_empty_scope() -> None:
    client = Client("http://127.0.0.1:8888/")
    session = client.session(Usecase("testing"))

    assert (
        session.object_url("foo/bar")
        == "http://127.0.0.1:8888/v1/objects/testing/_/foo/bar"
    )


def test_default_user_agent() -> None:
    client = Client("http://127.0.0.1:8888/")

    assert client._pool.headers["User-Agent"] == USER_AGENT


def test_user_agent_can_be_overridden() -> None:
    client = Client(
        "http://127.0.0.1:8888/",
        connection_kwargs={"headers": {"User-Agent": "custom-agent/1.0"}},
    )

    assert client._pool.headers["User-Agent"] == "custom-agent/1.0"
