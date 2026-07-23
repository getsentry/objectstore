from objectstore_client import Client, Usecase


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
