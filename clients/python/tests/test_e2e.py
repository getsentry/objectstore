import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path

import pytest
import urllib3
import zstandard
from objectstore_client import Client, Usecase
from objectstore_client.auth import Permission, TokenGenerator
from objectstore_client.client import RequestError
from objectstore_client.metadata import TimeToLive

TEST_EDDSA_KID: str = "test_kid"
TEST_EDDSA_PRIVKEY_PATH: str = (
    os.path.dirname(os.path.realpath(__file__)) + "/ed25519.private.pem"
)
TEST_EDDSA_PUBKEY_PATH: str = (
    os.path.dirname(os.path.realpath(__file__)) + "/ed25519.public.pem"
)


class TestTokenGenerator:
    _instance: TokenGenerator | None = None

    @classmethod
    def create(
        cls, expiry_seconds: int = 60, permissions: list[Permission] = Permission.max()
    ) -> TokenGenerator:
        with open(TEST_EDDSA_PRIVKEY_PATH) as f:
            return TokenGenerator(TEST_EDDSA_KID, f.read(), expiry_seconds, permissions)

    @classmethod
    def get(cls) -> TokenGenerator:
        if not cls._instance:
            with open(TEST_EDDSA_PRIVKEY_PATH) as f:
                cls._instance = TokenGenerator(TEST_EDDSA_KID, f.read())
        return cls._instance


class Server:
    """Manages an instance of the Objectstore server running in a subprocess."""

    def __init__(self) -> None:
        self._tempdir: str | None = None
        self._process: subprocess.Popen[bytes] | None = None

    def _wait_for_healthcheck(self) -> None:
        pool = urllib3.connectionpool.connection_from_url(self._url)
        max_attempts = 20
        sleep_seconds = 0.1

        for _ in range(max_attempts):
            try:
                response = pool.request("GET", "/health", timeout=0.1)
                if response.status == 200:
                    return
            except Exception:
                pass
            time.sleep(sleep_seconds)

        raise RuntimeError(
            f"Server failed to start within {max_attempts * sleep_seconds} seconds"
        )

    def start(self) -> str:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

        addr = f"127.0.0.1:{port}"

        server_bin = (
            Path(__file__).resolve().parents[3] / "target" / "debug" / "objectstore"
        )
        if not server_bin.exists():
            pytest.fail(
                "objectstore-server binary not found. "
                "Please run `cargo build --locked` first"
            )

        self._url = f"http://{addr}"
        self._tempdir = tempfile.mkdtemp()

        # this messy format is how Figment supports map structures in env variables
        env_key_map = f'{{{TEST_EDDSA_KID}={{key_files=["{TEST_EDDSA_PUBKEY_PATH}"],max_permissions=["object.read","object.write","object.delete"]}}}}'  # noqa: E501

        env = {
            **os.environ,
            "OS__HTTP_ADDR": addr,
            "OS__HIGH_VOLUME_STORAGE__TYPE": "filesystem",
            "OS__HIGH_VOLUME_STORAGE__PATH": f"{self._tempdir}/high-volume",
            "OS__LONG_TERM_STORAGE__TYPE": "filesystem",
            "OS__LONG_TERM_STORAGE__PATH": f"{self._tempdir}/long-term",
            "OS__LOG__LEVEL": "trace",
            "OS__AUTH__ENFORCE": "true",
            "OS__AUTH__KEYS": env_key_map,
        }

        self._process = subprocess.Popen([str(server_bin), "run"], env=env)
        self._wait_for_healthcheck()

        return self._url

    def stop(self) -> None:
        if self._process:
            self._process.send_signal(signal.SIGINT)
            self._process.wait()

        if self._tempdir:
            shutil.rmtree(self._tempdir, ignore_errors=True)


@pytest.fixture(scope="session")
def server_url() -> Generator[str]:
    server = Server()
    try:
        base_url = server.start()
        yield base_url
    finally:
        server.stop()


def test_full_cycle(server_url: str) -> None:
    client = Client(server_url, token_generator=TestTokenGenerator.get())
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(test_usecase, org=42, project=1337)

    object_key = session.put(b"test data")
    assert object_key is not None

    retrieved = session.get(object_key)
    assert retrieved.payload.read() == b"test data"
    assert retrieved.metadata.time_created is not None

    new_key = session.put(b"new data", key=object_key)
    assert new_key == object_key
    retrieved = session.get(object_key)
    assert retrieved.payload.read() == b"new data"

    session.delete(object_key)

    with pytest.raises(RequestError, check=lambda e: e.status == 404):
        session.get(object_key)


def test_full_cycle_uncompressed(server_url: str) -> None:
    client = Client(server_url, token_generator=TestTokenGenerator.get())
    test_usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(test_usecase, my_scope=42, my_nested_scope="something!")

    data = b"test data"
    compressor = zstandard.ZstdCompressor()
    compressed_data = compressor.compress(data)

    object_key = session.put(compressed_data, compression="none")
    assert object_key is not None

    retrieved = session.get(object_key)
    retrieved_data = retrieved.payload.read()

    assert retrieved_data == compressed_data

    decompressor = zstandard.ZstdDecompressor()
    decompressed_data = decompressor.decompress(retrieved_data)

    assert decompressed_data == data


def test_full_cycle_structured_key(server_url: str) -> None:
    client = Client(server_url, token_generator=TestTokenGenerator.get())
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(test_usecase, org=42, project=1337)
    object_key = session.put(b"test data", key="1/shard-0.json")
    assert object_key == "1/shard-0.json"

    retrieved = session.get(object_key)
    assert retrieved.payload.read() == b"test data"


def test_not_found_with_different_scope(server_url: str) -> None:
    client = Client(server_url, token_generator=TestTokenGenerator.get())
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    # First create an object with one scope
    session = client.session(test_usecase, org=42, project=1337)
    object_key = session.put(b"test data")

    # Now make sure we can't fetch it
    session = client.session(test_usecase, org=42, project=9999)
    with pytest.raises(RequestError, check=lambda e: e.status == 404):
        session.get(object_key)


def test_fails_with_insufficient_auth_perms(server_url: str) -> None:
    client = Client(
        server_url, token_generator=TestTokenGenerator.create(permissions=[])
    )
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(test_usecase, org=42, project=1337)

    # TODO: When server errors cause appropriate status codes to be returned,
    # ensure this is 403
    with pytest.raises(RequestError, check=lambda e: e.status == 500):
        _object_key = session.put(b"test data")


def test_connect_timeout() -> None:
    # this server accepts the connection
    # (even though the backlog is 0 and we never call `accept`),
    # but will never reply with anything, thus causing a read timeout
    s = socket.create_server(("127.0.0.1", 0), backlog=0)
    addr = s.getsockname()
    url = f"http://127.0.0.1:{addr[1]}"

    client = Client(url, token_generator=TestTokenGenerator.get())
    test_usecase = Usecase(
        "test-usecase",
        compression="zstd",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(
        test_usecase, org=12345, project=1337, app_slug="email_app"
    )

    with pytest.raises(urllib3.exceptions.MaxRetryError):
        session.get("foo")
