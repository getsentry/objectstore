import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from collections.abc import Generator
from pathlib import Path

import pytest
import urllib3
from objectstore_client import ClientBuilder
from objectstore_client.client import ClientError


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

        env = {
            **os.environ,
            "FSS_HTTP_ADDR": addr,
            "FSS_high_volume_storage__TYPE": "filesystem",
            "FSS_high_volume_storage__PATH": self._tempdir,
            "FSS_long_term_storage__TYPE": "filesystem",
            "FSS_long_term_storage__PATH": self._tempdir,
        }

        self._process = subprocess.Popen([str(server_bin)], env=env)
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
    client = ClientBuilder(server_url, "test-usecase").for_organization(12345)
    data = b"test data"

    object_key = client.put(data)
    assert object_key is not None

    retrieved = client.get(object_key)
    assert retrieved.payload.read() == data

    client.delete(object_key)

    with pytest.raises(ClientError, check=lambda e: e.status == 404):
        client.get(object_key)
