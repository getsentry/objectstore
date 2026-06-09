import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from collections.abc import Generator
from datetime import timedelta
from io import BytesIO
from pathlib import Path

import pytest
import urllib3
import zstandard
from objectstore_client import Client, Usecase
from objectstore_client.auth import Permission, TokenGenerator
from objectstore_client.errors import RequestError
from objectstore_client.metadata import TimeToLive
from objectstore_client.multipart import CompletePart, MultipartCompleteError
from objectstore_client.scope import Scope

TEST_EDDSA_KID: str = "test_kid"
TEST_EDDSA_PRIVKEY_PATH: str = (
    os.path.dirname(os.path.realpath(__file__)) + "/ed25519.private.pem"
)
TEST_EDDSA_PUBKEY_PATH: str = (
    os.path.dirname(os.path.realpath(__file__)) + "/ed25519.public.pem"
)
TEST_PRESIGNED_KID: str = "presigned-test"
TEST_PRESIGNED_SECRET: str = "presigned-secret-for-tests"


class UnrewindableStream(BytesIO):
    """Read-only stream that cannot report or restore position."""

    def seek(self, offset: int, whence: int = 0) -> int:
        raise OSError("stream is not seekable")

    def tell(self) -> int:
        raise OSError("stream does not expose a stable position")


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
            "OS__STORAGE__TYPE": "filesystem",
            "OS__STORAGE__PATH": self._tempdir,
            "OS__LOG__LEVEL": "trace",
            "OS__AUTH__ENFORCE": "true",
            "OS__AUTH__KEYS": env_key_map,
            "OS__AUTH__PRESIGNED__ACTIVE_KEY_ID": TEST_PRESIGNED_KID,
            "OS__AUTH__PRESIGNED__KEYS": (
                f'{{{TEST_PRESIGNED_KID}={{secrets=["{TEST_PRESIGNED_SECRET}"]}}}}'
            ),
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
    client = Client(
        server_url,
        token=TestTokenGenerator.get(),
    )
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

    with pytest.raises(RequestError) as exc_info:
        session.get(object_key)
    assert exc_info.value.status == 404


def test_head(server_url: str) -> None:
    client = Client(
        server_url,
        token=TestTokenGenerator.get(),
    )
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(test_usecase, org=42, project=1337)

    object_key = session.put(b"test data", origin="203.0.113.42")

    metadata = session.head(object_key)
    assert metadata is not None
    assert metadata.time_created is not None
    assert metadata.origin == "203.0.113.42"

    session.delete(object_key)

    assert session.head(object_key) is None


def test_presigned_url_get_and_head(server_url: str) -> None:
    client = Client(
        server_url,
        token=TestTokenGenerator.get(),
    )
    test_usecase = Usecase("test-usecase")
    session = client.session(test_usecase, org=42, project=1337)

    object_key = session.put(b"presigned data", compression="none")
    presigned_url = session.presigned_url(
        object_key, operation="GET", expiry_seconds=30
    )

    pool = urllib3.PoolManager()
    response = pool.request("GET", presigned_url)
    assert response.status == 200
    assert response.data == b"presigned data"

    response = pool.request("HEAD", presigned_url)
    assert response.status == 204


def test_full_cycle_with_origin(server_url: str) -> None:
    client = Client(
        server_url,
        token=TestTokenGenerator.get(),
    )
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(test_usecase, org=42, project=1337)

    object_key = session.put(b"test data", origin="203.0.113.42")
    assert object_key is not None

    retrieved = session.get(object_key)
    assert retrieved.payload.read() == b"test data"
    assert retrieved.metadata.origin == "203.0.113.42"


def test_full_cycle_uncompressed(server_url: str) -> None:
    client = Client(
        server_url,
        token=TestTokenGenerator.get(),
    )
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
    client = Client(
        server_url,
        token=TestTokenGenerator.get(),
    )
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
    client = Client(
        server_url,
        token=TestTokenGenerator.get(),
    )
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    # First create an object with one scope
    session = client.session(test_usecase, org=42, project=1337)
    object_key = session.put(b"test data")

    # Now make sure we can't fetch it
    session = client.session(test_usecase, org=42, project=9999)
    with pytest.raises(RequestError) as exc_info:
        session.get(object_key)
    assert exc_info.value.status == 404


def test_full_cycle_with_static_token(server_url: str) -> None:
    token_generator = TestTokenGenerator.get()
    token = token_generator.sign_for_scope("test-usecase", Scope(org=42, project=1337))

    client = Client(server_url, token=token)
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(test_usecase, org=42, project=1337)

    object_key = session.put(b"static token data")
    assert object_key is not None

    retrieved = session.get(object_key)
    assert retrieved.payload.read() == b"static token data"

    session.delete(object_key)

    with pytest.raises(RequestError) as exc_info:
        session.get(object_key)
    assert exc_info.value.status == 404


def test_fails_with_insufficient_auth_perms(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.create(permissions=[]))
    test_usecase = Usecase(
        "test-usecase",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )

    session = client.session(test_usecase, org=42, project=1337)

    with pytest.raises(RequestError) as exc_info:
        _object_key = session.put(b"test data")
    assert exc_info.value.status == 403


def test_read_timeout() -> None:
    # this server accepts the connection
    # (even though the backlog is 0 and we never call `accept`),
    # but will never reply with anything, thus causing a read timeout
    s = socket.create_server(("127.0.0.1", 0), backlog=0)
    addr = s.getsockname()
    url = f"http://127.0.0.1:{addr[1]}"

    client = Client(
        url,
        timeout_ms=500,
        token=TestTokenGenerator.get(),
    )
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


def test_connect_timeout() -> None:
    # Connect to a blackhole address that should not reply to SYN packets,
    # causing a connect timeout with the client's default connect timeout.
    # 10.255.255.1 is commonly unroutable in most environments.
    url = "http://10.255.255.1:9"

    # Do NOT set timeout_ms to ensure we exercise default timeouts
    client = Client(
        url,
        token=TestTokenGenerator.get(),
    )
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

    with pytest.raises(urllib3.exceptions.MaxRetryError):
        session.put(b"test data", compression="none")

    with pytest.raises(urllib3.exceptions.MaxRetryError):
        session.put(b"test data", compression="zstd")


def test_multipart_full_cycle_uncompressed(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(key="mp-uncompressed")
    assert upload.key == "mp-uncompressed"
    assert upload.upload_id

    part1 = upload.put_part(b"hello ", part_number=1, content_length=6)
    part2 = upload.put_part(b"world!", part_number=2, content_length=6)

    final_key = upload.complete([part1, part2])
    assert final_key == "mp-uncompressed"

    retrieved = session.get(final_key, decompress=False)
    assert retrieved.payload.read() == b"hello world!"


def test_multipart_full_cycle_compressed(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(
        key="mp-compressed",
        compression="zstd",
    )

    cctx = zstandard.ZstdCompressor()
    compressed_part1 = cctx.compress(b"hello ")
    compressed_part2 = cctx.compress(b"world!")

    part1 = upload.put_part(
        compressed_part1, part_number=1, content_length=len(compressed_part1)
    )
    part2 = upload.put_part(
        compressed_part2, part_number=2, content_length=len(compressed_part2)
    )

    final_key = upload.complete([part1, part2])

    # Verify raw compressed round-trip
    retrieved = session.get(final_key, decompress=False)
    assert retrieved.metadata.compression == "zstd"
    raw = retrieved.payload.read()
    assert raw == compressed_part1 + compressed_part2

    # Verify transparent decompression
    retrieved = session.get(final_key)
    assert retrieved.metadata.compression is None
    assert retrieved.payload.read() == b"hello world!"


def test_multipart_streaming_part_upload_uncompressed(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(key="mp-streaming-uncompressed")

    part1_payload = b"hello "
    part2_payload = b"world!"
    part1 = upload.put_part(
        UnrewindableStream(part1_payload),
        part_number=1,
        content_length=len(part1_payload),
    )
    part2 = upload.put_part(
        UnrewindableStream(part2_payload),
        part_number=2,
        content_length=len(part2_payload),
    )

    final_key = upload.complete([part1, part2])

    retrieved = session.get(final_key)
    assert retrieved.payload.read() == b"hello world!"


def test_multipart_streaming_part_upload_compressed(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(
        key="mp-streaming-compressed",
        compression="zstd",
    )

    cctx = zstandard.ZstdCompressor()
    compressed_part1 = cctx.compress(b"hello ")
    compressed_part2 = cctx.compress(b"world!")

    part1 = upload.put_part(
        UnrewindableStream(compressed_part1),
        part_number=1,
        content_length=len(compressed_part1),
    )
    part2 = upload.put_part(
        UnrewindableStream(compressed_part2),
        part_number=2,
        content_length=len(compressed_part2),
    )

    final_key = upload.complete([part1, part2])

    retrieved = session.get(final_key, decompress=False)
    assert retrieved.metadata.compression == "zstd"
    assert retrieved.payload.read() == compressed_part1 + compressed_part2

    retrieved = session.get(final_key)
    assert retrieved.metadata.compression is None
    assert retrieved.payload.read() == b"hello world!"


def test_multipart_server_generated_key(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload()
    assert upload.key

    part = upload.put_part(b"data", part_number=1, content_length=4)
    final_key = upload.complete([part])
    assert final_key

    retrieved = session.get(final_key)
    assert retrieved.payload.read() == b"data"


def test_multipart_list_parts(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(key="mp-list-parts")

    upload.put_part(b"part-two", part_number=2, content_length=8)
    upload.put_part(b"part-one", part_number=1, content_length=8)

    parts = upload.list_parts()
    assert len(parts) == 2

    p1 = next(p for p in parts if p.part_number == 1)
    p2 = next(p for p in parts if p.part_number == 2)
    assert p1.size == 8
    assert p2.size == 8

    upload.abort()


def test_multipart_abort(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(key="mp-abort")
    upload.put_part(b"some data", part_number=1, content_length=9)
    upload.abort()


def test_multipart_metadata_preserved(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(
        key="mp-metadata",
        content_type="text/plain",
        origin="203.0.113.42",
        metadata={"my-key": "my-value"},
    )

    part = upload.put_part(b"payload", part_number=1, content_length=7)
    final_key = upload.complete([part])

    retrieved = session.get(final_key)
    assert retrieved.metadata.content_type == "text/plain"
    assert retrieved.metadata.origin == "203.0.113.42"
    assert retrieved.metadata.custom.get("my-key") == "my-value"


def test_multipart_complete_with_bad_etag(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(key="mp-bad-etag")
    upload.put_part(b"real data", part_number=1, content_length=9)

    with pytest.raises(MultipartCompleteError) as exc_info:
        upload.complete([CompletePart(part_number=1, etag="bogus-etag")])

    assert exc_info.value.code
    assert exc_info.value.status == 200


def test_multipart_resume(server_url: str) -> None:
    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(key="mp-resume")
    saved_key = upload.key
    saved_upload_id = upload.upload_id

    upload.put_part(b"first", part_number=1, content_length=5)

    # Simulate resuming from saved state
    resumed = session.resume_multipart_upload(saved_key, saved_upload_id)
    assert resumed.key == saved_key
    assert resumed.upload_id == saved_upload_id

    resumed.put_part(b"second", part_number=2, content_length=6)

    existing = resumed.list_parts()
    assert len(existing) == 2

    final_key = resumed.complete(existing)

    retrieved = session.get(final_key)
    assert retrieved.payload.read() == b"firstsecond"


def test_multipart_concurrent_part_uploads(server_url: str) -> None:
    from concurrent.futures import ThreadPoolExecutor

    client = Client(server_url, token=TestTokenGenerator.get())
    usecase = Usecase(
        "test-usecase",
        compression="none",
        expiration_policy=TimeToLive(timedelta(days=1)),
    )
    session = client.session(usecase, org=42, project=1337)

    upload = session.initiate_multipart_upload(key="mp-concurrent")

    chunks = [f"chunk-{i}".encode() for i in range(8)]

    def put_part(part_number: int, data: bytes) -> CompletePart:
        return upload.put_part(data, part_number=part_number, content_length=len(data))

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(put_part, i + 1, chunk) for i, chunk in enumerate(chunks)
        ]
        parts = [f.result() for f in futures]

    final_key = upload.complete(parts)

    retrieved = session.get(final_key)
    assert retrieved.payload.read() == b"".join(chunks)
