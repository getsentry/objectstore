from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import SEEK_CUR, SEEK_END, BytesIO, UnsupportedOperation
from pathlib import Path
from threading import Thread
from typing import IO, cast

import pytest
import urllib3
from objectstore_client._resumable import BoundedReader
from urllib3.util.request import rewind_body, set_file_position

PAYLOAD = b"prefix:abcdefgh:suffix"


@pytest.fixture(params=["buffer", "file", "unbuffered_file", "http"])
def source(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[IO[bytes]]:
    if request.param == "buffer":
        with BytesIO(PAYLOAD) as stream:
            yield stream
    elif request.param in ("file", "unbuffered_file"):
        path = tmp_path / "payload"
        path.write_bytes(PAYLOAD)
        buffering = 0 if request.param == "unbuffered_file" else -1
        with path.open("rb", buffering=buffering) as file:
            yield file
    else:

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Length", str(len(PAYLOAD)))
                self.end_headers()
                self.wfile.write(PAYLOAD)

            def log_message(self, format: str, *args: object) -> None:
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = Thread(target=server.serve_forever)
            thread.start()
            pool = urllib3.HTTPConnectionPool("127.0.0.1", server.server_port)
            try:
                response = pool.request("GET", "/", preload_content=False)
                try:
                    yield cast(IO[bytes], response)
                finally:
                    response.close()
            finally:
                pool.close()
                server.shutdown()
                thread.join()


def test_bounded_reader_slice(source: IO[bytes]) -> None:
    assert source.read(7) == b"prefix:"
    reader = BoundedReader(source, 8)
    assert reader.read(0) == b""
    assert reader.read(3) == b"abc"
    assert reader.read(100) == b"defgh"
    assert reader.read() == b""
    reader.close()
    assert not source.closed
    assert source.read() == b":suffix"


def test_bounded_reader_empty(source: IO[bytes]) -> None:
    reader = BoundedReader(source, 0)
    assert reader.read() == b""
    assert source.read() == PAYLOAD


def test_bounded_reader_short_source(source: IO[bytes]) -> None:
    reader = BoundedReader(source, len(PAYLOAD) + 1)
    assert reader.read() == PAYLOAD
    with pytest.raises(EOFError, match="1 bytes remaining"):
        reader.read()


def test_bounded_reader_rewind(source: IO[bytes]) -> None:
    assert source.read(7) == b"prefix:"
    reader = BoundedReader(source, 8)
    position = set_file_position(reader, None)
    assert isinstance(position, int)
    assert reader.read(3) == b"abc"
    if not source.seekable():
        assert not reader.seekable()
        with pytest.raises(UnsupportedOperation):
            reader.seek(0)
        assert reader.read() == b"defgh"
        return

    assert reader.seekable()
    rewind_body(cast(IO[bytes], reader), position)
    assert reader.read() == b"abcdefgh"
    assert reader.read() == b""
    assert reader.seek(-2, SEEK_END) == 6
    assert reader.read() == b"gh"
    assert reader.seek(-3, SEEK_CUR) == 5
    assert reader.read() == b"fgh"
