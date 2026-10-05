from io import BytesIO
from typing import Any
from unittest.mock import Mock

import pytest
import urllib3
from objectstore_client import Client, RequestError, Session, Usecase, _resumable
from objectstore_client._resumable import UploadComplete


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch) -> Session:
    monkeypatch.setattr(_resumable, "RESUMABLE_THRESHOLD", 4)
    monkeypatch.setattr("objectstore_client._resumable.time.sleep", Mock())
    return Client("http://localhost:8888").session(Usecase("test", compression="none"))


@pytest.mark.parametrize(
    "size,enabled,declined",
    [(3, True, False), (4, True, False), (4, False, False), (4, True, True)],
)
def test_routing(
    session: Session,
    monkeypatch: pytest.MonkeyPatch,
    size: int,
    enabled: bool,
    declined: bool,
) -> None:
    handle = Mock(key="key")
    handle.put.return_value = UploadComplete()
    create = Mock(return_value=None if declined else handle)
    direct = Mock(return_value="key")
    monkeypatch.setattr(session, "_create_upload", create)
    monkeypatch.setattr(session, "_put_direct", direct)

    assert session.put(b"x" * size, resumable=enabled) == "key"
    eligible = enabled and size >= 4
    assert create.call_count == int(eligible)
    assert handle.put.call_count == int(eligible and not declined)
    assert direct.call_count == int(not eligible or declined)


def test_partial_failure_recovery(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    failure = urllib3.exceptions.ReadTimeoutError(session._pool, "/", "lost response")
    outcomes = Mock(
        side_effect=[
            urllib3.HTTPResponse(status=201, body=b'{"key":"key","session":"token"}'),
            failure,
            urllib3.HTTPResponse(status=204, headers={"Upload-Offset": "3"}),
            urllib3.HTTPResponse(status=201),
        ]
    )
    sent = []

    def request(*args: Any, **kwargs: Any) -> urllib3.HTTPResponse:
        if body := kwargs.get("body"):
            sent.append((kwargs["headers"]["Upload-Offset"], body.read()))
        return outcomes()

    monkeypatch.setattr(session._pool, "_make_request", request)
    source = BytesIO(b"prefixabcdefgh")
    source.seek(len(b"prefix"))
    assert session.put(source) == "key"
    assert not source.closed
    assert sent == [("0", b"abcdefgh"), ("3", b"defgh")]


def test_lost_final_response(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    handle = Mock(key="key")
    handle.put.side_effect = urllib3.exceptions.ProtocolError("lost response")
    handle.progress.return_value = UploadComplete()
    monkeypatch.setattr(session, "_create_upload", Mock(return_value=handle))
    assert session.put(b"complete") == "key"
    assert handle.put.call_count == 1
    handle.progress.assert_called_once_with()


@pytest.mark.parametrize("pool_retries", [0, 2])
def test_retry_exhaustion(
    session: Session, monkeypatch: pytest.MonkeyPatch, pool_retries: int
) -> None:
    policy = urllib3.Retry(total=pool_retries, read=pool_retries)
    session._pool.retries = policy
    failure = urllib3.exceptions.ReadTimeoutError(session._pool, "/", "lost response")
    sleep = Mock()
    monkeypatch.setattr("objectstore_client._resumable.time.sleep", sleep)

    def request(*args: Any, **kwargs: Any) -> urllib3.HTTPResponse:
        headers = kwargs["headers"]
        if "Upload-Length" in headers:
            return urllib3.HTTPResponse(
                status=201, body=b'{"key":"key","session":"token"}'
            )
        if headers["Upload-Offset"] == "*":
            return urllib3.HTTPResponse(status=204, headers={"Upload-Offset": "0"})
        raise failure

    make_request = Mock(side_effect=request)
    monkeypatch.setattr(session._pool, "_make_request", make_request)
    with pytest.raises(RequestError, match="^upload failed$") as raised:
        session.put(b"payload")
    assert isinstance(raised.value.__cause__, urllib3.exceptions.MaxRetryError)
    assert raised.value.__cause__.reason is failure
    # Creation, two progress queries, and three writes with their own pool retries.
    assert make_request.call_count == 1 + 2 + 3 * (pool_retries + 1)
    assert session._pool.retries is policy
    assert sleep.call_count == 2
    for call, delay in zip(sleep.call_args_list, [2, 4], strict=True):
        assert delay <= call.args[0] <= delay + 1


@pytest.mark.parametrize(
    "error",
    [
        RequestError("creation rejected", 403, "forbidden"),
        urllib3.exceptions.ProtocolError("connection lost"),
        ValueError("invalid creation response"),
    ],
)
def test_creation_failure_falls_back(
    session: Session, monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    monkeypatch.setattr(session, "_create_upload", Mock(side_effect=error))
    sent = []

    def request(*args: Any, **kwargs: Any) -> urllib3.HTTPResponse:
        sent.append(kwargs["body"].read())
        return urllib3.HTTPResponse(status=201, body=b'{"key":"key"}')

    monkeypatch.setattr(session._pool, "request", request)
    source = BytesIO(b"prefixpayload")
    source.seek(len(b"prefix"))
    assert session.put(source) == "key"
    assert sent == [b"payload"]
    assert not source.closed
