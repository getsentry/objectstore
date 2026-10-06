from io import BytesIO
from typing import Any
from unittest.mock import Mock

import pytest
import urllib3
from objectstore_client import Client, RequestError, Session, Usecase


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch) -> Session:
    monkeypatch.setattr("objectstore_client._resumable.time.sleep", Mock())
    return Client("http://localhost:8888").session(
        Usecase("test", compression="none", resumable_threshold_bytes=4)
    )


@pytest.mark.parametrize("complete", [False, True])
def test_failure_recovery(
    session: Session, monkeypatch: pytest.MonkeyPatch, complete: bool
) -> None:
    failure = urllib3.exceptions.ReadTimeoutError(session._pool, "/", "lost response")
    outcomes = Mock(
        side_effect=[
            urllib3.HTTPResponse(status=201, body=b'{"key":"key","session":"token"}'),
            failure,
            urllib3.HTTPResponse(
                status=201 if complete else 204, headers={"Upload-Offset": "3"}
            ),
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
    assert sent == (
        [("0", b"abcdefgh")] if complete else [("0", b"abcdefgh"), ("3", b"defgh")]
    )


@pytest.mark.parametrize("status_failure", [False, True])
def test_retry_exhaustion(
    session: Session,
    monkeypatch: pytest.MonkeyPatch,
    status_failure: bool,
) -> None:
    policy = urllib3.Retry(
        total=2,
        read=2,
        status=2,
        status_forcelist=[503],
    )
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
        if status_failure:
            return urllib3.HTTPResponse(status=503, body=b"unavailable")
        raise failure

    make_request = Mock(side_effect=request)
    monkeypatch.setattr(session._pool, "_make_request", make_request)
    with pytest.raises(RequestError, match="^upload failed$") as raised:
        session.put(b"payload")
    if status_failure:
        assert isinstance(raised.value.__cause__, RequestError)
        assert raised.value.__cause__.status == 503
        assert raised.value.response == "unavailable"
    else:
        assert isinstance(raised.value.__cause__, urllib3.exceptions.MaxRetryError)
        assert raised.value.__cause__.reason is failure
        assert raised.value.response is None
    # Creation, two progress queries, and three writes regardless of pool retries.
    assert make_request.call_count == 1 + 2 + 3
    assert session._pool.retries is policy
    assert sleep.call_count == 2
    for call, delay in zip(sleep.call_args_list, [2, 4], strict=True):
        assert delay <= call.args[0] <= delay + 1


def test_connection_retries_are_not_multiplied(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = urllib3.Retry(total=2, connect=2)
    session._pool.retries = policy
    handle = session._resume_upload("key", "token")
    monkeypatch.setattr(session, "_create_upload", Mock(return_value=handle))
    failure = urllib3.exceptions.ConnectTimeoutError("connection timed out")
    request = Mock(side_effect=failure)
    monkeypatch.setattr(session._pool, "_make_request", request)
    with pytest.raises(RequestError) as raised:
        session.put(b"payload")
    assert isinstance(raised.value.__cause__, urllib3.exceptions.MaxRetryError)
    assert raised.value.__cause__.reason is failure
    assert request.call_count == 3
    assert session._pool.retries is policy
