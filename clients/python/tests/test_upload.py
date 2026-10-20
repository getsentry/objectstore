from io import BytesIO
from typing import Any
from unittest.mock import Mock

import pytest
import urllib3
from objectstore_client import Client, RequestError, Session, Usecase
from objectstore_client._resumable import NoUploadProgress


@pytest.fixture
def session(monkeypatch: pytest.MonkeyPatch) -> Session:
    monkeypatch.setattr("objectstore_client._resumable.time.sleep", Mock())
    return Client("http://localhost:8888").session(
        Usecase("test", compression="none", resumable_threshold_bytes=4)
    )


@pytest.mark.parametrize("complete", [False, True])
@pytest.mark.parametrize("offsets,delays", [([3], [2]), ([0, 3, 3, 6], [2, 4, 2, 4])])
def test_failure_recovery(
    session: Session,
    monkeypatch: pytest.MonkeyPatch,
    complete: bool,
    offsets: list[int],
    delays: list[int],
) -> None:
    failure = urllib3.exceptions.ReadTimeoutError(session._pool, "/", "lost response")
    responses: list[urllib3.HTTPResponse | Exception] = [
        urllib3.HTTPResponse(status=201, body=b'{"key":"key","session":"token"}')
    ]
    for index, offset in enumerate(offsets):
        responses.extend(
            [
                failure,
                urllib3.HTTPResponse(
                    status=201 if complete and index == len(offsets) - 1 else 204,
                    headers={"Upload-Offset": str(offset)},
                ),
            ]
        )
    responses.append(urllib3.HTTPResponse(status=201))
    outcomes = Mock(side_effect=responses)
    sleep = Mock()
    monkeypatch.setattr("objectstore_client._resumable.time.sleep", sleep)
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
    write_offsets = [0, *offsets[:-1]] if complete else [0, *offsets]
    assert sent == [(str(offset), b"abcdefgh"[offset:]) for offset in write_offsets]
    assert sleep.call_count == len(delays)
    for call, delay in zip(sleep.call_args_list, delays, strict=True):
        assert delay <= call.args[0] <= delay + 1


@pytest.mark.parametrize("failure_kind", ["timeout", "status", "no_progress"])
def test_retry_exhaustion(
    session: Session,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    failure = urllib3.exceptions.ReadTimeoutError(session._pool, "/", "lost response")
    sleep = Mock()
    monkeypatch.setattr("objectstore_client._resumable.time.sleep", sleep)

    def request(*args: Any, **kwargs: Any) -> urllib3.HTTPResponse:
        headers = kwargs["headers"]
        if args[1] == "DELETE":
            return urllib3.HTTPResponse(status=204)
        if "Upload-Length" in headers:
            return urllib3.HTTPResponse(
                status=201, body=b'{"key":"key","session":"token"}'
            )
        if headers["Upload-Offset"] == "*":
            return urllib3.HTTPResponse(status=204, headers={"Upload-Offset": "0"})
        if failure_kind == "status":
            return urllib3.HTTPResponse(status=503, body=b"unavailable")
        if failure_kind == "no_progress":
            return urllib3.HTTPResponse(status=204, headers={"Upload-Offset": "0"})
        raise failure

    make_request = Mock(side_effect=request)
    monkeypatch.setattr(session._pool, "_make_request", make_request)
    with pytest.raises(RequestError, match="^upload failed$") as raised:
        session.put(b"payload")
    if failure_kind == "status":
        assert isinstance(raised.value.__cause__, RequestError)
        assert raised.value.__cause__.status == 503
        assert raised.value.response == "unavailable"
    elif failure_kind == "timeout":
        assert isinstance(raised.value.__cause__, urllib3.exceptions.MaxRetryError)
        assert raised.value.__cause__.reason is failure
        assert raised.value.response is None
    else:
        assert isinstance(raised.value.__cause__, NoUploadProgress)
        assert str(raised.value.__cause__) == "Upload made no progress"
    # Creation, two progress queries, three writes, and cancellation.
    assert make_request.call_count == 1 + 2 + 3 + 1
    assert sleep.call_count == 2
    for call, delay in zip(sleep.call_args_list, [2, 4], strict=True):
        assert delay <= call.args[0] <= delay + 1


def test_sizing_must_restore_position(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = Mock(wraps=BytesIO(b"payload"))
    source.seek.side_effect = [None, OSError("cannot restore position")]
    request = Mock()
    monkeypatch.setattr(session._pool, "request", request)

    with pytest.raises(OSError, match="cannot restore position"):
        session.put(source)

    request.assert_not_called()
