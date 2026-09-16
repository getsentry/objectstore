from datetime import UTC, datetime, timedelta, timezone
from typing import TypedDict
from unittest.mock import Mock

import pytest
from objectstore_client import Client, Usecase


class ExpiryArgs(TypedDict, total=False):
    at: datetime
    from_creation: timedelta
    from_now: timedelta


@pytest.mark.parametrize(
    ("kwargs", "extension"),
    [
        ({"from_now": timedelta(0)}, {"after": "0s", "from": "now"}),
        (
            {"from_creation": timedelta(days=400, microseconds=500_000)},
            {"after": "400d", "from": "creation"},
        ),
        (
            {"at": datetime(2030, 1, 1, 2, 0, 0, 123456, timezone(timedelta(hours=2)))},
            {"at": "2030-01-01T00:00:00.123456+00:00"},
        ),
    ],
)
def test_wire_targets(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: ExpiryArgs,
    extension: dict[str, str],
) -> None:
    session = Client("http://localhost:8888", token="test-token").session(
        Usecase("test"), org=42
    )
    request = Mock(return_value=Mock(status=204))
    monkeypatch.setattr(session._pool, "request", request)
    session.extend_expiry("key", **kwargs)
    args, sent = request.call_args
    assert args == ("PATCH", session._make_url("key"))
    assert sent["json"] == {"extend_expiry": extension}
    assert sent["headers"]["x-os-auth"] == "Bearer test-token"


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"at": datetime(2030, 1, 1)},
        {"from_now": timedelta(microseconds=-1)},
        {"from_creation": timedelta(days=-1)},
        {"from_now": timedelta(0), "from_creation": timedelta(0)},
        {"at": datetime(2030, 1, 1, tzinfo=UTC), "from_now": timedelta(0)},
    ],
)
def test_invalid_targets(monkeypatch: pytest.MonkeyPatch, kwargs: ExpiryArgs) -> None:
    session = Client("http://localhost:8888").session(Usecase("test"))
    request = Mock()
    monkeypatch.setattr(session._pool, "request", request)
    with pytest.raises(ValueError):
        session.extend_expiry("key", **kwargs)
    request.assert_not_called()
