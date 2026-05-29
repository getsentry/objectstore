from __future__ import annotations

import urllib3


class RequestError(Exception):
    """Exception raised if an API call to Objectstore fails."""

    def __init__(self, message: str, status: int, response: str):
        super().__init__(message)
        self.status = status
        self.response = response


def raise_for_status(response: urllib3.BaseHTTPResponse) -> None:
    if response.status >= 400:
        res = (response.data or response.read() or b"").decode("utf-8", "replace")
        raise RequestError(
            f"Objectstore request failed with status {response.status}",
            response.status,
            res,
        )
