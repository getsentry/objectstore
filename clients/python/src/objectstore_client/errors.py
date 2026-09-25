from __future__ import annotations

import urllib3


class RequestError(Exception):
    """Exception raised if an API call to Objectstore fails."""

    def __init__(self, message: str, status: int, response: str):
        super().__init__(message)
        self.status = status
        self.response = response


class ObjectNotFound(RequestError):
    """The object was observed to be absent or expired."""


class ExpiryExtensionRejected(RequestError):
    """The object's expiration deadline could not be extended.

    The object is non-expiring, lacks required creation metadata, or conflicts
    with a conditional update. A conflict can result from concurrent deletion,
    so this does not guarantee that the object still exists.
    """


def raise_for_status(
    response: urllib3.BaseHTTPResponse,
    *,
    error_type: type[RequestError] = RequestError,
) -> None:
    if response.status >= 400:
        res = (response.data or response.read() or b"").decode("utf-8", "replace")
        raise error_type(
            f"Objectstore request failed with status {response.status}",
            response.status,
            res,
        )
