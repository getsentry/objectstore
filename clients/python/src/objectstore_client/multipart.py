from __future__ import annotations

import base64
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import IO, TYPE_CHECKING
from urllib.parse import urlencode

from objectstore_client.errors import RequestError, raise_for_status
from objectstore_client.metrics import measure_storage_operation

if TYPE_CHECKING:
    from objectstore_client.client import Session


@dataclass
class CompletePart:
    """A reference to an uploaded part, used when completing a multipart upload."""

    part_number: int
    etag: str


@dataclass
class PartInfo:
    """Information about an uploaded part"""

    part_number: int
    etag: str
    last_modified: datetime
    size: int

    def to_complete_part(self) -> CompletePart:
        return CompletePart(part_number=self.part_number, etag=self.etag)


class MultipartCompleteError(RequestError):
    """Error returned as part of a multipart:complete response's body."""

    def __init__(self, code: str, message: str):
        super().__init__(
            f"Multipart complete failed ({code}): {message}",
            status=200,
            response=message,
        )
        self.code = code


class MultipartUpload:
    """
    Handle for an in-progress multipart upload.

    Create via :meth:`Session.initiate_multipart_upload` or
    :meth:`Session.resume_multipart_upload`.
    """

    def __init__(self, session: Session, key: str, upload_id: str):
        self._session = session
        self._key = key
        self._upload_id = upload_id

    @property
    def key(self) -> str:
        return self._key

    @property
    def upload_id(self) -> str:
        return self._upload_id

    def put_part(
        self,
        contents: bytes | IO[bytes],
        *,
        part_number: int,
        content_length: int,
        content_md5: bytes | None = None,
    ) -> CompletePart:
        """
        Uploads a single part.

        IMPORTANT: Unlike :meth:`Session.put`, this does **not**
        automatically compress `contents`.
        The caller must pre-compress each part according to the
        compression set as part of the metadata when initiating
        the upload.

        Args:
            contents: The part data. If this upload was initiated
                with compression, this must be pre-compressed.
            part_number: 1-indexed part number.
            content_length: The length in bytes of the payload
                being uploaded. If this upload was initiated with
                compression, this must be the post-compression
                length.
            content_md5: Optional raw MD5 digest of `contents`.
        """
        if isinstance(contents, bytes):
            if len(contents) != content_length:
                raise ValueError(
                    "content_length must match the size of the provided payload"
                )
            body: bytes | IO[bytes] = BytesIO(contents)
        else:
            body = contents

        if content_md5 is not None and len(content_md5) != 16:
            raise ValueError("content_md5 must be exactly 16 bytes")

        headers = self._session._make_headers()
        headers["Content-Length"] = str(content_length)

        if content_md5 is not None:
            headers["Content-MD5"] = base64.b64encode(content_md5).decode("ascii")

        query = urlencode(
            {"upload_id": self._upload_id, "part_number": str(part_number)}
        )
        url = self._session._make_multipart_url("parts", self._key, query)

        with measure_storage_operation(
            self._session._metrics_backend,
            "multipart.put_part",
            self._session._usecase.name,
        ) as metric_emitter:
            response = self._session._pool.request(
                "PUT",
                url,
                body=body,
                headers=headers,
                preload_content=True,
                decode_content=True,
            )
            raise_for_status(response)
            res = response.json()
            metric_emitter.record_size(content_length)
            return CompletePart(part_number=part_number, etag=res["etag"])

    def list_parts(self) -> list[PartInfo]:
        """List all uploaded parts, auto-paginating."""
        all_parts: list[PartInfo] = []
        marker: int | None = None

        while True:
            params: dict[str, str] = {"upload_id": self._upload_id}
            if marker is not None:
                params["part_number_marker"] = str(marker)

            query = urlencode(params)
            url = self._session._make_multipart_url("parts", self._key, query)
            headers = self._session._make_headers()

            response = self._session._pool.request(
                "GET",
                url,
                headers=headers,
                preload_content=True,
            )
            raise_for_status(response)
            data = response.json()

            for p in data["parts"]:
                all_parts.append(
                    PartInfo(
                        part_number=p["part_number"],
                        etag=p["etag"],
                        last_modified=datetime.fromisoformat(p["last_modified"]),
                        size=p["size"],
                    )
                )

            if not data["is_truncated"]:
                return all_parts

            marker = data.get("next_part_number_marker")
            if marker is None:
                raise RequestError(
                    "Server returned is_truncated=true but no next_part_number_marker",
                    status=200,
                    response=str(data),
                )

    def abort(self) -> None:
        """Abort this multipart upload, cleaning up server-side state."""
        query = urlencode({"upload_id": self._upload_id})
        url = self._session._make_multipart_url(None, self._key, query)
        headers = self._session._make_headers()

        response = self._session._pool.request(
            "DELETE",
            url,
            headers=headers,
        )
        raise_for_status(response)

    def complete(self, parts: Sequence[CompletePart]) -> str:
        """Complete the multipart upload, assembling all parts into the final object.

        Returns the final object key.

        Raises :class:`MultipartCompleteError` if the server reports an error
        during assembly in a 200 response body. Ordinary non-2xx HTTP errors are
        still raised as :class:`RequestError`.
        """
        query = urlencode({"upload_id": self._upload_id})
        url = self._session._make_multipart_url("complete", self._key, query)
        headers = self._session._make_headers()
        headers["Content-Type"] = "application/json"

        request_body = json.dumps(
            {"parts": [{"part_number": p.part_number, "etag": p.etag} for p in parts]}
        ).encode("utf-8")

        with measure_storage_operation(
            self._session._metrics_backend,
            "multipart_complete",
            self._session._usecase.name,
        ):
            response = self._session._pool.request(
                "POST",
                url,
                body=request_body,
                headers=headers,
                preload_content=True,
                decode_content=True,
            )
            raise_for_status(response)

            # Successful completion responses may include keepalive whitespace.
            raw = (response.data or b"").decode("utf-8").strip()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                raise RequestError(
                    "Failed to parse multipart complete response",
                    status=response.status,
                    response=raw,
                )

            if "error" in data:
                raise MultipartCompleteError(
                    code=data["error"]["code"],
                    message=data["error"]["message"],
                )

            return data["key"]
