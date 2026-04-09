import os
from urllib.parse import parse_qs, urlparse

import pytest
from objectstore_client.presign import (
    PresignedUrl,
    _canonical_presigned_request,
    presign_url,
)

TEST_EDDSA_KID: str = "test_kid"
TEST_EDDSA_PRIVKEY_PATH: str = (
    os.path.dirname(os.path.realpath(__file__)) + "/ed25519.private.pem"
)


def _read_private_key() -> str:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        return f.read()


class TestCanonicalForm:
    def test_basic(self) -> None:
        canonical = _canonical_presigned_request(
            "/v1/objects/attachments/org=123;project=456/my-key",
            "X-Os-Expires=1712668800&X-Os-KeyId=relay-prod&X-Os-Signature=abc123",
        )
        assert canonical == (
            "GET\n"
            "/v1/objects/attachments/org=123;project=456/my-key\n"
            "X-Os-Expires=1712668800&X-Os-KeyId=relay-prod"
        )

    def test_percent_encoded_path(self) -> None:
        canonical = _canonical_presigned_request(
            "/v1/objects/attachments/org%3D123%3Bproject%3D456/my-key",
            "X-Os-Expires=1712668800&X-Os-KeyId=relay-prod",
        )
        # Should match the non-encoded version after decoding
        assert canonical == (
            "GET\n"
            "/v1/objects/attachments/org=123;project=456/my-key\n"
            "X-Os-Expires=1712668800&X-Os-KeyId=relay-prod"
        )

    def test_reordered_query_params(self) -> None:
        c1 = _canonical_presigned_request(
            "/path",
            "X-Os-KeyId=test&X-Os-Expires=1000",
        )
        c2 = _canonical_presigned_request(
            "/path",
            "X-Os-Expires=1000&X-Os-KeyId=test",
        )
        assert c1 == c2


class TestPresignUrl:
    def test_produces_valid_format(self) -> None:
        url = "http://localhost:8888/v1/objects/attachments/org=123;project=456/my-key"
        result = presign_url(TEST_EDDSA_KID, _read_private_key(), "GET", url)

        assert isinstance(result, PresignedUrl)
        parsed = urlparse(result.url)
        query = parse_qs(parsed.query)

        assert "X-Os-Expires" in query
        assert query["X-Os-KeyId"] == [TEST_EDDSA_KID]
        assert "X-Os-Signature" in query

    def test_head_produces_same_canonical(self) -> None:
        """Both GET and HEAD produce canonical forms starting with GET."""
        url = "http://localhost:8888/v1/objects/test/org=1/key"

        r1 = presign_url(TEST_EDDSA_KID, _read_private_key(), "GET", url)
        r2 = presign_url(TEST_EDDSA_KID, _read_private_key(), "HEAD", url)

        p1 = urlparse(r1.url)
        p2 = urlparse(r2.url)
        c1 = _canonical_presigned_request(p1.path, p1.query)
        c2 = _canonical_presigned_request(p2.path, p2.query)

        assert c1.startswith("GET\n")
        assert c2.startswith("GET\n")

    def test_invalid_method(self) -> None:
        url = "http://localhost:8888/v1/objects/test/org=1/key"
        with pytest.raises(ValueError, match="Unsupported method"):
            presign_url(TEST_EDDSA_KID, _read_private_key(), "PUT", url)

    def test_default_expiry(self) -> None:
        url = "http://localhost:8888/v1/objects/test/org=1/key"
        result = presign_url(TEST_EDDSA_KID, _read_private_key(), "GET", url)

        # Default is 5 minutes
        from datetime import UTC, datetime

        diff = result.expires_at - datetime.now(tz=UTC)
        assert 298 <= diff.total_seconds() <= 301

    def test_custom_expiry(self) -> None:
        url = "http://localhost:8888/v1/objects/test/org=1/key"
        result = presign_url(
            TEST_EDDSA_KID, _read_private_key(), "GET", url, expires_in=60
        )

        from datetime import UTC, datetime

        diff = result.expires_at - datetime.now(tz=UTC)
        assert 58 <= diff.total_seconds() <= 61
