import os
from urllib.parse import parse_qs, urlparse

import pytest
from objectstore_client.presign import (
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
        # = and ; in path are canonically encoded; query keys/values are
        # individually encoded (= and & are structural delimiters)
        canonical = _canonical_presigned_request(
            "/v1/objects/attachments/org=123;project=456/my-key",
            "X-Os-Expires=1712668800&X-Os-KeyId=relay-prod&X-Os-Signature=abc123",
        )
        assert canonical == (
            "GET\n"
            "%2Fv1%2Fobjects%2Fattachments%2Forg%3D123%3Bproject%3D456%2Fmy-key\n"
            "X-Os-Expires=1712668800&X-Os-KeyId=relay-prod"
        )

    def test_percent_encoded_path(self) -> None:
        # Pre-encoded and unencoded paths produce identical canonical forms
        canonical_unencoded = _canonical_presigned_request(
            "/v1/objects/attachments/org=123;project=456/my-key",
            "X-Os-Expires=1712668800&X-Os-KeyId=relay-prod",
        )
        canonical_encoded = _canonical_presigned_request(
            "/v1/objects/attachments/org%3D123%3Bproject%3D456/my-key",
            "X-Os-Expires=1712668800&X-Os-KeyId=relay-prod",
        )
        assert canonical_unencoded == canonical_encoded

    def test_lowercase_hex(self) -> None:
        # Lowercase and uppercase hex digits produce the same canonical form
        canonical_upper = _canonical_presigned_request(
            "/v1/objects/test/org%3D1/key",
            "X-Os-Expires=1000&X-Os-KeyId=test",
        )
        canonical_lower = _canonical_presigned_request(
            "/v1/objects/test/org%3d1/key",
            "X-Os-Expires=1000&X-Os-KeyId=test",
        )
        assert canonical_upper == canonical_lower

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

        assert isinstance(result, str)
        parsed = urlparse(result)
        query = parse_qs(parsed.query)

        assert "X-Os-Expires" in query
        assert query["X-Os-KeyId"] == [TEST_EDDSA_KID]
        assert "X-Os-Signature" in query

    def test_head_produces_same_canonical(self) -> None:
        """Both GET and HEAD produce canonical forms starting with GET."""
        url = "http://localhost:8888/v1/objects/test/org=1/key"

        r1 = presign_url(TEST_EDDSA_KID, _read_private_key(), "GET", url)
        r2 = presign_url(TEST_EDDSA_KID, _read_private_key(), "HEAD", url)

        p1 = urlparse(r1)
        p2 = urlparse(r2)
        c1 = _canonical_presigned_request(p1.path, p1.query)
        c2 = _canonical_presigned_request(p2.path, p2.query)

        assert c1.startswith("GET\n")
        assert c2.startswith("GET\n")

    def test_invalid_method(self) -> None:
        url = "http://localhost:8888/v1/objects/test/org=1/key"
        with pytest.raises(ValueError, match="Unsupported method"):
            presign_url(TEST_EDDSA_KID, _read_private_key(), "PUT", url)

    def test_custom_expiry(self) -> None:
        url = "http://localhost:8888/v1/objects/test/org=1/key"
        result = presign_url(
            TEST_EDDSA_KID, _read_private_key(), "GET", url, expires_in=60
        )

        # Verify the expires param is ~60s from now
        parsed = urlparse(result)
        query = parse_qs(parsed.query)
        expires_ts = int(query["X-Os-Expires"][0])

        from datetime import UTC, datetime

        diff = expires_ts - int(datetime.now(tz=UTC).timestamp())
        assert 58 <= diff <= 61
