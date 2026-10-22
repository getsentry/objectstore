"""Unit tests for the pre-signed URL signing utilities."""

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from objectstore_client import Client, Usecase, presign, utils
from objectstore_client.auth import SecretKey, TokenGenerator

TEST_EDDSA_PRIVKEY_PATH = os.path.join(os.path.dirname(__file__), "ed25519.private.pem")

# Raw query string as it would appear on the wire: unsorted, includes the
# os_sig that must be excluded.
SAMPLE_QUERY = (
    "os_timestamp=1985-04-12T23:20:50.52Z"
    "&os_kid=relay"
    "&os_duration=3600"
    "&os_sig=should-be-excluded"
)


def test_canonical_form_full_query() -> None:
    # Base case: pins path (slashes, `=`, `;` unencoded), os_sig exclusion,
    # key lowercasing, and query sort order.
    path = "/v1/objects/testing/org=17;project=42/foo/bar"
    canonical = presign.build_canonical_form(
        "GET", utils.encode_path(path), utils.encode_query(SAMPLE_QUERY)
    )
    assert canonical == (
        "GET\n"
        "/v1/objects/testing/org=17;project=42/foo/bar\n"
        "os_duration=3600&os_kid=relay&os_timestamp=1985-04-12T23:20:50.52Z"
    )


def test_canonical_form_duplicate_keys_preserved_and_sorted() -> None:
    canonical = presign.build_canonical_form(
        "GET",
        utils.encode_path("/v1/objects/testing/_/key"),
        utils.encode_query("dup=b&dup=a&x=1"),
    )
    assert canonical == "GET\n/v1/objects/testing/_/key\ndup=a&dup=b&x=1"


def test_head_is_normalized_to_get() -> None:
    path = utils.encode_path("/v1/objects/testing/_/key")
    query = utils.encode_query(SAMPLE_QUERY)
    assert presign.build_canonical_form(
        "HEAD", path, query
    ) == presign.build_canonical_form("GET", path, query)


def test_presigned_url_response_options() -> None:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        secret = SecretKey("test_kid", f.read())
    session = Client(
        "https://example.com/prefix", token=TokenGenerator(secret)
    ).session(Usecase("testing"), org=1)
    with patch("objectstore_client.client.datetime") as clock:
        clock.now.return_value = datetime(2026, 4, 20, 13, 37, tzinfo=UTC)
        legacy = session.presigned_object_url("GET", "key")
        explicit_none = session.presigned_object_url(
            "GET", "key", response_content_disposition=None
        )
        url = session.presigned_object_url(
            "GET",
            "key",
            response_content_disposition="inline",
        )
    assert legacy == explicit_none
    assert "response-content-disposition" not in parse_qs(urlsplit(legacy).query)
    parsed = urlsplit(url)
    assert parsed.path == urlsplit(legacy).path
    query = parse_qs(parsed.query)
    assert query["response-content-disposition"] == ["inline"]
    assert len(query) == 5


def test_jwt_url_options_roundtrip_without_binding_token() -> None:
    with open(TEST_EDDSA_PRIVKEY_PATH) as f:
        secret = SecretKey("test_kid", f.read())
    session = Client(
        "https://example.com/prefix", token=TokenGenerator(secret)
    ).session(Usecase("testing"))
    with patch.object(session, "mint_token", return_value="same.jwt.token"):
        legacy = session.object_url("key", token_validity=timedelta(minutes=5))
        url = session.object_url(
            "key",
            token_validity=timedelta(minutes=5),
            response_content_disposition="inline",
        )
    assert parse_qs(urlsplit(url).query) == {
        "os_auth": ["same.jwt.token"],
        "response-content-disposition": ["inline"],
    }
    assert legacy.endswith("?os_auth=same.jwt.token")
    assert session.object_url(
        "key", response_content_disposition=None
    ) == session.object_url("key")
