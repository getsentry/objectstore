from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Self

import jwt

from objectstore_client.scope import Scope


class Permission(StrEnum):
    """
    Enum listing permissions that Objectstore tokens may be granted.
    """

    OBJECT_READ = "object.read"
    OBJECT_WRITE = "object.write"
    OBJECT_DELETE = "object.delete"

    @classmethod
    def max(cls) -> list[Self]:
        return list(cls.__members__.values())


class TokenGenerator:
    """
    A utility to generate auth tokens for Objectstore requests.

    Use this for internal services that have access to an EdDSA keypair. You can
    pass a ``TokenGenerator`` directly to ``Client(token=...)`` and it will sign
    a fresh JWT for each request.
    """

    def __init__(
        self,
        kid: str,
        secret_key: str,
        expiry_seconds: int = 60,
        permissions: list[Permission] = Permission.max(),
    ):
        self.kid = kid
        self.secret_key = secret_key
        self.expiry_seconds = expiry_seconds
        self.permissions = permissions

    def sign_for_scope(self, usecase: str, scope: Scope) -> str:
        """
        Sign a JWT for the passed-in usecase and scope using the configured key
        information, expiry, and permissions.

        The JWT is signed using EdDSA, so `self.secret_key` must be an EdDSA private
        key. `self.kid` is used by the Objectstore server to load the corresponding
        public key from its configuration.
        """
        headers = {"kid": self.kid}
        claims = {
            "res": {
                "os:usecase": usecase,
                **{k: str(v) for k, v in scope.dict().items()},
            },
            "permissions": self.permissions,
            "exp": datetime.now(tz=UTC) + timedelta(seconds=self.expiry_seconds),
        }

        return jwt.encode(claims, self.secret_key, algorithm="EdDSA", headers=headers)


TokenProvider = TokenGenerator | str
"""Authentication provider for Objectstore requests.

Can be either a :class:`TokenGenerator` that signs a fresh JWT per request,
or a static pre-signed JWT string.
"""
