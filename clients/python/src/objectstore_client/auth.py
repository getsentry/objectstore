import base64
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Self

import jwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

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


class SecretKey:
    """
    An EdDSA keypair used to authenticate with Objectstore.

    Can generate JWTs for request-level auth and sign canonical forms for
    pre-signed URLs. Pass a ``SecretKey`` directly to ``Client(token=...)``
    and it will sign a fresh JWT for each request.
    """

    def __init__(
        self,
        kid: str,
        secret_key: str,
        expiry_seconds: int = 60,
        permissions: list[Permission] | None = None,
    ):
        self.kid = kid
        self.secret_key = secret_key
        self.expiry_seconds = expiry_seconds
        self.permissions = permissions if permissions is not None else Permission.max()

    def token_for_scope(
        self,
        usecase: str,
        scope: Scope,
        permissions: list[Permission] | None = None,
        expiry_seconds: int | None = None,
    ) -> str:
        """
        Sign a JWT for the passed-in usecase and scope using the configured key
        information, expiry, and permissions.

        When ``permissions`` is ``None``, the default permissions are used.
        When provided, they override the defaults for this token only.

        When ``expiry_seconds`` is ``None``, the default expiry is used.

        Raises ``ValueError`` if any requested permission is not granted to
        this key.

        The JWT is signed using EdDSA, so ``self.secret_key`` must be an EdDSA
        private key. ``self.kid`` is used by the Objectstore server to load the
        corresponding public key from its configuration.
        """
        if permissions is not None:
            escalated = set(permissions) - set(self.permissions)
            if escalated:
                raise ValueError(
                    "requested permissions not granted to this "
                    f"secret key: {sorted(escalated)}"
                )

        headers = {"kid": self.kid}
        expiry = expiry_seconds if expiry_seconds is not None else self.expiry_seconds
        claims = {
            "res": {
                "os:usecase": usecase,
                **{k: str(v) for k, v in scope.dict().items()},
            },
            "permissions": (
                permissions if permissions is not None else self.permissions
            ),
            "exp": datetime.now(tz=UTC) + timedelta(seconds=expiry),
        }

        return jwt.encode(claims, self.secret_key, algorithm="EdDSA", headers=headers)

    def sign_canonical(self, canonical_form: str) -> str:
        """Signs a canonical request form with the Ed25519 private key.

        Returns the base64url-encoded (no padding) signature, suitable as the
        value of the ``os_sig`` query parameter.
        """
        key = load_pem_private_key(self.secret_key.encode(), password=None)
        if not isinstance(key, Ed25519PrivateKey):
            raise ValueError("pre-signed URLs require an Ed25519 private key")
        signature = key.sign(canonical_form.encode())
        return base64.urlsafe_b64encode(signature).rstrip(b"=").decode()


TokenProvider = SecretKey | str
"""Authentication provider for Objectstore requests.

Can be either a :class:`SecretKey` that signs a fresh JWT per request,
or a static pre-signed JWT string.
"""
