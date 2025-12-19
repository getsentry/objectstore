import string

# Characters allowed in a Scope's key and value.
# These are the URL safe characters, except for `.` which we use as separator between
# key and value of Scope components.
SCOPE_VALUE_ALLOWED_CHARS = set(string.ascii_letters + string.digits + "-_()$!+'")


class Scope:
    def __init__(self, **scopes: str | int | bool):
        parts = []
        for key, value in scopes.items():
            if not key:
                raise ValueError("Scope key cannot be empty")
            if not value:
                raise ValueError("Scope value cannot be empty")

            if any(c not in SCOPE_VALUE_ALLOWED_CHARS for c in key):
                raise ValueError(
                    f"Invalid scope key {key}. The valid character set is: "
                    f"{''.join(SCOPE_VALUE_ALLOWED_CHARS)}"
                )

            value = str(value)
            if any(c not in SCOPE_VALUE_ALLOWED_CHARS for c in value):
                raise ValueError(
                    f"Invalid scope value {value}. The valid character set is: "
                    f"{''.join(SCOPE_VALUE_ALLOWED_CHARS)}"
                )

            formatted = f"{key}={value}"
            parts.append(formatted)

        self._scope = scopes
        self._scope_str = ";".join(parts)

    def __str__(self) -> str:
        return self._scope_str

    def dict(self) -> dict[str, str | int | bool]:
        return self._scope
