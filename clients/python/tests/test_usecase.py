import pytest
from objectstore_client import Client, Usecase


@pytest.mark.parametrize("length", [0, 1, 2, 3, 64, 65])
def test_length_boundaries(length: int) -> None:
    name = "a" * length
    if 3 <= length <= 64:
        assert Usecase(name).name == name
    else:
        with pytest.raises(ValueError, match="3–64"):
            Usecase(name)


@pytest.mark.parametrize(
    "name",
    [
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-",
        "Debug_Files-123",
        "___",
        "---",
        "-a_",
    ],
)
def test_allowed_names(name: str) -> None:
    assert Usecase(name).name == name


@pytest.mark.parametrize(
    "char", ["/", "\\", ".", "%", "=", ";", "?", "#", " ", "\t", "\n", "\0", "é"]
)
def test_forbidden_characters(char: str) -> None:
    with pytest.raises(ValueError, match="ASCII letters"):
        Usecase(f"test{char}")


def test_name_is_read_only() -> None:
    usecase = Usecase("testing")
    with pytest.raises(AttributeError):
        setattr(usecase, "name", "testing/org=999/injected")
    assert usecase.name == "testing"


def test_path_injection_rejected() -> None:
    client = Client("http://127.0.0.1:8888/")
    with pytest.raises(ValueError):
        client.session(Usecase("testing/org=999/injected"), org=123)
