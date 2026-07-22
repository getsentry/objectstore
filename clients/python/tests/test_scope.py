import pytest
from objectstore_client.scope import EMPTY_SCOPES, Scope


def test_empty_scope_renders_sentinel() -> None:
    scope = Scope()
    assert str(scope) == EMPTY_SCOPES
    assert str(scope) == "_"


def test_single_scope() -> None:
    scope = Scope(org="12345")
    assert str(scope) == "org=12345"


def test_multiple_scopes() -> None:
    scope = Scope(org="12345", project="1337")
    assert str(scope) == "org=12345;project=1337"


def test_integer_value() -> None:
    scope = Scope(org=12345)
    assert str(scope) == "org=12345"


def test_integer_zero_is_accepted() -> None:
    scope = Scope(key=0)
    assert str(scope) == "key=0"


def test_bool_false_is_accepted() -> None:
    scope = Scope(key=False)
    assert str(scope) == "key=False"


def test_bool_true_is_accepted() -> None:
    scope = Scope(key=True)
    assert str(scope) == "key=True"


def test_empty_string_value_rejected() -> None:
    with pytest.raises(ValueError, match="Scope value cannot be empty"):
        Scope(key="")


def test_invalid_key_chars_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid scope key"):
        Scope(**{"key/bad": "value"})


def test_invalid_value_chars_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid scope value"):
        Scope(key="val/ue")


def test_dict_returns_original_values() -> None:
    scope = Scope(org=42, project="test")
    result = scope.dict()
    assert result == {"org": 42, "project": "test"}
