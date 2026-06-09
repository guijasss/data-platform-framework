from dataclasses import dataclass

import pytest

from src.helpers import get_env_or_raise, validate_required_fields


def test_get_env_or_raise_returns_value(monkeypatch):
    monkeypatch.setenv("DATABASE_NAME", "catalog")

    assert get_env_or_raise("DATABASE_NAME") == "catalog"


def test_get_env_or_raise_raises_for_missing_value(monkeypatch):
    monkeypatch.delenv("DATABASE_NAME", raising=False)

    with pytest.raises(
        OSError,
        match="Environment variable DATABASE_NAME not defined!",
    ):
        get_env_or_raise("DATABASE_NAME")


@dataclass
class RequiredFieldsSubject:
    present: str | None
    missing: str | None


def test_validate_required_fields_accepts_present_fields():
    subject = RequiredFieldsSubject(present="ok", missing=None)

    validate_required_fields(subject, ["present"])


def test_validate_required_fields_raises_for_missing_fields():
    subject = RequiredFieldsSubject(present="ok", missing=None)

    with pytest.raises(ValueError, match="Required fields are missing: missing"):
        validate_required_fields(subject, ["present", "missing"])
