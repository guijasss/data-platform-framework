from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pytest

from src.exceptions import DataContractNotFoundError
from src.helpers import get_env_or_raise, load_data_contract_yaml, validate_required_fields


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


def test_load_data_contract_yaml_reads_matching_contract_without_real_filesystem(
    monkeypatch,
):
    monkeypatch.setattr(
        "src.helpers.Path.glob",
        lambda self, pattern: [Path("/contracts/silver/users.yml")],
    )
    monkeypatch.setattr(
        "builtins.open",
        lambda path, mode, encoding: StringIO(
            """
table: silver.users
quality_rules:
  - name: positive_value
    expression: value > 0
    severity: error
"""
        ),
    )

    contract = load_data_contract_yaml("silver.users")

    assert contract["table"] == "silver.users"
    assert contract["quality_rules"][0]["name"] == "positive_value"


def test_load_data_contract_yaml_uses_yml_or_yaml_glob_pattern(monkeypatch):
    glob_calls = []

    def glob(self, pattern):
        glob_calls.append(pattern)
        return [Path("/contracts/silver/users.yaml")]

    monkeypatch.setattr("src.helpers.Path.glob", glob)
    monkeypatch.setattr(
        "builtins.open",
        lambda path, mode, encoding: StringIO("table: silver.users\n"),
    )

    assert load_data_contract_yaml("silver.users") == {"table": "silver.users"}
    assert glob_calls == ["users.y*ml"]


def test_load_data_contract_yaml_raises_when_contract_is_missing(monkeypatch):
    monkeypatch.setattr("src.helpers.Path.glob", lambda self, pattern: [])

    with pytest.raises(DataContractNotFoundError, match="Table silver.users"):
        load_data_contract_yaml("silver.users")
