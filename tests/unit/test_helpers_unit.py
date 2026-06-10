from dataclasses import dataclass

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


def test_load_data_contract_yaml_reads_matching_yml_contract(tmp_path, monkeypatch):
    contract_path = tmp_path / "silver" / "users.yml"
    contract_path.parent.mkdir()
    contract_path.write_text(
        """
table: silver.users
quality_rules:
  - name: positive_value
    expression: value > 0
    severity: error
""",
        encoding="utf-8",
    )
    monkeypatch.setattr("src.helpers.DEFAULT_CONTRACTS_DIR", tmp_path)

    contract = load_data_contract_yaml("silver.users")

    assert contract["table"] == "silver.users"
    assert contract["quality_rules"][0]["name"] == "positive_value"


def test_load_data_contract_yaml_reads_matching_yaml_contract(tmp_path, monkeypatch):
    contract_path = tmp_path / "silver" / "users.yaml"
    contract_path.parent.mkdir()
    contract_path.write_text("table: silver.users\n", encoding="utf-8")
    monkeypatch.setattr("src.helpers.DEFAULT_CONTRACTS_DIR", tmp_path)

    assert load_data_contract_yaml("silver.users") == {"table": "silver.users"}


def test_load_data_contract_yaml_raises_when_contract_is_missing(tmp_path, monkeypatch):
    monkeypatch.setattr("src.helpers.DEFAULT_CONTRACTS_DIR", tmp_path)

    with pytest.raises(DataContractNotFoundError, match="Table silver.users"):
        load_data_contract_yaml("silver.users")
