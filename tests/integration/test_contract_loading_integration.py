import pytest

from src.exceptions import DataContractNotFoundError
from src.helpers import load_data_contract_yaml


@pytest.mark.integration
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


@pytest.mark.integration
def test_load_data_contract_yaml_reads_matching_yaml_contract(tmp_path, monkeypatch):
    contract_path = tmp_path / "silver" / "users.yaml"
    contract_path.parent.mkdir()
    contract_path.write_text("table: silver.users\n", encoding="utf-8")
    monkeypatch.setattr("src.helpers.DEFAULT_CONTRACTS_DIR", tmp_path)

    assert load_data_contract_yaml("silver.users") == {"table": "silver.users"}


@pytest.mark.integration
def test_load_data_contract_yaml_raises_when_contract_is_missing(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("src.helpers.DEFAULT_CONTRACTS_DIR", tmp_path)

    with pytest.raises(DataContractNotFoundError, match="Table silver.users"):
        load_data_contract_yaml("silver.users")
