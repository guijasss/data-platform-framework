from pathlib import Path

import polars as pl
import pytest

from src.database.connection import table_exists
from src.exceptions import DataQualityError
from src.writer import WriteConfig, write


def _write_contract(
    contracts_dir: Path,
    target_table: str,
    expression: str = "value > 0",
) -> None:
    schema, table = target_table.split(".")
    contract_path = contracts_dir / schema / f"{table}.yml"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text(
        f"""
table: {target_table}
quality_rules:
  - name: positive_value
    expression: {expression}
    severity: error
""",
        encoding="utf-8",
    )


@pytest.mark.integration
def test_write_succeeds_when_data_quality_rule_passes(
    database,
    tmp_path,
    monkeypatch,
):
    target_table = "silver_test.quality_users"
    _write_contract(tmp_path, target_table)
    monkeypatch.setattr("src.helpers.DEFAULT_CONTRACTS_DIR", tmp_path)

    data = pl.DataFrame(
        {
            "id": [1, 2],
            "value": [10.0, 20.0],
        }
    ).lazy()

    write(
        WriteConfig(
            target_table=target_table,
            method="APPEND",
            data=data,
            comments=None,
        )
    )

    assert table_exists(target_table) is True


@pytest.mark.integration
def test_write_fails_when_data_quality_rule_fails(
    database,
    tmp_path,
    monkeypatch,
):
    target_table = "silver_test.rejected_quality_users"
    _write_contract(tmp_path, target_table)
    monkeypatch.setattr("src.helpers.DEFAULT_CONTRACTS_DIR", tmp_path)

    data = pl.DataFrame(
        {
            "id": [1, 2],
            "value": [10.0, -20.0],
        }
    ).lazy()

    with pytest.raises(
        DataQualityError,
        match="Quality check positive_value failed",
    ):
        write(
            WriteConfig(
                target_table=target_table,
                method="APPEND",
                data=data,
                comments=None,
            )
        )

    assert table_exists(target_table) is False
