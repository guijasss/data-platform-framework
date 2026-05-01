from pathlib import Path

import yaml

from src.data_contract import (
    ColumnDefinition,
    DataContractDefinition,
    create_data_contract_template,
    data_contract_exists,
    data_contract_is_valid,
)


def _build_definition() -> DataContractDefinition:
    return DataContractDefinition(
        owner="data-platform",
        description="Sales records ingested from source systems.",
        write_mode="INCREMENTAL",
        columns=[
            ColumnDefinition(
                name="amount",
                data_type="decimal(18,2)",
                nullable=False,
                description="Sale amount.",
            ),
        ],
        quality_rules=[
            "WHERE amount IS NOT NULL",
        ],
    )


def test_should_create_data_contract_template_in_catalog_layer_directory(
    tmp_path: Path,
) -> None:
    # Arrange
    definition = _build_definition()

    # Act
    contract_path = create_data_contract_template(
        table_name="catalog.silver.sales",
        definition=definition,
        output_path=tmp_path,
    )

    # Assert
    assert contract_path == tmp_path / "catalog" / "silver" / "sales_data_contract.yaml"
    assert contract_path.exists()

    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    assert contract["table"]["catalog"] == "catalog"
    assert contract["table"]["layer"] == "silver"
    assert contract["table"]["name"] == "sales"
    assert contract["table"]["full_name"] == "catalog.silver.sales"
    assert contract["schema"][0]["name"] == "id"
    assert contract["schema"][1]["name"] == "processed_at"
    assert contract["schema"][2]["name"] == "amount"


def test_should_return_true_when_data_contract_exists(tmp_path: Path) -> None:
    # Arrange
    create_data_contract_template(
        table_name="catalog.silver.sales",
        definition=_build_definition(),
        output_path=tmp_path,
    )

    # Act
    result = data_contract_exists(
        table_name="catalog.silver.sales",
        output_path=tmp_path,
    )

    # Assert
    assert result is True


def test_should_return_false_when_data_contract_file_does_not_exist(
    tmp_path: Path,
) -> None:
    # Act
    result = data_contract_exists(
        table_name="catalog.silver.sales",
        output_path=tmp_path,
    )

    # Assert
    assert result is False


def test_should_return_true_when_data_contract_is_valid(tmp_path: Path) -> None:
    # Arrange
    create_data_contract_template(
        table_name="catalog.silver.sales",
        definition=_build_definition(),
        output_path=tmp_path,
    )

    # Act
    result = data_contract_is_valid(
        table_name="catalog.silver.sales",
        output_path=tmp_path,
    )

    # Assert
    assert result is True


def test_should_return_false_when_data_contract_is_invalid(tmp_path: Path) -> None:
    # Arrange
    contract_path = tmp_path / "catalog" / "silver" / "sales_data_contract.yaml"
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(
        yaml.safe_dump(
            {
                "table": {
                    "catalog": "catalog",
                    "layer": "silver",
                    "name": "sales",
                    "full_name": "catalog.silver.sales",
                    "write_mode": "INCREMENTAL",
                },
                "schema": [{"name": "id"}],
                "quality": {"rules": ["amount > 0"]},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    # Act
    result = data_contract_is_valid(
        table_name="catalog.silver.sales",
        output_path=tmp_path,
    )

    # Assert
    assert result is False


def test_should_return_false_when_data_contract_validation_file_does_not_exist(
    tmp_path: Path,
) -> None:
    # Act
    result = data_contract_is_valid(
        table_name="catalog.silver.sales",
        output_path=tmp_path,
    )

    # Assert
    assert result is False
