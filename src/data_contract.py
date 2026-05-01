from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml


DEFAULT_CONTRACTS_PATH = Path(__file__).resolve().parents[1] / "metadata"
SUPPORTED_LAYERS = {"bronze", "silver", "gold"}
SUPPORTED_WRITE_MODES = {"APPEND", "INCREMENTAL", "FULL_OVERWRITE"}
STANDARD_COLUMNS = (
    {
        "name": "id",
        "data_type": "string",
        "nullable": False,
        "description": "Unique identifier for the row.",
    },
    {
        "name": "processed_at",
        "data_type": "timestamp",
        "nullable": False,
        "description": "UTC timestamp of when the row was inserted or updated.",
    },
)


@dataclass(frozen=True)
class ColumnDefinition:
    name: str
    data_type: str
    description: str
    nullable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "description": self.description,
        }


@dataclass(frozen=True)
class DataContractDefinition:
    owner: str
    description: str
    write_mode: Literal["APPEND", "INCREMENTAL", "FULL_OVERWRITE"]
    columns: list[ColumnDefinition] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=lambda: ["id"])
    quality_rules: list[str] = field(default_factory=list)


DEFAULT_TABLE_NAME = "mycatalog.bronze.salesforce_customers"
DEFAULT_CONTRACT_DEFINITION = DataContractDefinition(
    owner="data-platform",
    description="Customer records ingested from Salesforce.",
    write_mode="INCREMENTAL",
    columns=[
        ColumnDefinition(
            name="email",
            data_type="string",
            nullable=True,
            description="Customer email address.",
        ),
        ColumnDefinition(
            name="is_active",
            data_type="boolean",
            nullable=False,
            description="Whether the customer is active.",
        ),
    ],
    quality_rules=[
        "WHERE id IS NOT NULL",
        "WHERE processed_at IS NOT NULL",
        "WHERE email IS NOT NULL",
    ],
)


def create_data_contract_template(
    table_name: str,
    definition: DataContractDefinition,
    output_path: Path | None = None,
) -> Path:
    catalog, layer, table = _parse_table_name(table_name)
    _validate_definition(definition)

    contract = {
        "version": 1,
        "table": {
            "catalog": catalog,
            "layer": layer,
            "name": table,
            "full_name": table_name,
            "owner": definition.owner,
            "description": definition.description,
            "write_mode": definition.write_mode,
            "primary_key": definition.primary_key,
        },
        "schema": [*STANDARD_COLUMNS, *(column.to_dict() for column in definition.columns)],
        "quality": {
            "rules": definition.quality_rules,
        },
    }

    contract_path = get_data_contract_path(
        table_name=table_name,
        output_path=output_path,
    )
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(
        yaml.safe_dump(contract, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return contract_path


def data_contract_exists(
    table_name: str,
    output_path: Path | None = None,
) -> bool:
    contract_path = get_data_contract_path(
        table_name=table_name,
        output_path=output_path,
    )
    return contract_path.exists()


def data_contract_is_valid(
    table_name: str,
    output_path: Path | None = None,
) -> bool:
    contract_path = get_data_contract_path(
        table_name=table_name,
        output_path=output_path,
    )
    if not contract_path.exists():
        return False

    with contract_path.open("r", encoding="utf-8") as contract_file:
        contract = yaml.safe_load(contract_file)

    if not isinstance(contract, dict):
        return False

    try:
        _validate_contract_payload(table_name=table_name, contract=contract)
    except ValueError:
        return False

    return True


def get_data_contract_path(
    table_name: str,
    output_path: Path | None = None,
) -> Path:
    catalog, layer, table = _parse_table_name(table_name)
    contracts_root = output_path or DEFAULT_CONTRACTS_PATH
    return contracts_root / catalog / layer / f"{table}_data_contract.yaml"


def load_data_contract(
    table_name: str,
    output_path: Path | None = None,
) -> dict[str, Any]:
    contract_path = get_data_contract_path(
        table_name=table_name,
        output_path=output_path,
    )

    with contract_path.open("r", encoding="utf-8") as contract_file:
        contract = yaml.safe_load(contract_file)

    if not isinstance(contract, dict):
        raise ValueError("data contract must be a mapping")

    return contract


def _parse_table_name(table_name: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in table_name.split(".")]
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            "table_name must follow the format <catalog>.<layer>.<table>"
        )

    catalog, layer, table = parts
    if layer not in SUPPORTED_LAYERS:
        raise ValueError(f"unsupported layer: {layer}")

    return catalog, layer, table


def _validate_definition(definition: DataContractDefinition) -> None:
    if not definition.owner.strip():
        raise ValueError("owner is required")

    if not definition.description.strip():
        raise ValueError("description is required")

    if definition.write_mode not in SUPPORTED_WRITE_MODES:
        raise ValueError(f"unsupported write mode: {definition.write_mode}")

    column_names = {column.name for column in definition.columns}
    reserved_columns = {column["name"] for column in STANDARD_COLUMNS}
    duplicated_reserved_columns = column_names.intersection(reserved_columns)
    if duplicated_reserved_columns:
        duplicated_column = sorted(duplicated_reserved_columns)[0]
        raise ValueError(
            f"column is automatically managed by template: {duplicated_column}"
        )

    invalid_quality_rule = next(
        (
            quality_rule
            for quality_rule in definition.quality_rules
            if not _is_valid_quality_rule(quality_rule)
        ),
        None,
    )
    if invalid_quality_rule is not None:
        raise ValueError("quality rules must be simple SQL WHERE statements")


def _validate_contract_payload(table_name: str, contract: dict[str, Any]) -> None:
    catalog, layer, table = _parse_table_name(table_name)

    table_section = contract.get("table")
    schema = contract.get("schema")
    quality = contract.get("quality")

    if not isinstance(table_section, dict):
        raise ValueError("missing table section")
    if not isinstance(schema, list):
        raise ValueError("missing schema section")
    if not isinstance(quality, dict):
        raise ValueError("missing quality section")

    if table_section.get("catalog") != catalog:
        raise ValueError("invalid catalog")
    if table_section.get("layer") != layer:
        raise ValueError("invalid layer")
    if table_section.get("name") != table:
        raise ValueError("invalid table name")
    if table_section.get("full_name") != table_name:
        raise ValueError("invalid full table name")
    if table_section.get("write_mode") not in SUPPORTED_WRITE_MODES:
        raise ValueError("invalid write mode")

    schema_column_names = {
        column.get("name")
        for column in schema
        if isinstance(column, dict)
    }
    if "id" not in schema_column_names or "processed_at" not in schema_column_names:
        raise ValueError("missing standard columns")

    quality_rules = quality.get("rules")
    if not isinstance(quality_rules, list):
        raise ValueError("invalid quality rules")
    if any(not _is_valid_quality_rule(rule) for rule in quality_rules):
        raise ValueError("invalid quality rules")


def _is_valid_quality_rule(quality_rule: str) -> bool:
    return (
        isinstance(quality_rule, str)
        and bool(quality_rule.strip())
        and quality_rule.lstrip().upper().startswith("WHERE ")
    )


if __name__ == "__main__":
    create_data_contract_template(
        table_name=DEFAULT_TABLE_NAME,
        definition=DEFAULT_CONTRACT_DEFINITION,
    )
