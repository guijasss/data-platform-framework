from typing import Any, NoReturn, Sequence
from os import getenv
from yaml import safe_load
from pathlib import Path

from src.exceptions import DataContractNotFoundError
from src.protocols import DEFAULT_CONTRACTS_DIR


def get_env_or_raise(env_variable_name: str) -> str:
    value = getenv(env_variable_name)
    if not value:
        raise EnvironmentError(f"Environment variable {env_variable_name} not defined!")
    return value


def validate_required_fields(
    instance: Any,
    required_fields: Sequence[str],
) -> NoReturn:
    missing_fields = [
        field_name
        for field_name in required_fields
        if getattr(instance, field_name) is None
    ]

    if missing_fields:
        raise ValueError(
            f"Required fields are missing: {', '.join(missing_fields)}"
        )


def load_data_contract_yaml(target_table: str) -> dict:
    schema, table = target_table.split(".")

    base_path = Path(DEFAULT_CONTRACTS_DIR) / schema

    matches = list(base_path.glob(f"{table}.y*ml"))

    if not matches:
        raise DataContractNotFoundError(target_table)

    with open(matches[0], "r", encoding="utf-8") as f:
        return safe_load(f)
