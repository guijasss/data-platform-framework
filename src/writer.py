from dataclasses import dataclass
from typing import get_args, Literal, Mapping, Optional
from logging import getLogger

from polars import LazyFrame, col, sql_expr

from src.database.connection import execute_sql, make_engine, table_exists
from src.database.helpers import generate_create_table_sql
from src.exceptions import DataQualityError
from src.helpers import load_data_contract_yaml


logger = getLogger(__name__)

WriteMethod = Literal["APPEND", "MERGE", "OVERWRITE"]


@dataclass(frozen=True)
class CONSTANTS:
    methods = get_args(WriteMethod)


@dataclass
class WriteConfig:
    target_table: str
    method: WriteMethod
    data: LazyFrame
    comments: Optional[Mapping[str,str]]


def _run_validations(config: WriteConfig) -> None:
    validations: list = [
        (
            f"Invalid write method: {config.method}. Options are: {CONSTANTS.methods}"
            if config.method not in CONSTANTS.methods
            else None
        )
    ]

    while validations:
        error_message = validations.pop(0)
        if error_message:
            raise ValueError(error_message)
        

def _run_data_quality_check(target_table: str, data: LazyFrame) -> None:
    contract = load_data_contract_yaml(target_table)

    for column_contract in contract.get("schema", []):
        column_name = column_contract["column"]

        if column_contract.get("nullable", True) is False:
            null_exists = (
                data
                .filter(col(column_name).is_null())
                .limit(1)
                .collect()
                .height > 0
            )

            if null_exists:
                raise DataQualityError(
                    f"Column {column_name} failed nullable check"
                )

        if column_contract.get("unique", False) is True:
            duplicate_exists = (
                data
                .filter(col(column_name).is_duplicated())
                .limit(1)
                .collect()
                .height > 0
            )

            if duplicate_exists:
                raise DataQualityError(
                    f"Column {column_name} failed unique check"
                )

    quality_checks = contract.get("quality_rules", [])

    for check in quality_checks:
        invalid_exists = (
            data
            .filter(~sql_expr(check["expression"]))
            .limit(1)
            .collect()
            .height > 0
        )

        if invalid_exists:
            message = f"Quality check {check['name']} failed"
            if check["severity"] == "error":
                raise DataQualityError(message)

            if check["severity"] == "warning":
                logger.warning(message)


def _write_append(config: WriteConfig): ...

def _write_merge(config: WriteConfig): ...

def _write_overwrite(config: WriteConfig):
    (
        config.data
        .collect()
        .write_database(
            table_name=config.target_table,
            connection=make_engine(kind="sqlalchemy"),
            if_table_exists="replace",
        )
    )


def write(config: WriteConfig) -> LazyFrame:
    _run_validations(config)
    _run_data_quality_check(config.target_table, config.data)

    write_methods_callable_mapping = {
        "APPEND": _write_append,
        "MERGE": _write_merge,
        "OVERWRITE": _write_overwrite
    }

    if not table_exists(config.target_table):
        create_table_statement: str = generate_create_table_sql(
            data=config.data,
            table_name=config.target_table,
            column_comments=config.comments
        )
        print("Table doesn't exist! Executing create table statement...", end=" ")
        execute_sql(create_table_statement)
        print("Done!")
    
    return write_methods_callable_mapping[config.method](config)
