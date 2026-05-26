from dataclasses import dataclass
from datetime import datetime
from typing import get_args, List, Literal, Optional

from polars import LazyFrame

from src.database.connection import get_table_watermark, read_table, table_exists
from src.protocols import WATERMARK_COLUMN


ReadMethod = Literal["INCREMENTAL", "FULL_LOAD"]

@dataclass(frozen=True)
class CONSTANTS:
    methods = get_args(ReadMethod)
    target_watermark_column = "processed_at"

@dataclass
class ReadConfig:
    source_table: str
    method: ReadMethod
    target_table: Optional[str] = None
    source_watermark_column: Optional[str] = None
    columns: Optional[List[str]] = None


def _valid_read_method(method: str | None) -> str | None:
    if method not in CONSTANTS.methods:
        return f"Invalid read method: {method}. Options are: {CONSTANTS.methods}"

    return None


def _run_validations(config: ReadConfig) -> None:
    validations: list = [
        _valid_read_method(config.method)
    ]

    while validations:
        error_message = validations.pop(0)
        if error_message:
            raise ValueError(error_message)


def _read_incremental(config: ReadConfig) -> LazyFrame:
    watermark_value: datetime = get_table_watermark(config.target_table)
    
    base_reader = read_table(
        table=config.source_table,
        columns=config.columns,
        where=[f"{WATERMARK_COLUMN} > {watermark_value}"]
    )

    return base_reader


def _read_full_load(config: ReadConfig) -> LazyFrame:
    return read_table(
        table=config.source_table,
        columns=config.columns
    )


def read(config: ReadConfig) -> LazyFrame:
    _run_validations(config)

    read_methods_callable_mapping = {
        "FULL_LOAD": _read_full_load,
        "INCREMENTAL": _read_incremental
    }

    if not table_exists(config.source_table):
        raise ValueError(f"Table {config.source_table} doesn't exist!")

    if not table_exists(config.target_table or "raw.default"):
        return _read_full_load(config)
    
    return read_methods_callable_mapping[config.method](config)
