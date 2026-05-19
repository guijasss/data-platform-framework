from dataclasses import dataclass
from typing import Literal, Optional

from polars import DataFrame

from src.database import table_exists


read_methods = ["INCREMENTAL", "FULL_LOAD"]
ReadMethod = Literal[*read_methods]

@dataclass
class ReadConfig:
    source_table: str
    method: str
    target_table: Optional[str]


def _run_validations(config: ReadConfig) -> None:
    validations: list = [
        _valid_read_method(config.method)
        
    ]

    while validations:
        error_message = validations.pop(0)
        if error_message:
            raise ValueError(error_message)


def _valid_read_method(method: str | None) -> str | None:
    if method not in read_methods:
        return f"Invalid read method: {method}. Options are: {read_methods}"

    return None

def _read_incremental(source_table: str) -> DataFrame: ...
def _read_full_load() -> DataFrame: ...


def read(config: ReadConfig) -> DataFrame:
    _run_validations(config)

    read_methods_callable_mapping = {
        "FULL_LOAD": _read_full_load,
        "INCREMENTAL": _read_incremental
    }

    if not table_exists(config.target_table):
        return _read_full_load(config)
    
    return read_methods_callable_mapping[config.method](config)
