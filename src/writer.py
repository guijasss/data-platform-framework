from dataclasses import dataclass
from typing import get_args, Literal, Mapping, Optional

from polars import LazyFrame

from src.database.connection import execute_sql, make_engine, table_exists
from src.database.helpers import generate_create_table_sql


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

    ]

    while validations:
        error_message = validations.pop(0)
        if error_message:
            raise ValueError(error_message)
        

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
