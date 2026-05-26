from typing import Mapping, Optional

from polars import (
    LazyFrame,
    Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Boolean, String, Date, Time, Datetime, Duration, Binary
)


def quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace('"', '""')}"'


def quote_table_name(table_name: str) -> str:
    return ".".join(
        quote_identifier(part)
        for part in table_name.split(".")
    )

def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


POLARS_TO_POSTGRES_TYPES = {
    Int8: "SMALLINT",
    Int16: "SMALLINT",
    Int32: "INTEGER",
    Int64: "BIGINT",
    UInt8: "SMALLINT",
    UInt16: "INTEGER",
    UInt32: "BIGINT",
    UInt64: "NUMERIC(20, 0)",
    Float32: "REAL",
    Float64: "DOUBLE PRECISION",
    Boolean: "BOOLEAN",
    String: "TEXT",
    Date: "DATE",
    Time: "TIME",
    Datetime: "TIMESTAMP",
    Duration: "INTERVAL",
    Binary: "BYTEA",
}


def generate_create_table_sql(
    data: LazyFrame,
    table_name: str,
    column_comments: Optional[Mapping[str, str]]  = None
) -> str:
    column_comments = column_comments or {}

    schema = data.collect_schema()

    table_sql_name = quote_table_name(table_name)

    columns_sql = []

    for column_name, dtype in schema.items():
        columns_sql.append(
            f"    {quote_identifier(column_name)} {POLARS_TO_POSTGRES_TYPES.get(dtype, "TEXT")}"
        )

    statements = [
        f"CREATE TABLE {table_sql_name} (\n"
        + ",\n".join(columns_sql)
        + "\n);"
    ]

    for column_name, comment in column_comments.items():
        if column_name not in schema:
            raise ValueError(f"Coluna não existe no LazyFrame: {column_name}")

        statements.append(
            f"COMMENT ON COLUMN {table_sql_name}.{quote_identifier(column_name)} "
            f"IS {quote_literal(comment)};"
        )

    return "\n\n".join(statements)
