from __future__ import annotations

from contextlib import suppress
from typing import Any
from uuid import uuid4

from src import database


def write_table(
    connection: Any,
    table_name: str,
    df: Any,
    mode: str = "append",
) -> None:
    database.validate_connection(connection)
    normalized_table_name = database.validate_table_name(table_name)
    database.validate_dataframe(df)
    normalized_mode = database.normalize_write_mode(mode)

    df.write_database(
        table_name=normalized_table_name,
        connection=connection,
        if_table_exists=normalized_mode,
    )


def write_dataset(
    connection: Any,
    table_name: str,
    df: Any,
    mode: str = "append",
    primary_key: str = "id",
) -> None:
    normalized_mode = database.normalize_framework_write_mode(mode)

    if normalized_mode == "append":
        write_table(connection=connection, table_name=table_name, df=df, mode="append")
        return

    if normalized_mode == "full_overwrite":
        write_table(
            connection=connection,
            table_name=table_name,
            df=df,
            mode="replace",
        )
        return

    merge_table(
        connection=connection,
        table_name=table_name,
        df=df,
        key_columns=[primary_key],
    )


def merge_table(
    connection: Any,
    table_name: str,
    df: Any,
    key_columns: list[str] | tuple[str, ...],
) -> None:
    database.validate_connection(connection)
    normalized_table_name = database.validate_table_name(table_name)
    database.validate_dataframe(df)

    normalized_key_columns = [column.strip() for column in key_columns if column.strip()]
    if not normalized_key_columns:
        raise ValueError("key_columns must contain at least one non-empty column")

    if not database.table_exists(connection, normalized_table_name):
        write_table(
            connection=connection,
            table_name=normalized_table_name,
            df=df,
            mode="append",
        )
        return

    staging_table_name = f"{normalized_table_name}__staging_{uuid4().hex[:8]}"
    write_table(
        connection=connection,
        table_name=staging_table_name,
        df=df,
        mode="replace",
    )

    sqlalchemy = database.load_sqlalchemy()
    merge_query = database.build_merge_query(
        table_name=normalized_table_name,
        staging_table_name=staging_table_name,
        columns=list(df.columns),
        key_columns=normalized_key_columns,
    )

    if hasattr(connection, "begin"):
        with connection.begin() as active_connection:
            active_connection.execute(sqlalchemy.text(merge_query))
    else:
        connection.execute(sqlalchemy.text(merge_query))

    drop_statement = sqlalchemy.text(f"DROP TABLE IF EXISTS {staging_table_name}")
    with suppress(Exception):
        connection.execute(drop_statement)
