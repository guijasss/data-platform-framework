from __future__ import annotations

from importlib import import_module
from typing import Any

from src.exceptions import TableDoesNotExistException


def read_table(
    connection: Any,
    table_name: str,
    where: str | None = None,
) -> Any:
    if not table_name or not table_name.strip():
        raise ValueError("table_name must be a non-empty string")

    if connection is None:
        raise ValueError("connection is required")

    if not _table_exists(connection, table_name):
        raise TableDoesNotExistException(table_name)

    polars = _load_polars()
    query = _build_select_query(table_name, where)
    return polars.read_database(query=query, connection=connection)


def _table_exists(connection: Any, table_name: str) -> bool:
    sqlalchemy = _load_sqlalchemy()
    schema, table = _split_table_name(table_name)
    inspector = sqlalchemy.inspect(connection)
    return bool(inspector.has_table(table_name=table, schema=schema))


def _build_select_query(table_name: str, where: str | None = None) -> str:
    query = f"SELECT * FROM {table_name}"
    if where and where.strip():
        query = f"{query} WHERE {where.strip()}"
    return query


def _split_table_name(table_name: str) -> tuple[str | None, str]:
    parts = [part.strip() for part in table_name.split(".") if part.strip()]
    if len(parts) == 1:
        return None, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError("table_name must follow the format <table> or <schema>.<table>")


def _load_polars() -> Any:
    try:
        return import_module("polars")
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("polars is required to read tables") from exc


def _load_sqlalchemy() -> Any:
    try:
        return import_module("sqlalchemy")
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("sqlalchemy is required to inspect tables") from exc
