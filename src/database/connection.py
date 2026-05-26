from typing import Any, Literal, Optional, Sequence
from datetime import datetime

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Result
from sqlalchemy.engine.base import Engine
from polars import col, LazyFrame, read_database_uri

from src.helpers import get_env_or_raise
from src.protocols import WATERMARK_COLUMN


ConnectionKind = Literal["sqlalchemy", "polars"]


def make_engine(kind: ConnectionKind) -> Engine:
    return create_engine(_build_full_uri(kind=kind))


def table_exists(full_table_name: str) -> bool:
    engine = make_engine(kind="sqlalchemy")
    insp = inspect(engine)

    schema, table = full_table_name.split(".")

    return insp.has_table(table, schema=schema)


def get_table_watermark(table_name: str) -> datetime | None:
    result = (
        read_table(
            table=table_name,
            columns=[WATERMARK_COLUMN],
        )
        .select(col(WATERMARK_COLUMN).max().alias("watermark"))
        .collect()
    )

    return result.item()


def _build_full_uri(kind: Literal["sqlalchemy", "polars"]) -> str:
    host = get_env_or_raise("DATABASE_HOST")
    user = get_env_or_raise("DATABASE_USER")
    password = get_env_or_raise("DATABASE_PASSWORD")
    database = get_env_or_raise("DATABASE_NAME")

    uri_format_mapping = {
        "polars": f"postgresql://{user}:{password}@{host}:5432/{database}",
        "sqlalchemy": f"postgresql+psycopg://{user}:{password}@{host}:5432/{database}"
    }

    if not kind in uri_format_mapping:
        raise ValueError(f"Invalid kind of connection: {kind}")

    return uri_format_mapping.get(kind)


def execute_sql(
    query: str,
    params: dict[str, Any] | None = None,
) -> Result:
    engine = make_engine(kind="sqlalchemy")

    with engine.begin() as connection:
        return connection.execute(
            text(query),
            params or {},
        )


def read_table(
    table: str,
    columns: Optional[Sequence[str]] = None,
    where: Optional[Sequence[str]] = None,
) -> LazyFrame:

    if columns:
        selected_columns = ", ".join(columns)
    else:
        selected_columns = "*"

    query = f"""
        SELECT {selected_columns}
        FROM {table}
    """

    if where:
        where_clause = " AND ".join(f"({condition})" for condition in where)
        query += f"""
        WHERE {where_clause}
        """

    return read_database_uri(
        query=query,
        uri=_build_full_uri(kind="polars"),
        engine="connectorx"
    ).lazy()
