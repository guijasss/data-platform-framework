from typing import Optional, Sequence
from datetime import datetime

from polars import col, LazyFrame, read_database_uri
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Engine

from src.helpers import get_env_or_raise
from src.protocols import WATERMARK_COLUMN


def make_engine() -> Engine:
    return create_engine(_build_full_uri())


def table_exists(table_name: str) -> bool:
    engine = make_engine()
    insp = inspect(engine)
    return insp.has_table(table_name)


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

def _build_full_uri() -> str:
    host = get_env_or_raise("DATABASE_HOST")
    user = get_env_or_raise("DATABASE_USER")
    password = get_env_or_raise("DATABASE_PASSWORD")
    database = get_env_or_raise("DATABASE_NAME")
    return f"postgresql+psycopg://{user}:{password}@{host}/{database}"


def read_table(
    table: str,
    columns: Optional[Sequence[str]] = None,
    where: Optional[Sequence[str]] = None,
) -> LazyFrame:
    engine = make_engine()
    inspector = inspect(engine)

    if not table_exists(table):
        raise ValueError(f"Tabela não existe: {table}")

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
        uri=_build_full_uri(),
        engine="connectorx",
    ).lazy()
