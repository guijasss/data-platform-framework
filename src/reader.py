from __future__ import annotations
from typing import Optional
from datetime import datetime

from pyspark.sql import DataFrame

from src.exceptions import TableDoesNotExistException
from src.utils.catalog import get_table_watermark, table_exists
from src.utils.config import load_config, WATERMARK_COLUMN, get_spark

def _base_read(source_table: str) -> DataFrame:
    return get_spark().table(source_table)


def _incremental_read(source_table: str, watermark: datetime) -> DataFrame:
    return _base_read(source_table).where(f"{WATERMARK_COLUMN} > TIMESTAMP '{watermark.isoformat(sep=' ')}'")


def _should_check_updates() -> bool:
    return load_config().check_updates


def _has_updates(source_table: str, watermark: Optional[datetime]) -> bool:
    if watermark is None:
        return True

    latest_watermark = get_table_watermark(source_table)
    if latest_watermark is None:
        return False

    return latest_watermark > watermark


def _read(source_table: str, watermark: Optional[datetime]) -> DataFrame:
    if watermark is None:
        return _base_read(source_table)

    return _incremental_read(source_table, watermark)


def read_delta_table(
    source_table: str,
    watermark: Optional[datetime] = None,
) -> DataFrame:
    if not table_exists(source_table):
        raise TableDoesNotExistException(source_table)

    if _should_check_updates() and not _has_updates(source_table, watermark):
        return _base_read(source_table).limit(0)

    return _read(source_table, watermark)
