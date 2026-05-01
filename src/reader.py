from __future__ import annotations

from datetime import datetime
from typing import Callable

from pyspark.sql import DataFrame

from src.exceptions import TableDoesNotExistException
from src.protocols import ReadMethod
from src.utils.catalog import get_table_watermark, table_exists
from src.utils.config import load_config, WATERMARK_COLUMN, get_spark

def _base_read(source_table: str) -> DataFrame:
    return get_spark().table(source_table)


def _incremental_read(source_table: str, watermark: datetime) -> DataFrame:
    return _base_read(source_table).where(f"{WATERMARK_COLUMN} > TIMESTAMP '{watermark.isoformat(sep=' ')}'")


def _should_check_updates() -> bool:
    return load_config().check_updates


def _has_updates(source_table: str, watermark: datetime | None) -> bool:
    if watermark is None:
        return True

    latest_watermark = get_table_watermark(source_table)
    if latest_watermark is None:
        return False

    return latest_watermark > watermark


def _read_full_load(source_table: str, watermark: datetime | None) -> DataFrame:
    del watermark
    return _base_read(source_table)


def _read_incremental(source_table: str, watermark: datetime | None) -> DataFrame:
    if watermark is None:
        raise ValueError("watermark is required for incremental reads")

    return _incremental_read(source_table, watermark)


READ_METHOD_HANDLERS: dict[ReadMethod, Callable[[str, datetime | None], DataFrame]] = {
    ReadMethod.FULL_LOAD: _read_full_load,
    ReadMethod.INCREMENTAL: _read_incremental,
}


def read_delta_table(
    source_table: str,
    method: ReadMethod = ReadMethod.FULL_LOAD,
    watermark: datetime | None = None,
) -> DataFrame:
    if not table_exists(source_table):
        raise TableDoesNotExistException(source_table)

    if _should_check_updates() and not _has_updates(source_table, watermark):
        return _base_read(source_table).limit(0)

    handler = READ_METHOD_HANDLERS.get(method)
    if handler is None:
        raise ValueError(f"unsupported read method: {method}")

    return handler(source_table, watermark)
