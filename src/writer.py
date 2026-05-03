from __future__ import annotations

from importlib import import_module
from typing import Any


VALID_WRITE_MODES = {"append", "replace", "fail"}


def write_table(
    connection: Any,
    table_name: str,
    df: Any,
    mode: str = "append",
) -> None:
    if not table_name or not table_name.strip():
        raise ValueError("table_name must be a non-empty string")

    if connection is None:
        raise ValueError("connection is required")

    if df is None:
        raise ValueError("df is required")

    normalized_mode = mode.lower()
    if normalized_mode not in VALID_WRITE_MODES:
        raise ValueError(
            f"mode must be one of: {', '.join(sorted(VALID_WRITE_MODES))}"
        )

    _load_polars()
    df.write_database(
        table_name=table_name,
        connection=connection,
        if_table_exists=normalized_mode,
    )


def _load_polars() -> Any:
    try:
        return import_module("polars")
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("polars is required to write tables") from exc
