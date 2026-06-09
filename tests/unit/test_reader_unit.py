from datetime import datetime

import pytest

from src.protocols import WATERMARK_COLUMN
from src.reader import ReadConfig, _valid_read_method, read


def test_valid_read_method_accepts_known_method():
    assert _valid_read_method("FULL_LOAD") is None


def test_read_raises_for_invalid_method():
    config = ReadConfig(source_table="raw.users", method="INVALID")

    with pytest.raises(ValueError, match="Invalid read method: INVALID"):
        read(config)


def test_read_raises_when_source_table_does_not_exist(monkeypatch):
    monkeypatch.setattr("src.reader.table_exists", lambda table: False)

    config = ReadConfig(source_table="raw.missing", method="FULL_LOAD")

    with pytest.raises(ValueError, match="Table raw.missing doesn't exist!"):
        read(config)


def test_read_falls_back_to_full_load_when_target_table_does_not_exist(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "src.reader.table_exists",
        lambda table: table == "raw.users",
    )
    monkeypatch.setattr(
        "src.reader.read_table",
        lambda **kwargs: calls.append(kwargs) or "full-load",
    )

    config = ReadConfig(
        source_table="raw.users",
        target_table="silver.users",
        method="INCREMENTAL",
        columns=["id"],
    )

    assert read(config) == "full-load"
    assert calls == [{"table": "raw.users", "columns": ["id"]}]


def test_read_full_load_when_target_table_is_none(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "src.reader.table_exists",
        lambda table: table in {"raw.users"},
    )
    monkeypatch.setattr(
        "src.reader.read_table",
        lambda **kwargs: calls.append(kwargs) or "full-load",
    )

    config = ReadConfig(
        source_table="raw.users",
        method="FULL_LOAD",
        columns=None,
    )

    assert read(config) == "full-load"
    assert calls == [{"table": "raw.users", "columns": None}]


def test_read_incremental_uses_target_watermark(monkeypatch):
    watermark = datetime(2024, 1, 1, 12, 0, 0)
    calls = []

    monkeypatch.setattr("src.reader.table_exists", lambda table: True)
    monkeypatch.setattr("src.reader.get_table_watermark", lambda table: watermark)
    monkeypatch.setattr(
        "src.reader.read_table",
        lambda **kwargs: calls.append(kwargs) or "incremental",
    )

    config = ReadConfig(
        source_table="raw.users",
        target_table="silver.users",
        method="INCREMENTAL",
        columns=["id", WATERMARK_COLUMN],
    )

    assert read(config) == "incremental"
    assert calls == [
        {
            "table": "raw.users",
            "columns": ["id", WATERMARK_COLUMN],
            "where": [f"{WATERMARK_COLUMN} > '{watermark}'"],
        }
    ]
