from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src import reader
from src.exceptions import TableDoesNotExistException
from src.utils.config import FrameworkConfig


# ---------------------------------------------------------------------------
# read_delta_table
# ---------------------------------------------------------------------------
class TestReadDeltaTable:

    def test_full_load_returns_base_read(self, monkeypatch):
        expected_df = MagicMock(name="full_load_df")

        monkeypatch.setattr(reader, "table_exists", lambda _: True)
        monkeypatch.setattr(reader, "_should_check_updates", lambda: False)
        monkeypatch.setattr(reader, "_base_read", lambda _: expected_df)

        result = reader.read_delta_table("source_table")

        assert result is expected_df

    def test_incremental_uses_incremental_read(self, monkeypatch):
        watermark = datetime(2024, 1, 1, 12, 0, 0)
        expected_df = MagicMock(name="incremental_df")

        monkeypatch.setattr(reader, "table_exists", lambda _: True)
        monkeypatch.setattr(reader, "_should_check_updates", lambda: False)
        monkeypatch.setattr(
            reader,
            "_incremental_read",
            lambda source_table, current_watermark: expected_df
            if (source_table, current_watermark) == ("source_table", watermark)
            else None,
        )

        result = reader.read_delta_table(
            "source_table",
            watermark=watermark,
        )

        assert result is expected_df

    def test_read_uses_implicit_full_load_when_watermark_is_not_provided(self, monkeypatch):
        expected_df = MagicMock(name="full_load_df")

        monkeypatch.setattr(reader, "table_exists", lambda _: True)
        monkeypatch.setattr(reader, "_should_check_updates", lambda: False)
        monkeypatch.setattr(
            reader,
            "_read",
            lambda source_table, watermark: expected_df
            if (source_table, watermark) == ("source_table", None)
            else None,
        )

        result = reader.read_delta_table("source_table")

        assert result is expected_df

    def test_read_uses_implicit_incremental_load_when_watermark_is_provided(self, monkeypatch):
        watermark = datetime(2024, 1, 1, 12, 0, 0)
        expected_df = MagicMock(name="incremental_df")

        monkeypatch.setattr(reader, "table_exists", lambda _: True)
        monkeypatch.setattr(reader, "_should_check_updates", lambda: False)
        monkeypatch.setattr(
            reader,
            "_read",
            lambda source_table, current_watermark: expected_df
            if (source_table, current_watermark) == ("source_table", watermark)
            else None,
        )

        result = reader.read_delta_table("source_table", watermark=watermark)

        assert result is expected_df

    def test_missing_table_raises(self, monkeypatch):
        monkeypatch.setattr(reader, "table_exists", lambda _: False)

        with pytest.raises(TableDoesNotExistException):
            reader.read_delta_table("missing_table")

    def test_enabled_update_check_returns_empty_dataframe_when_no_updates(self, monkeypatch):
        base_df = MagicMock(name="base_df")
        empty_df = MagicMock(name="empty_df")
        base_df.limit.return_value = empty_df

        monkeypatch.setattr(reader, "table_exists", lambda _: True)
        monkeypatch.setattr(reader, "_should_check_updates", lambda: True)
        monkeypatch.setattr(reader, "_has_updates", lambda *_: False)
        monkeypatch.setattr(reader, "_base_read", lambda _: base_df)

        result = reader.read_delta_table(
            "source_table",
            watermark=datetime(2024, 1, 1, 12, 0, 0),
        )

        base_df.limit.assert_called_once_with(0)
        assert result is empty_df

    def test_enabled_update_check_still_reads_everything_when_watermark_is_not_provided(self, monkeypatch):
        expected_df = MagicMock(name="full_load_df")

        monkeypatch.setattr(reader, "table_exists", lambda _: True)
        monkeypatch.setattr(reader, "_should_check_updates", lambda: True)
        monkeypatch.setattr(reader, "_has_updates", lambda *_: True)
        monkeypatch.setattr(
            reader,
            "_read",
            lambda source_table, watermark: expected_df
            if (source_table, watermark) == ("source_table", None)
            else None,
        )

        result = reader.read_delta_table("source_table")

        assert result is expected_df


# ---------------------------------------------------------------------------
# _should_check_updates
# ---------------------------------------------------------------------------
class TestShouldCheckUpdates:

    def test_reads_flag_from_framework_config(self, monkeypatch):
        monkeypatch.setattr(
            reader,
            "load_config",
            lambda: FrameworkConfig(check_updates=True),
        )

        assert reader._should_check_updates() is True


# ---------------------------------------------------------------------------
# _has_updates
# ---------------------------------------------------------------------------
class TestHasUpdates:

    def test_returns_true_when_watermark_is_not_provided(self):
        assert reader._has_updates("source_table", None) is True

    def test_returns_false_when_latest_watermark_is_missing(self, monkeypatch):
        watermark = datetime(2024, 1, 1, 12, 0, 0)

        monkeypatch.setattr(reader, "get_table_watermark", lambda _: None)

        assert reader._has_updates("source_table", watermark) is False

    def test_returns_true_when_latest_watermark_is_newer(self, monkeypatch):
        watermark = datetime(2024, 1, 1, 12, 0, 0)

        monkeypatch.setattr(
            reader,
            "get_table_watermark",
            lambda _: datetime(2024, 1, 2, 0, 0, 0),
        )

        assert reader._has_updates("source_table", watermark) is True

    def test_returns_false_when_latest_watermark_is_not_newer(self, monkeypatch):
        watermark = datetime(2024, 1, 1, 12, 0, 0)

        monkeypatch.setattr(
            reader,
            "get_table_watermark",
            lambda _: datetime(2024, 1, 1, 0, 0, 0),
        )

        assert reader._has_updates("source_table", watermark) is False


# ---------------------------------------------------------------------------
# _read
# ---------------------------------------------------------------------------
class TestRead:

    def test_returns_base_read_when_watermark_is_not_provided(self, monkeypatch):
        expected_df = MagicMock(name="full_load_df")

        monkeypatch.setattr(reader, "_base_read", lambda _: expected_df)

        result = reader._read("source_table", None)

        assert result is expected_df

    def test_returns_incremental_read_when_watermark_is_provided(self, monkeypatch):
        watermark = datetime(2024, 1, 1, 12, 0, 0)
        expected_df = MagicMock(name="incremental_df")

        monkeypatch.setattr(
            reader,
            "_incremental_read",
            lambda source_table, current_watermark: expected_df
            if (source_table, current_watermark) == ("source_table", watermark)
            else None,
        )

        result = reader._read("source_table", watermark)

        assert result is expected_df
