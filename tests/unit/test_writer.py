import polars as pl
import pytest

from src.writer import (
    WriteConfig,
    _write_append,
    _write_merge,
    _write_overwrite,
    write,
)


def test_write_append_and_merge_are_noops():
    config = WriteConfig(
        target_table="silver.users",
        method="APPEND",
        data=pl.DataFrame({"id": [1]}).lazy(),
        comments=None,
    )

    assert _write_append(config) is None
    assert _write_merge(config) is None


def test_write_overwrite_replaces_table(monkeypatch):
    calls = {}

    class Collected:
        def write_database(self, **kwargs):
            calls.update(kwargs)

    class Data:
        def collect(self):
            return Collected()

    monkeypatch.setattr("src.writer.make_engine", lambda kind: "engine")

    config = WriteConfig(
        target_table="silver.users",
        method="OVERWRITE",
        data=Data(),
        comments=None,
    )

    assert _write_overwrite(config) is None
    assert calls == {
        "table_name": "silver.users",
        "connection": "engine",
        "if_table_exists": "replace",
    }


def test_write_creates_missing_table_then_dispatches(monkeypatch, capsys):
    calls = []
    data = pl.DataFrame({"id": [1]}).lazy()

    monkeypatch.setattr("src.writer.table_exists", lambda table: False)
    monkeypatch.setattr(
        "src.writer.generate_create_table_sql",
        lambda data, table_name, column_comments: "CREATE TABLE silver.users;",
    )
    monkeypatch.setattr("src.writer.execute_sql", lambda sql: calls.append(sql))

    config = WriteConfig(
        target_table="silver.users",
        method="APPEND",
        data=data,
        comments={"id": "identifier"},
    )

    assert write(config) is None
    assert calls == ["CREATE TABLE silver.users;"]
    assert "Table doesn't exist!" in capsys.readouterr().out


def test_write_dispatches_existing_table_to_overwrite(monkeypatch):
    calls = {}

    class Collected:
        def write_database(self, **kwargs):
            calls.update(kwargs)

    class Data:
        def collect(self):
            return Collected()

    monkeypatch.setattr("src.writer.table_exists", lambda table: True)
    monkeypatch.setattr("src.writer.make_engine", lambda kind: "engine")

    config = WriteConfig(
        target_table="silver.users",
        method="OVERWRITE",
        data=Data(),
        comments=None,
    )

    assert write(config) is None
    assert calls["if_table_exists"] == "replace"


def test_write_raises_for_unknown_method(monkeypatch):
    monkeypatch.setattr("src.writer.table_exists", lambda table: True)

    config = WriteConfig(
        target_table="silver.users",
        method="INVALID",
        data=pl.DataFrame({"id": [1]}).lazy(),
        comments=None,
    )

    with pytest.raises(ValueError, match="Invalid write method: INVALID"):
        write(config)
