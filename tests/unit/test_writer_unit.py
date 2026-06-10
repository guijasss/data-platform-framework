import polars as pl
import pytest

from src.exceptions import DataQualityError
from src.writer import (
    WriteConfig,
    _run_data_quality_check,
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

    monkeypatch.setattr("src.writer._run_data_quality_check", lambda target, data: None)
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

    monkeypatch.setattr("src.writer._run_data_quality_check", lambda target, data: None)
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


def test_run_data_quality_check_passes_when_all_error_rules_match(monkeypatch):
    data = pl.DataFrame({"value": [1, 2]}).lazy()
    monkeypatch.setattr(
        "src.writer.load_data_contract_yaml",
        lambda target: {
            "quality_rules": [
                {
                    "name": "positive_value",
                    "expression": "value > 0",
                    "severity": "error",
                }
            ]
        },
    )

    _run_data_quality_check("silver.users", data)


def test_run_data_quality_check_raises_for_failed_error_rule(monkeypatch):
    data = pl.DataFrame({"value": [1, -1]}).lazy()
    monkeypatch.setattr(
        "src.writer.load_data_contract_yaml",
        lambda target: {
            "quality_rules": [
                {
                    "name": "positive_value",
                    "expression": "value > 0",
                    "severity": "error",
                }
            ]
        },
    )

    with pytest.raises(DataQualityError, match="Quality check positive_value failed"):
        _run_data_quality_check("silver.users", data)


def test_run_data_quality_check_logs_warning_for_failed_warning_rule(
    monkeypatch,
    caplog,
):
    data = pl.DataFrame({"value": [1, -1]}).lazy()
    monkeypatch.setattr(
        "src.writer.load_data_contract_yaml",
        lambda target: {
            "quality_rules": [
                {
                    "name": "positive_value",
                    "expression": "value > 0",
                    "severity": "warning",
                }
            ]
        },
    )

    _run_data_quality_check("silver.users", data)

    assert "Quality check positive_value failed" in caplog.text


def test_write_runs_quality_check_before_table_creation(monkeypatch):
    calls = []
    data = pl.DataFrame({"id": [1]}).lazy()

    def quality_check(target_table, frame):
        calls.append(("quality", target_table, frame))

    monkeypatch.setattr("src.writer._run_data_quality_check", quality_check)
    monkeypatch.setattr(
        "src.writer.table_exists",
        lambda table: calls.append(("exists", table)) or True,
    )
    monkeypatch.setattr("src.writer._write_append", lambda config: calls.append(("write", config.method)))

    config = WriteConfig(
        target_table="silver.users",
        method="APPEND",
        data=data,
        comments=None,
    )

    write(config)

    assert calls == [
        ("quality", "silver.users", data),
        ("exists", "silver.users"),
        ("write", "APPEND"),
    ]
