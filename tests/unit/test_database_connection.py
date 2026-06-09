from datetime import datetime

import pytest

from src.database import connection


def test_build_full_uri_for_sqlalchemy(monkeypatch):
    monkeypatch.setenv("DATABASE_HOST", "localhost")
    monkeypatch.setenv("DATABASE_PORT", "5433")
    monkeypatch.setenv("DATABASE_USER", "engineering")
    monkeypatch.setenv("DATABASE_PASSWORD", "app")
    monkeypatch.setenv("DATABASE_NAME", "catalog_test")

    assert (
        connection._build_full_uri("sqlalchemy")
        == "postgresql+psycopg://engineering:app@localhost:5433/catalog_test"
    )


def test_build_full_uri_for_polars_defaults_port(monkeypatch):
    monkeypatch.setenv("DATABASE_HOST", "postgres")
    monkeypatch.delenv("DATABASE_PORT", raising=False)
    monkeypatch.setenv("DATABASE_USER", "engineering")
    monkeypatch.setenv("DATABASE_PASSWORD", "app")
    monkeypatch.setenv("DATABASE_NAME", "catalog")

    assert (
        connection._build_full_uri("polars")
        == "postgresql://engineering:app@postgres:5432/catalog"
    )


def test_build_full_uri_raises_for_invalid_kind(monkeypatch):
    monkeypatch.setenv("DATABASE_HOST", "localhost")
    monkeypatch.setenv("DATABASE_USER", "engineering")
    monkeypatch.setenv("DATABASE_PASSWORD", "app")
    monkeypatch.setenv("DATABASE_NAME", "catalog")

    with pytest.raises(ValueError, match="Invalid kind of connection: invalid"):
        connection._build_full_uri("invalid")


def test_make_engine_uses_built_uri(monkeypatch):
    calls = {}

    monkeypatch.setattr(connection, "_build_full_uri", lambda kind: f"uri:{kind}")
    monkeypatch.setattr(
        connection,
        "create_engine",
        lambda uri: calls.setdefault("uri", uri) or "engine",
    )

    assert connection.make_engine("sqlalchemy") == "uri:sqlalchemy"
    assert calls["uri"] == "uri:sqlalchemy"


def test_table_exists_checks_schema_and_table(monkeypatch):
    class Inspector:
        def has_table(self, table, schema=None):
            self.table = table
            self.schema = schema
            return True

    inspector = Inspector()

    monkeypatch.setattr(connection, "make_engine", lambda kind: "engine")
    monkeypatch.setattr(connection, "inspect", lambda engine: inspector)

    assert connection.table_exists("raw.users") is True
    assert inspector.schema == "raw"
    assert inspector.table == "users"


def test_get_table_watermark_reads_max_watermark(monkeypatch):
    watermark = datetime(2024, 1, 1, 10, 0, 0)

    class Collected:
        def item(self):
            return watermark

    class Selected:
        def collect(self):
            return Collected()

    class Table:
        def select(self, expression):
            self.expression = expression
            return Selected()

    fake_table = Table()
    monkeypatch.setattr(connection, "read_table", lambda table, columns: fake_table)

    assert connection.get_table_watermark("silver.users") == watermark


def test_execute_sql_executes_inside_transaction(monkeypatch):
    calls = {}

    class Transaction:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params):
            calls["query"] = query
            calls["params"] = params
            return "result"

    class Engine:
        def begin(self):
            return Transaction()

    monkeypatch.setattr(connection, "make_engine", lambda kind: Engine())

    assert connection.execute_sql("SELECT :id", {"id": 1}) == "result"
    assert str(calls["query"]) == "SELECT :id"
    assert calls["params"] == {"id": 1}


def test_execute_sql_defaults_params(monkeypatch):
    calls = {}

    class Transaction:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params):
            calls["params"] = params
            return "result"

    class Engine:
        def begin(self):
            return Transaction()

    monkeypatch.setattr(connection, "make_engine", lambda kind: Engine())

    assert connection.execute_sql("SELECT 1") == "result"
    assert calls["params"] == {}


def test_read_table_builds_query_with_columns_and_where(monkeypatch):
    calls = {}

    class DataFrame:
        def lazy(self):
            return "lazy-frame"

    def read_database_uri(**kwargs):
        calls.update(kwargs)
        return DataFrame()

    monkeypatch.setattr(connection, "_build_full_uri", lambda kind: "postgresql://uri")
    monkeypatch.setattr(connection, "read_database_uri", read_database_uri)

    result = connection.read_table(
        table="raw.users",
        columns=["id", "name"],
        where=["id > 1", "name <> 'Alice'"],
    )

    assert result == "lazy-frame"
    assert "SELECT id, name" in calls["query"]
    assert "FROM raw.users" in calls["query"]
    assert "WHERE (id > 1) AND (name <> 'Alice')" in calls["query"]
    assert calls["uri"] == "postgresql://uri"
    assert calls["engine"] == "connectorx"


def test_read_table_builds_query_with_all_columns(monkeypatch):
    calls = {}

    class DataFrame:
        def lazy(self):
            return "lazy-frame"

    monkeypatch.setattr(connection, "_build_full_uri", lambda kind: "postgresql://uri")
    monkeypatch.setattr(
        connection,
        "read_database_uri",
        lambda **kwargs: calls.update(kwargs) or DataFrame(),
    )

    assert connection.read_table(table="raw.users") == "lazy-frame"
    assert "SELECT *" in calls["query"]
    assert "WHERE" not in calls["query"]
