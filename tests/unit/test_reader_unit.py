from unittest.mock import MagicMock

import pytest

from src import reader
from src.exceptions import TableDoesNotExistException


class TestReadTable:

    def test_should_raise_when_table_name_is_empty(self) -> None:
        with pytest.raises(ValueError, match="table_name must be a non-empty string"):
            reader.read_table(connection=MagicMock(), table_name="")

    def test_should_raise_when_connection_is_missing(self) -> None:
        with pytest.raises(ValueError, match="connection is required"):
            reader.read_table(connection=None, table_name="customers")

    def test_should_raise_when_table_does_not_exist(self, monkeypatch) -> None:
        monkeypatch.setattr(reader, "_table_exists", lambda *_: False)

        with pytest.raises(TableDoesNotExistException):
            reader.read_table(connection=MagicMock(), table_name="customers")

    def test_should_read_table_without_filter(self, monkeypatch) -> None:
        polars = MagicMock()
        expected_df = MagicMock()
        polars.read_database.return_value = expected_df

        monkeypatch.setattr(reader, "_table_exists", lambda *_: True)
        monkeypatch.setattr(reader, "_load_polars", lambda: polars)

        result = reader.read_table(connection="db-connection", table_name="customers")

        polars.read_database.assert_called_once_with(
            query="SELECT * FROM customers",
            connection="db-connection",
        )
        assert result is expected_df

    def test_should_read_table_with_filter(self, monkeypatch) -> None:
        polars = MagicMock()

        monkeypatch.setattr(reader, "_table_exists", lambda *_: True)
        monkeypatch.setattr(reader, "_load_polars", lambda: polars)

        reader.read_table(
            connection="db-connection",
            table_name="public.customers",
            where="is_active = true",
        )

        polars.read_database.assert_called_once_with(
            query="SELECT * FROM public.customers WHERE is_active = true",
            connection="db-connection",
        )


class TestTableExists:

    def test_should_check_table_existence_with_schema(self, monkeypatch) -> None:
        inspector = MagicMock()
        sqlalchemy = MagicMock()
        sqlalchemy.inspect.return_value = inspector

        monkeypatch.setattr(reader, "_load_sqlalchemy", lambda: sqlalchemy)

        reader._table_exists("db-connection", "public.customers")

        sqlalchemy.inspect.assert_called_once_with("db-connection")
        inspector.has_table.assert_called_once_with(
            table_name="customers",
            schema="public",
        )


class TestBuildSelectQuery:

    def test_should_build_query_without_where(self) -> None:
        assert reader._build_select_query("customers") == "SELECT * FROM customers"

    def test_should_build_query_with_where(self) -> None:
        assert (
            reader._build_select_query("customers", "is_active = true")
            == "SELECT * FROM customers WHERE is_active = true"
        )
