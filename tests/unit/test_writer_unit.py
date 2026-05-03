from unittest.mock import MagicMock

import pytest

from src import writer


class TestWriteTable:

    def test_should_raise_when_table_name_is_empty(self) -> None:
        with pytest.raises(ValueError, match="table_name must be a non-empty string"):
            writer.write_table(connection=MagicMock(), table_name="", df=MagicMock())

    def test_should_raise_when_connection_is_missing(self) -> None:
        with pytest.raises(ValueError, match="connection is required"):
            writer.write_table(connection=None, table_name="customers", df=MagicMock())

    def test_should_raise_when_df_is_missing(self) -> None:
        with pytest.raises(ValueError, match="df is required"):
            writer.write_table(connection=MagicMock(), table_name="customers", df=None)

    def test_should_raise_when_mode_is_invalid(self) -> None:
        with pytest.raises(
            ValueError,
            match="mode must be one of: append, fail, replace",
        ):
            writer.write_table(
                connection=MagicMock(),
                table_name="customers",
                df=MagicMock(),
                mode="overwrite",
            )

    def test_should_write_dataframe_with_selected_mode(self, monkeypatch) -> None:
        df = MagicMock()

        monkeypatch.setattr(writer, "_load_polars", lambda: MagicMock())

        writer.write_table(
            connection="db-connection",
            table_name="customers",
            df=df,
            mode="replace",
        )

        df.write_database.assert_called_once_with(
            table_name="customers",
            connection="db-connection",
            if_table_exists="replace",
        )
