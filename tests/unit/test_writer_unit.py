from unittest.mock import MagicMock

import pytest

from src import writer


class TestWriteDeltaTable:

    def test_should_raise_when_target_table_is_empty(self) -> None:
        with pytest.raises(ValueError, match="target_table must be a non-empty string"):
            writer.write_delta_table("", df=MagicMock())

    def test_should_raise_when_df_is_missing(self) -> None:
        with pytest.raises(ValueError, match="df is required"):
            writer.write_delta_table("catalog.silver.sales", df=None)

    def test_should_raise_when_mode_is_not_supported(self) -> None:
        with pytest.raises(
            ValueError,
            match="mode must be one of: append, full_overwrite, incremental",
        ):
            writer.write_delta_table(
                "catalog.silver.sales",
                df=MagicMock(),
                mode="invalid-mode",
            )

    def test_should_raise_when_data_contract_does_not_exist(self, monkeypatch) -> None:
        monkeypatch.setattr(writer, "data_contract_exists", lambda _: False)

        with pytest.raises(
            ValueError,
            match="data contract does not exist for table: catalog.silver.sales",
        ):
            writer.write_delta_table("catalog.silver.sales", df=MagicMock())

    def test_should_raise_when_data_contract_is_invalid(self, monkeypatch) -> None:
        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: False)

        with pytest.raises(
            ValueError,
            match="data contract is invalid for table: catalog.silver.sales",
        ):
            writer.write_delta_table("catalog.silver.sales", df=MagicMock())

    def test_should_append_when_mode_is_append(self, monkeypatch) -> None:
        df = MagicMock()
        df.columns = ["id", "processed_at"]
        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(writer, "validate_data_quality", lambda *_: None)
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "APPEND"},
                "schema": [{"name": "id"}, {"name": "processed_at"}],
            },
        )

        writer.write_delta_table("catalog.silver.sales", df=df, mode="append")

        df.write.format.assert_called_once_with("delta")
        df.write.format.return_value.mode.assert_called_once_with("append")
        df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
            "catalog.silver.sales"
        )

    def test_should_overwrite_when_mode_is_full_overwrite(self, monkeypatch) -> None:
        df = MagicMock()
        df.columns = ["id", "processed_at"]
        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(writer, "validate_data_quality", lambda *_: None)
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "FULL_OVERWRITE"},
                "schema": [{"name": "id"}, {"name": "processed_at"}],
            },
        )

        writer.write_delta_table("catalog.silver.sales", df=df, mode="full_overwrite")

        df.write.format.assert_called_once_with("delta")
        df.write.format.return_value.mode.assert_called_once_with("overwrite")
        df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
            "catalog.silver.sales"
        )

    def test_should_raise_when_mode_does_not_match_contract(self, monkeypatch) -> None:
        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(writer, "validate_data_quality", lambda *_: None)
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "APPEND"},
                "schema": [{"name": "id"}, {"name": "processed_at"}],
            },
        )

        with pytest.raises(
            ValueError,
            match="mode 'full_overwrite' does not match contract write_mode 'APPEND'",
        ):
            writer.write_delta_table(
                "catalog.silver.sales",
                df=MagicMock(),
                mode="full_overwrite",
            )

    def test_should_raise_when_df_is_missing_contract_columns(self, monkeypatch) -> None:
        df = MagicMock()
        df.columns = ["id"]
        prepared_df = MagicMock()
        prepared_df.columns = ["id", "processed_at"]
        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(writer, "validate_data_quality", lambda *_: None)
        monkeypatch.setattr(writer, "current_timestamp", lambda: "ts")
        df.withColumn.return_value = prepared_df
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "APPEND"},
                "schema": [{"name": "id"}, {"name": "processed_at"}, {"name": "amount"}],
            },
        )

        with pytest.raises(
            ValueError,
            match="df is missing columns required by contract: amount",
        ):
            writer.write_delta_table("catalog.silver.sales", df=df, mode="append")

    def test_should_create_table_with_append_when_incremental_target_does_not_exist(
        self,
        monkeypatch,
    ) -> None:
        df = MagicMock()
        df.columns = ["id", "processed_at", "amount"]
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False

        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(writer, "validate_data_quality", lambda *_: None)
        monkeypatch.setattr(writer, "get_spark", lambda: spark)
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "INCREMENTAL", "primary_key": ["id"]},
                "schema": [{"name": "id"}, {"name": "processed_at"}, {"name": "amount"}],
            },
        )

        writer.write_delta_table("catalog.silver.sales", df=df, mode="incremental")

        df.write.format.assert_called_once_with("delta")
        df.write.format.return_value.mode.assert_called_once_with("append")
        df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
            "catalog.silver.sales"
        )

    def test_should_merge_when_incremental_target_exists(self, monkeypatch) -> None:
        df = MagicMock()
        df.columns = ["id", "processed_at", "amount"]
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True

        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(writer, "validate_data_quality", lambda *_: None)
        monkeypatch.setattr(writer, "get_spark", lambda: spark)
        monkeypatch.setattr(writer, "uuid4", lambda: MagicMock(hex="abc123"))
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "INCREMENTAL", "primary_key": ["id"]},
                "schema": [{"name": "id"}, {"name": "processed_at"}, {"name": "amount"}],
            },
        )

        writer.write_delta_table("catalog.silver.sales", df=df, mode="incremental")

        df.createOrReplaceTempView.assert_called_once_with(
            "_tmp_catalog_silver_sales_abc123"
        )
        spark.sql.assert_called_once()
        merge_query = spark.sql.call_args.args[0]
        assert "MERGE INTO catalog.silver.sales AS target" in merge_query
        assert "USING _tmp_catalog_silver_sales_abc123 AS source" in merge_query
        assert "target.`id` = source.`id`" in merge_query
        assert "WHEN MATCHED THEN UPDATE SET" in merge_query
        assert "WHEN NOT MATCHED THEN INSERT" in merge_query
        spark.catalog.dropTempView.assert_called_once_with(
            "_tmp_catalog_silver_sales_abc123"
        )

    def test_should_infer_processed_at_when_column_is_missing(self, monkeypatch) -> None:
        df = MagicMock()
        df.columns = ["id", "amount"]
        prepared_df = MagicMock()
        prepared_df.columns = ["id", "amount", "processed_at"]

        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(writer, "validate_data_quality", lambda *_: None)
        monkeypatch.setattr(writer, "current_timestamp", lambda: "ts")
        df.withColumn.return_value = prepared_df
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "APPEND"},
                "schema": [{"name": "id"}, {"name": "processed_at"}, {"name": "amount"}],
            },
        )

        writer.write_delta_table("catalog.silver.sales", df=df, mode="append")

        df.withColumn.assert_called_once_with("processed_at", "ts")
        prepared_df.write.format.assert_called_once_with("delta")

    def test_should_run_data_quality_validation_before_writing(self, monkeypatch) -> None:
        df = MagicMock()
        df.columns = ["id", "processed_at"]
        validate_data_quality = MagicMock()

        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(writer, "validate_data_quality", validate_data_quality)
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "APPEND"},
                "schema": [{"name": "id"}, {"name": "processed_at"}],
                "quality": {"rules": ["WHERE id IS NOT NULL"]},
            },
        )

        writer.write_delta_table("catalog.silver.sales", df=df, mode="append")

        validate_data_quality.assert_called_once()

    def test_should_raise_when_data_quality_validation_fails(self, monkeypatch) -> None:
        df = MagicMock()
        df.columns = ["id", "processed_at"]

        monkeypatch.setattr(writer, "data_contract_exists", lambda _: True)
        monkeypatch.setattr(writer, "data_contract_is_valid", lambda _: True)
        monkeypatch.setattr(
            writer,
            "validate_data_quality",
            lambda *_: (_ for _ in ()).throw(ValueError("quality failed")),
        )
        monkeypatch.setattr(
            writer,
            "load_data_contract",
            lambda _: {
                "table": {"write_mode": "APPEND"},
                "schema": [{"name": "id"}, {"name": "processed_at"}],
                "quality": {"rules": ["WHERE id IS NOT NULL"]},
            },
        )

        with pytest.raises(ValueError, match="quality failed"):
            writer.write_delta_table("catalog.silver.sales", df=df, mode="append")
