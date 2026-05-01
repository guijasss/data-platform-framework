from __future__ import annotations

from uuid import uuid4

from pyspark.sql.functions import current_timestamp

from src.data_contract import (
    data_contract_exists,
    data_contract_is_valid,
    load_data_contract,
)
from src.data_quality import validate_data_quality
from src.utils.config import get_spark


VALID_WRITE_MODES = {"append", "full_overwrite", "incremental"}
CONTRACT_WRITE_MODE_MAPPING = {
    "APPEND": "append",
    "FULL_OVERWRITE": "full_overwrite",
    "INCREMENTAL": "incremental",
}


def write_delta_table(target_table: str, df=None, mode: str = "append") -> None:
    if not target_table or not target_table.strip():
        raise ValueError("target_table must be a non-empty string")

    if df is None:
        raise ValueError("df is required")

    normalized_mode = mode.lower()
    if normalized_mode not in VALID_WRITE_MODES:
        raise ValueError(
            f"mode must be one of: {', '.join(sorted(VALID_WRITE_MODES))}"
        )

    if not data_contract_exists(target_table):
        raise ValueError(f"data contract does not exist for table: {target_table}")

    if not data_contract_is_valid(target_table):
        raise ValueError(f"data contract is invalid for table: {target_table}")

    contract = load_data_contract(target_table)
    _validate_mode_against_contract(normalized_mode, contract)
    prepared_df = _ensure_standard_columns(df)
    _validate_schema_against_contract(prepared_df, contract)
    validate_data_quality(prepared_df, contract)

    if normalized_mode == "append":
        _write_with_save_as_table(target_table, prepared_df, "append")
        return

    if normalized_mode == "full_overwrite":
        _write_with_save_as_table(target_table, prepared_df, "overwrite")
        return

    _write_incremental(target_table, prepared_df, contract)


def _write_with_save_as_table(target_table: str, df, save_mode: str) -> None:
    df.write.format("delta").mode(save_mode).saveAsTable(target_table)


def _write_incremental(target_table: str, df, contract: dict) -> None:
    spark = get_spark()
    if not spark.catalog.tableExists(target_table):
        _write_with_save_as_table(target_table, df, "append")
        return

    primary_key = contract["table"]["primary_key"]
    merge_condition = " AND ".join(
        f"target.`{column}` = source.`{column}`" for column in primary_key
    )
    assignment_clause = ", ".join(
        f"target.`{column}` = source.`{column}`" for column in df.columns
    )
    insert_columns = ", ".join(f"`{column}`" for column in df.columns)
    insert_values = ", ".join(f"source.`{column}`" for column in df.columns)
    temp_view_name = f"_tmp_{target_table.replace('.', '_')}_{uuid4().hex}"

    df.createOrReplaceTempView(temp_view_name)
    try:
        spark.sql(
            f"""
            MERGE INTO {target_table} AS target
            USING {temp_view_name} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {assignment_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
        )
    finally:
        spark.catalog.dropTempView(temp_view_name)


def _ensure_standard_columns(df):
    if "processed_at" not in df.columns:
        return df.withColumn("processed_at", current_timestamp())

    return df


def _validate_mode_against_contract(mode: str, contract: dict) -> None:
    contract_mode = CONTRACT_WRITE_MODE_MAPPING.get(contract["table"]["write_mode"])
    if contract_mode != mode:
        raise ValueError(
            f"mode '{mode}' does not match contract write_mode '{contract['table']['write_mode']}'"
        )


def _validate_schema_against_contract(df, contract: dict) -> None:
    contract_columns = {
        column["name"]
        for column in contract["schema"]
        if isinstance(column, dict) and "name" in column
    }
    missing_columns = sorted(contract_columns.difference(df.columns))
    if missing_columns:
        raise ValueError(
            f"df is missing columns required by contract: {', '.join(missing_columns)}"
        )
