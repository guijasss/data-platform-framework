from datetime import date, datetime, time, timedelta

import polars as pl
import pytest

from src.database.helpers import (
    generate_create_table_sql,
    quote_identifier,
    quote_literal,
    quote_table_name,
)


def test_quote_identifier_escapes_double_quotes():
    assert quote_identifier('bad"name') == '"bad""name"'


def test_quote_table_name_quotes_each_part():
    assert quote_table_name('raw.users"2024') == '"raw"."users""2024"'


def test_quote_literal_escapes_single_quotes():
    assert quote_literal("owner's data") == "'owner''s data'"


def test_generate_create_table_sql_maps_types_and_comments():
    data = pl.DataFrame(
        {
            "int8_col": pl.Series([1], dtype=pl.Int8),
            "int16_col": pl.Series([1], dtype=pl.Int16),
            "int32_col": pl.Series([1], dtype=pl.Int32),
            "int64_col": pl.Series([1], dtype=pl.Int64),
            "uint8_col": pl.Series([1], dtype=pl.UInt8),
            "uint16_col": pl.Series([1], dtype=pl.UInt16),
            "uint32_col": pl.Series([1], dtype=pl.UInt32),
            "uint64_col": pl.Series([1], dtype=pl.UInt64),
            "float32_col": pl.Series([1.5], dtype=pl.Float32),
            "float64_col": pl.Series([1.5], dtype=pl.Float64),
            "bool_col": pl.Series([True], dtype=pl.Boolean),
            "string_col": pl.Series(["Alice"], dtype=pl.String),
            "date_col": pl.Series([date(2024, 1, 1)], dtype=pl.Date),
            "time_col": pl.Series([time(10, 30)], dtype=pl.Time),
            "datetime_col": pl.Series([datetime(2024, 1, 1)], dtype=pl.Datetime),
            "duration_col": pl.Series([timedelta(days=1)], dtype=pl.Duration),
            "binary_col": pl.Series([b"abc"], dtype=pl.Binary),
            "null_col": pl.Series([None], dtype=pl.Null),
        }
    ).lazy()

    sql = generate_create_table_sql(
        data=data,
        table_name='raw.users"',
        column_comments={"string_col": "owner's name"},
    )

    assert 'CREATE TABLE "raw"."users""" (' in sql
    assert '"int8_col" SMALLINT' in sql
    assert '"int16_col" SMALLINT' in sql
    assert '"int32_col" INTEGER' in sql
    assert '"int64_col" BIGINT' in sql
    assert '"uint8_col" SMALLINT' in sql
    assert '"uint16_col" INTEGER' in sql
    assert '"uint32_col" BIGINT' in sql
    assert '"uint64_col" NUMERIC(20, 0)' in sql
    assert '"float32_col" REAL' in sql
    assert '"float64_col" DOUBLE PRECISION' in sql
    assert '"bool_col" BOOLEAN' in sql
    assert '"string_col" TEXT' in sql
    assert '"date_col" DATE' in sql
    assert '"time_col" TIME' in sql
    assert '"datetime_col" TIMESTAMP' in sql
    assert '"duration_col" INTERVAL' in sql
    assert '"binary_col" BYTEA' in sql
    assert '"null_col" TEXT' in sql
    assert (
        'COMMENT ON COLUMN "raw"."users"""."string_col" IS '
        "'owner''s name';"
    ) in sql


def test_generate_create_table_sql_raises_for_unknown_comment_column():
    data = pl.DataFrame({"id": [1]}).lazy()

    with pytest.raises(ValueError, match="Coluna não existe no LazyFrame: missing"):
        generate_create_table_sql(
            data=data,
            table_name="raw.users",
            column_comments={"missing": "not present"},
        )
