# tests/integration/test_reader_integration.py

import pytest

from src.protocols import WATERMARK_COLUMN
from src.reader import ReadConfig, read


@pytest.mark.integration
def test_read_incremental_reads_only_new_rows(database):
    config = ReadConfig(
        source_table="raw_test.users",
        target_table="silver_test.users",
        method="INCREMENTAL",
        columns=[
            "id",
            "name",
            WATERMARK_COLUMN,
        ],
    )

    result = read(config).collect()

    assert result["id"].to_list() == [2, 3]

    assert result["name"].to_list() == [
        "Bob",
        "Carol",
    ]


@pytest.mark.integration
def test_read_falls_back_to_full_load_when_target_table_does_not_exist(database):
    config = ReadConfig(
        source_table="raw_test.users",
        target_table="silver_test.missing_users",
        method="INCREMENTAL",
        columns=[
            "id",
            "name",
        ],
    )

    result = read(config).collect()

    assert result["id"].to_list() == [1, 2, 3]

    assert result["name"].to_list() == [
        "Alice",
        "Bob",
        "Carol",
    ]


@pytest.mark.integration
def test_read_raises_when_source_table_does_not_exist(database):
    not_exist_table = "raw_test.missing_users"

    config = ReadConfig(
        source_table=not_exist_table,
        target_table="silver_test.users",
        method="FULL_LOAD",
        columns=["id"],
    )

    with pytest.raises(
        ValueError,
        match=f"Table {not_exist_table} doesn't exist!",
    ):
        read(config)
