from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import pytest

from src.data_quality import validate_data_quality


def test_should_not_raise_when_all_rows_match_quality_rules(spark) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "a@example.com"), (2, "b@example.com")],
        schema=StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("email", StringType(), nullable=True),
            ]
        ),
    )
    contract = {
        "quality": {
            "rules": [
                "WHERE id IS NOT NULL",
                "WHERE email IS NOT NULL",
            ]
        }
    }

    # Act / Assert
    validate_data_quality(df, contract)


def test_should_raise_when_any_quality_rule_is_not_matched(spark) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1, "a@example.com"), (2, None)],
        schema=StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("email", StringType(), nullable=True),
            ]
        ),
    )
    contract = {
        "quality": {
            "rules": [
                "WHERE email IS NOT NULL",
            ]
        }
    }

    # Act / Assert
    with pytest.raises(
        ValueError,
        match="data quality rule failed: 'WHERE email IS NOT NULL' matched 1 invalid rows",
    ):
        validate_data_quality(df, contract)


def test_should_accept_quality_rules_without_where_prefix(spark) -> None:
    # Arrange
    df = spark.createDataFrame(
        [(1,), (2,)],
        schema=StructType(
            [
                StructField("id", IntegerType(), nullable=False),
            ]
        ),
    )
    contract = {
        "quality": {
            "rules": [
                "id IS NOT NULL",
            ]
        }
    }

    # Act / Assert
    validate_data_quality(df, contract)


def test_should_raise_when_quality_section_is_missing(spark) -> None:
    # Arrange
    df = spark.createDataFrame([(1,)], ["id"])

    # Act / Assert
    with pytest.raises(ValueError, match="data contract is missing quality section"):
        validate_data_quality(df, {})


def test_should_raise_when_quality_rule_is_empty(spark) -> None:
    # Arrange
    df = spark.createDataFrame([(1,)], ["id"])
    contract = {
        "quality": {
            "rules": [""],
        }
    }

    # Act / Assert
    with pytest.raises(
        ValueError,
        match="data contract quality rules must be non-empty strings",
    ):
        validate_data_quality(df, contract)
