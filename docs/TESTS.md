# Tests

Unit tests for `coe_utils.data_transformations_utils.DeltaTransform`.

## Prerequisites

- **Python** >= 3.9
- **Java** 17 (required by PySpark)
- **pip packages**: `pyspark`, `pytest`, `pyyaml`

```bash
pip install pyspark pytest pyyaml
```

On macOS, if Java is not installed:

```bash
brew install openjdk@17
```

Make sure `JAVA_HOME` is set before running the tests:

```bash
export JAVA_HOME=$(brew --prefix openjdk@17)
```

## Running the tests

From the project root:

```bash
export JAVA_HOME=$(brew --prefix openjdk@17)
python -m pytest tests/ -v
```

## Project structure

```
tests/
├── README.md
├── conftest.py              # SparkSession fixture + try_to_date polyfill
└── test_convert_columns.py  # Tests for convert_date_columns / convert_timestamp_columns
```

### `conftest.py`

- Provides a session-scoped local `SparkSession` fixture shared across all tests.
- Polyfills `try_to_date` (Databricks-only function not present in open-source PySpark) using `try_to_timestamp` + `.cast("date")`.

### `test_convert_columns.py`

Stubs Databricks-only transitive imports (`pyspark.dbutils`, `data_quality`, etc.) so `DeltaTransform` can be imported locally without a Databricks runtime.

Covers `convert_date_columns` (12 tests) and `convert_timestamp_columns` (14 tests).

## Notes

- The tests run against a **local PySpark** session (no cluster required).
- `try_to_date` is only available on Databricks runtimes. The `conftest.py` polyfill replicates its null-on-failure behaviour using `try_to_timestamp(...).cast("date")`.
- Databricks-only imports (`pyspark.dbutils`, `data_quality.*`, etc.) are stubbed with `unittest.mock.MagicMock` so the test module can load without a Databricks environment.
