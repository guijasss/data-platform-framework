# Tests

This document describes how to run tests for this repository with the current setup.

At the moment, the project uses `pytest` and a local virtual environment under `venv/`.

## Current Test Layout

Tests live under the `tests/` directory.

Current examples:

* `tests/test_reader.py`
* `tests/test_writer.py`
* `tests/integration/test_reader_integration.py`
* `tests/integration/test_writer_integration.py`

Production code currently under test lives in `src/`.

Shared fixtures live in `tests/conftest.py`.

## Prerequisites

Use the repository-local Python environment:

```bash
venv/bin/python --version
```

If the environment does not exist yet, create and populate it before running tests.

## Run All Tests

Run the full test suite with:

```bash
venv/bin/python -m pytest
```

What this does:

* `-m pytest`: runs the `pytest` test runner
* test discovery is controlled by `pytest.ini`

## Run One Test File

To run a single test module:

```bash
venv/bin/python -m pytest tests/test_reader.py
```

Another example:

```bash
venv/bin/python -m pytest tests/test_writer.py
```

## Run One Test Case

To run one specific test function:

```bash
venv/bin/python -m pytest tests/test_reader.py::test_reads_delta_table_from_spark
```

To run all tests in the integration folder:

```bash
venv/bin/python -m pytest tests/integration
```

To run only tests marked as integration:

```bash
venv/bin/python -m pytest -m integration
```

To skip integration tests:

```bash
venv/bin/python -m pytest -m "not integration"
```

For more verbose output:

```bash
venv/bin/python -m pytest -v
```

## Coverage

Code coverage is not installed in the local `venv` by default at the time of writing.

Install it with:

```bash
venv/bin/python -m pip install coverage
```

After installation, run the test suite with coverage collection enabled:

```bash
venv/bin/python -m coverage run --source=src -m pytest
```

Then print a terminal report:

```bash
venv/bin/python -m coverage report -m
```

The `--source=src` option limits coverage measurement to the project source code.

## HTML Coverage Report

Generate an HTML report with:

```bash
venv/bin/python -m coverage html
```

This creates the report under:

```text
htmlcov/index.html
```

## Typical TDD Loop

For a small TDD cycle in this repository:

1. Add or update a failing test in `tests/`.
2. Run the relevant test module.
3. Implement the minimum code change in `src/`.
4. Run the full suite.
5. Optionally run coverage to see untested paths.

Example:

```bash
venv/bin/python -m pytest tests/test_writer.py
venv/bin/python -m pytest -m "not integration"
venv/bin/python -m coverage run --source=src -m pytest
venv/bin/python -m coverage report -m
```

## Spark Integration Test Fixtures

Integration tests that need a real Spark runtime are defined with `pytest` fixtures in `tests/conftest.py`.

Available fixtures:

* `spark_session`: creates a local Spark session for generic Spark integration tests
* `delta_spark_session`: creates a Spark session configured for Delta Lake
* `configured_spark`: sets `src.utils.config.SPARK` to the shared Spark fixture
* `configured_delta_spark`: sets `src.utils.config.SPARK` to the shared Delta-enabled fixture

Those fixtures centralize setup and teardown so integration tests do not create sessions ad hoc.

Example:

```python
@pytest.mark.integration
def test_example(configured_delta_spark):
    spark = configured_delta_spark
    df = spark.createDataFrame([("a",)], ["id"])
    assert df.count() == 1
```

## Integration Test Requirements

Unit tests run with mocks and should work without Spark.

Integration tests have stricter runtime requirements:

* Java must be available in `PATH`
* the `delta-spark` package must be installed for Delta integration tests

If those requirements are missing, the integration fixtures skip the affected tests instead of failing the whole suite.

## Troubleshooting

If `No module named coverage` appears:

* install coverage with `venv/bin/python -m pip install coverage`

If `No module named pytest` appears:

* install pytest with `venv/bin/python -m pip install pytest`

If test discovery returns zero tests:

* confirm files are inside `tests/`
* confirm filenames start with `test_`
* confirm test functions start with `test_`

If imports from `src` fail:

* make sure you are running commands from the repository root
* use `venv/bin/python`, not the system Python

If integration tests are skipped:

* check whether Java is installed and available through `java -version`
* install `delta-spark` if you want to run Delta-backed integration tests

## Next Improvements

Useful follow-up improvements for the project:

* add `pytest`, `coverage`, and `delta-spark` to a dependency file
* add a `.coveragerc` file
* add a simple `Makefile` or task runner command for tests
* add dataframe comparison helpers for pipeline tests
