# Code Review: `src/` Database Coupling and Unit-Testability

## Scope

Reviewed modules under `src/`:

- `src/reader.py`
- `src/writer.py`
- `src/database/connection.py`
- `src/database/helpers.py`
- `src/helpers.py`
- `src/protocols.py`

This review focuses on coupling to the database, how that affects unit testing, and the highest-risk design issues that follow from it.

## Findings

### 1. High: Public read/write flows are hard-wired to live database infrastructure

`src/reader.py` imports and directly calls `get_table_watermark`, `read_table`, and `table_exists` from `src.database.connection` ([reader.py](/home/guijas/gui/dev/data-platform-framework/src/reader.py:7), [reader.py](/home/guijas/gui/dev/data-platform-framework/src/reader.py:45), [reader.py](/home/guijas/gui/dev/data-platform-framework/src/reader.py:72)).

`src/writer.py` does the same with `execute_sql`, `make_engine`, and `table_exists` ([writer.py](/home/guijas/gui/dev/data-platform-framework/src/writer.py:6), [writer.py](/home/guijas/gui/dev/data-platform-framework/src/writer.py:41), [writer.py](/home/guijas/gui/dev/data-platform-framework/src/writer.py:62)).

Impact:

- The application layer cannot be exercised without patching module-level functions or talking to a real database.
- Unit tests cannot verify decision logic in isolation, because the business flow and persistence flow are the same function.
- The current API shape encourages integration tests as the only realistic safety net.

Why this matters:

- `read()` should mostly be decision logic: validate config, choose full load vs incremental, derive filters.
- `write()` should mostly be orchestration logic: validate config, decide create-vs-write strategy, delegate persistence.
- In the current design, both functions immediately cross the database boundary.

### 2. High: Database connection construction is global, implicit, and repeated

`src/database/connection.py` builds connections from environment variables inside low-level functions ([connection.py](/home/guijas/gui/dev/data-platform-framework/src/database/connection.py:16), [connection.py](/home/guijas/gui/dev/data-platform-framework/src/database/connection.py:42)).

Impact:

- Any test that touches `table_exists`, `execute_sql`, `read_table`, or `make_engine` needs environment variables and usually a reachable Postgres instance.
- Configuration is not injectable, so test doubles cannot be supplied cleanly.
- Connection creation happens repeatedly instead of being owned by one adapter object, which makes lifecycle and mocking harder.

This is a strong source of hidden coupling: callers appear to depend on simple functions, but they actually depend on process environment, SQLAlchemy, psycopg, ConnectorX, and a running database.

### 3. High: Incremental-read business rules are encoded as SQL strings

`_read_incremental()` derives the watermark and then builds a raw SQL predicate string inline ([reader.py](/home/guijas/gui/dev/data-platform-framework/src/reader.py:45), [reader.py](/home/guijas/gui/dev/data-platform-framework/src/reader.py:51)).

`read_table()` then concatenates selected columns, table names, and `where` conditions directly into SQL text ([connection.py](/home/guijas/gui/dev/data-platform-framework/src/database/connection.py:72), [connection.py](/home/guijas/gui/dev/data-platform-framework/src/database/connection.py:83), [connection.py](/home/guijas/gui/dev/data-platform-framework/src/database/connection.py:88)).

Impact:

- The domain rule "load rows newer than watermark" is inseparable from SQL syntax generation.
- Unit tests must inspect generated SQL strings or hit a database to validate behavior.
- The code exposes SQL injection and quoting risks for table names, column names, and conditions.

Even if inputs are currently trusted, the coupling problem remains: the application layer is reasoning in SQL text instead of domain concepts.

### 4. Medium: Schema-management behavior is embedded in the writer orchestration path

`write()` checks table existence, generates `CREATE TABLE` SQL, prints user-facing messages, executes DDL, and then dispatches to the write method ([writer.py](/home/guijas/gui/dev/data-platform-framework/src/writer.py:53), [writer.py](/home/guijas/gui/dev/data-platform-framework/src/writer.py:62), [writer.py](/home/guijas/gui/dev/data-platform-framework/src/writer.py:68)).

Impact:

- A single function owns validation, schema provisioning, logging/output, and persistence.
- There is no clean seam to test "what should happen if the table is missing?" without also dealing with SQL generation and execution.
- The writer is coupled not just to data storage, but also to provisioning policy.

This is a classic sign that orchestration and infrastructure concerns are mixed in one place.

### 5. Medium: The only implemented tests are integration tests against a real database

The suite under `tests/` contains only integration tests for `read()` and uses real schemas/tables via SQLAlchemy setup/teardown ([test_reader.py](/home/guijas/gui/dev/data-platform-framework/tests/integration/test_reader.py:1), [conftest.py](/home/guijas/gui/dev/data-platform-framework/tests/integration/conftest.py:13)).

Observed locally:

- `pytest -q` fails because the suite expects a reachable Postgres instance on `localhost:5432`.

Impact:

- The test strategy mirrors the code structure: since there are no seams, tests validate behavior through the database.
- Feedback is slower, setup is heavier, and failure diagnosis is noisier.
- Simple branching logic in `read()` and `write()` is not protected by fast unit tests.

This is more symptom than root cause, but it confirms the architectural issue.

### 6. Medium: `reader.py` exposes configuration fields that the implementation does not really honor

`ReadConfig` declares `source_watermark_column`, but incremental reads ignore it and instead hard-code `WATERMARK_COLUMN` ([reader.py](/home/guijas/gui/dev/data-platform-framework/src/reader.py:23), [reader.py](/home/guijas/gui/dev/data-platform-framework/src/reader.py:51), [protocols.py](/home/guijas/gui/dev/data-platform-framework/src/protocols.py:1)).

Impact:

- The public configuration suggests flexibility that does not exist.
- Tests cannot express alternate watermark behavior without changing internals.
- This increases coupling to one physical schema convention.

This is not just a correctness concern; it is a design smell that domain policy and storage assumptions are fused together.

### 7. Medium: `writer.py` has no meaningful validation seam and partial implementations

`_run_validations()` is empty, while `_write_append()` and `_write_merge()` are stubs ([writer.py](/home/guijas/gui/dev/data-platform-framework/src/writer.py:26), [writer.py](/home/guijas/gui/dev/data-platform-framework/src/writer.py:37)).

Impact:

- The public API suggests multiple write modes, but only one exists.
- As more behavior gets added, it is likely to be added directly against the database layer because no abstraction boundary exists yet.
- This raises the risk that coupling gets worse before it gets better.

### 8. Low: Useful pure logic already exists, but it is isolated in helper modules rather than shaping the architecture

`generate_create_table_sql()` and `validate_required_fields()` are examples of logic that can be tested without infrastructure ([helpers.py](/home/guijas/gui/dev/data-platform-framework/src/helpers.py:12), [database/helpers.py](/home/guijas/gui/dev/data-platform-framework/src/database/helpers.py:44)).

Impact:

- The project already has pieces that could support a more testable design.
- The issue is not lack of pure functions; it is that the main entry points do not depend on them through stable boundaries.

## Overall Assessment

Your concern is accurate: the current architecture is strongly coupled to the database.

The main problem is not merely "it uses SQLAlchemy" or "it talks to Postgres." The deeper issue is that application decisions, storage rules, connection setup, and SQL construction all happen inside the same call paths. That removes the seam where unit tests normally attach.

In practical terms:

- `read()` is not a pure use-case function; it is an integration workflow.
- `write()` is not a pure orchestration function; it is also a schema manager and persistence adapter.
- `src.database.connection` is not an adapter behind an interface; it is a globally imported implementation detail that leaks everywhere.

## What To Refactor First

### 1. Introduce repository/gateway interfaces at the application boundary

Example split:

- `ReaderGateway`
  - `source_exists(table_name) -> bool`
  - `target_exists(table_name) -> bool`
  - `get_watermark(table_name, column_name) -> datetime | None`
  - `read_since(table_name, columns, watermark_column, watermark) -> LazyFrame`
  - `read_all(table_name, columns) -> LazyFrame`
- `WriterGateway`
  - `table_exists(table_name) -> bool`
  - `create_table_from_frame(table_name, data, comments) -> None`
  - `overwrite(table_name, data) -> None`
  - `append(table_name, data) -> None`
  - `merge(table_name, data, keys...) -> None`

Then make `read()` and `write()` depend on those interfaces rather than importing `src.database.connection` directly.

This is the single highest-value change for unit testability.

### 2. Move environment-based connection construction into one adapter object

Instead of global free functions that call `getenv()` transitively, create one concrete adapter, for example:

- `PostgresReaderGateway(config)`
- `PostgresWriterGateway(config)`
- or one `PostgresDatabase(config)`

That lets tests pass a fake implementation without patching module globals.

### 3. Separate decision logic from SQL generation

Refactor incremental read selection into a pure function that returns a read plan, for example:

- input: `ReadConfig`, `source_exists`, `target_exists`, `watermark`
- output: `FullLoadPlan(...)` or `IncrementalPlan(...)`

Then test those plan decisions with plain unit tests.

The SQL adapter can translate the plan into actual queries.

### 4. Stop passing raw SQL fragments across layers

Current `where: Sequence[str]` is an infrastructure leak. The application layer should not build SQL snippets like `"processed_at > '...'"`.

Better options:

- pass structured filters, or
- expose higher-level gateway methods such as `read_since(...)`.

### 5. Remove misleading configuration or make it real

Either:

- honor `source_watermark_column`, or
- remove it from `ReadConfig` until supported.

Exposed configuration should represent actual behavior.

## Suggested Unit-Test Surface After Refactor

These tests should become fast and database-free:

- `read()` returns full load when target table is missing.
- `read()` raises when source table is missing.
- `read()` requests incremental load with the expected watermark and column list.
- `write()` creates a table before writing when the target is missing.
- `write()` dispatches to overwrite/append/merge correctly.
- validation errors for invalid methods or missing required fields.

Only the adapter implementation should need integration tests against Postgres.

## Residual Notes

- `src/database/helpers.py` is relatively close to unit-testable already and is a good candidate to keep as pure infrastructure-support code.
- `src/helpers.py` is also already test-friendly.
- The highest leverage is not adding more mocks to the current design; it is introducing a boundary so mocks/fakes are attached to a coherent interface rather than to module-level functions.

## Bottom Line

Yes, the code is highly coupled to the database, and the coupling is architectural rather than incidental.

The clearest fix is:

1. move database access behind explicit gateway/repository interfaces,
2. inject those dependencies into `read()` and `write()`,
3. keep SQL/text/engine concerns inside the Postgres adapter,
4. unit-test the orchestration layer separately from the adapter layer.

Without that split, new behavior will continue to default toward integration-style testing and the current tight coupling will keep growing.
