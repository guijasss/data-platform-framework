# Project Context

This is a Python project. Follow the existing project structure, naming conventions, and implementation patterns before introducing new ones.

Prefer simple, readable, maintainable code over clever abstractions.

# General Development Guidelines

- Write clear, idiomatic Python.
- Follow PEP 8 and project linting rules.
- Prefer explicit code over implicit behavior.
- Keep functions small and focused.
- Avoid unnecessary abstractions.
- Do not introduce new dependencies unless clearly justified.
- Reuse existing utilities, helpers, services, and patterns when available.
- Preserve backwards compatibility unless explicitly asked otherwise.
- Avoid large refactors unless they are required for the requested change.

# Code Style

- Use descriptive variable, function, class, and module names.
- Prefer type hints for public functions, class methods, and complex logic.
- Use `dataclasses`, `TypedDict`, `Protocol`, or `pydantic` only when they fit the existing project style.
- Prefer early returns to reduce nesting.
- Avoid broad `except Exception` blocks unless there is a clear reason.
- Do not swallow exceptions silently.
- Keep comments focused on why, not what.
- Do not leave TODOs unless explicitly requested.

# Architecture

- Respect the existing boundaries between modules and layers.
- Keep business logic separate from I/O, framework, database, and API concerns.
- Prefer dependency injection for external systems where practical.
- Avoid circular imports.
- Avoid global mutable state.
- Keep configuration outside business logic.
- Make code testable by design.

# Error Handling

- Raise specific exceptions when possible.
- Preserve useful error context.
- Validate inputs at system boundaries.
- Avoid returning `None` for error states unless that is the project convention.
- Log errors where they are handled, not where they are raised.

# Logging

- Use the project’s existing logging approach.
- Do not use `print()` for application logs.
- Log meaningful events and failures.
- Avoid logging sensitive data, secrets, tokens, credentials, or personal information.

# Testing Guidelines

- Add or update tests for every behavioral change.
- Follow existing test structure and naming conventions.
- Prefer unit tests for business logic.
- Add integration tests when behavior depends on external systems or framework wiring.
- Use mocks or fakes for external APIs, databases, queues, filesystems, and network calls.
- Tests should be deterministic and isolated.
- Do not rely on test execution order.
- Avoid real network calls in tests.
- Avoid real credentials, secrets, or production resources.

# Test Naming

Use descriptive test names such as:

```python
def test_should_return_user_when_id_exists():
    ...
```

Or follow the existing project convention.

# Test Structure

Prefer Arrange / Act / Assert:

```python
def test_should_calculate_total_with_discount():
    # Arrange
    ...

    # Act
    ...

    # Assert
    ...
```

# Python Tooling

Use the project’s existing tools. If present, prefer:

- `pytest` for tests
- `ruff` for linting and formatting
- `mypy` or `pyright` for type checking
- `black` only if already configured
- `poetry`, `uv`, or `pip-tools` according to the project setup

Do not add or change tooling without explicit approval.

# Dependencies

- Check existing dependencies before adding new ones.
- Prefer the standard library when sufficient.
- Avoid heavy dependencies for simple tasks.
- If a dependency is necessary, explain why.
- Keep dependency changes minimal.

# Security

- Never hardcode secrets, tokens, passwords, API keys, or credentials.
- Do not expose sensitive values in logs, tests, fixtures, or examples.
- Validate external inputs.
- Be careful with file paths, shell commands, SQL queries, and deserialization.
- Avoid unsafe functions such as `eval()` and unnecessary shell execution.

# Database and Persistence

- Follow existing migration and schema conventions.
- Keep migrations small and reversible when possible.
- Avoid destructive changes unless explicitly requested.
- Do not modify production data directly.
- Use parameterized queries or ORM-safe methods.
- Keep persistence logic separate from business rules.

# API and External Integrations

- Keep API clients isolated behind clear interfaces.
- Add timeouts to network calls when applicable.
- Handle retries carefully and avoid infinite retry loops.
- Mock external integrations in tests.
- Preserve existing request and response contracts.

# Documentation

- Update documentation when behavior, commands, configuration, or public APIs change.
- Keep docstrings useful and concise.
- Prefer examples for non-obvious behavior.
- Do not over-document obvious code.

# Before Making Changes

Before implementing:

- Inspect the existing code and tests.
- Identify the project conventions.
- Prefer the smallest safe change.
- Consider edge cases.
- Update or add tests.

# Before Finishing

Before considering the task complete:

- Run or suggest the relevant tests.
- Run or suggest lint and type checks if configured.
- Ensure no debug code, temporary files, or unused imports remain.
- Summarize what changed and why.
- Mention any tests not run.

# Response Style

When responding:

- Be concise and practical.
- Explain important trade-offs.
- Mention assumptions.
- Point out risks or unclear requirements.
- Do not claim tests were run unless they were actually run.
