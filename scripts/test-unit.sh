#!/usr/bin/env bash
set -Eeuo pipefail

uv run pytest tests/unit \
  -m "not integration" \
  --cov=src \
  --cov-report=term-missing \
  --cov-fail-under=100
