#!/usr/bin/env bash
set -Eeuo pipefail

cleanup() {
  docker compose --profile test stop postgres_test
  docker compose --profile test rm -f postgres_test
}

trap cleanup EXIT

docker compose --profile test up -d --wait postgres_test
uv run pytest tests/integration -m integration
