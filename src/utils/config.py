from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


WATERMARK_COLUMN = "processed_at"
CONFIG_PATH = Path(__file__).resolve().parents[2] / "config.yml"


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


@dataclass(frozen=True)
class FrameworkConfig:
    check_updates: bool = False


def load_config() -> FrameworkConfig:
    if not CONFIG_PATH.exists():
        return FrameworkConfig()

    config = _load_yaml_config(CONFIG_PATH)
    reader_config = config.get("reader", {})
    if not isinstance(reader_config, dict):
        return FrameworkConfig()

    return FrameworkConfig(
        check_updates=bool(reader_config.get("check_updates", False))
    )


def _load_yaml_config(config_path: str | Path) -> dict[str, Any]:
    with Path(config_path).open("r", encoding="utf-8") as config_file:
        raw_text = config_file.read()

    if yaml is not None:
        parsed_config = yaml.safe_load(raw_text)
        if isinstance(parsed_config, dict):
            return parsed_config

    return _parse_simple_yaml(raw_text)


def _parse_simple_yaml(raw_text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for raw_line in raw_text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        key, _, raw_value = raw_line.strip().partition(":")
        value = raw_value.strip()

        while indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]
        if not value:
            parent[key] = {}
            stack.append((indent, parent[key]))
            continue

        parent[key] = _parse_yaml_scalar(value)

    return root


def _parse_yaml_scalar(value: str) -> Any:
    normalized = value.lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    if normalized in {"null", "~"}:
        return None

    return value.strip("'\"")
