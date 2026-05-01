from __future__ import annotations

from typing import Any


def validate_data_quality(df, contract: dict[str, Any]) -> None:
    quality_section = contract.get("quality")
    if not isinstance(quality_section, dict):
        raise ValueError("data contract is missing quality section")

    quality_rules = quality_section.get("rules")
    if not isinstance(quality_rules, list):
        raise ValueError("data contract quality rules must be a list")

    for quality_rule in quality_rules:
        if not isinstance(quality_rule, str) or not quality_rule.strip():
            raise ValueError("data contract quality rules must be non-empty strings")

        predicate = _normalize_quality_rule(quality_rule)
        failed_rows = df.where(f"NOT ({predicate})").count()
        if failed_rows > 0:
            raise ValueError(
                f"data quality rule failed: '{quality_rule}' matched {failed_rows} invalid rows"
            )


def _normalize_quality_rule(quality_rule: str) -> str:
    normalized_rule = quality_rule.strip()
    if normalized_rule.upper().startswith("WHERE "):
        return normalized_rule[6:].strip()

    return normalized_rule
