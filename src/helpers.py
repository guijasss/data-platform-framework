from typing import Any, NoReturn, Sequence
from os import getenv


def get_env_or_raise(env_variable_name: str) -> str:
    value = getenv(env_variable_name)
    if not value:
        raise EnvironmentError(f"Environment variable {env_variable_name} not defined!")
    return value


def validate_required_fields(
    instance: Any,
    required_fields: Sequence[str],
) -> NoReturn:
    missing_fields = [
        field_name
        for field_name in required_fields
        if getattr(instance, field_name) is None
    ]

    if missing_fields:
        raise ValueError(
            f"Required fields are missing: {', '.join(missing_fields)}"
        )
