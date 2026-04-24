VALID_WRITE_MODES = {"append", "overwrite"}


def write_delta_table(target_table: str, df=None, mode: str = "append") -> None:
    if not target_table or not target_table.strip():
        raise ValueError("target_table must be a non-empty string")

    if df is None:
        raise ValueError("df is required")

    normalized_mode = mode.lower()
    if normalized_mode not in VALID_WRITE_MODES:
        raise ValueError(
            f"mode must be one of: {', '.join(sorted(VALID_WRITE_MODES))}"
        )

    df.write.format("delta").mode(normalized_mode).saveAsTable(target_table)
