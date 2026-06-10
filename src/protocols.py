from textwrap import dedent

from pathlib import Path

WATERMARK_COLUMN = "processed_at"

DEFAULT_CONTRACTS_DIR = Path("metadata/contracts")
DEFAULT_TASKS_DIR = Path("tasks")
DEFAULT_READ_METHOD = "FULL_LOAD"
DEFAULT_WRITE_METHOD = "OVERWRITE"


def contract_template(target_table: str) -> str:
    return dedent(
        f"""\
        table: {target_table}
        owner: engineering@acme.com

        sla:
          freshness: 1d

        write:
          method: {DEFAULT_WRITE_METHOD}

        schema:
          - column: id
            type: string
            nullable: false
            unique: true

          - column: processed_at
            type: timestamp
            nullable: false

          - column: value
            type: double
            nullable: false

        quality_rules:
          - name: positive_value
            expression: value > 0
            severity: error

          - name: contains_valid_values
            expression: column NOT IN ("INVALID")
            severity: warning
        """
    )


def task_template(
    source_table: str,
    target_table: str,
) -> str:
    return dedent(
        f"""\
        from src.reader import ReadConfig, read
        from src.writer import WriteConfig, write


        def run() -> None:
            read_config = ReadConfig(
                source_table="{source_table}",
                method="{DEFAULT_READ_METHOD}",
            )

            data = read(read_config)

            write_config = WriteConfig(
                target_table="{target_table}",
                method="{DEFAULT_WRITE_METHOD}",
                data=data,
                comments=None,
            )

            write(write_config)


        if __name__ == "__main__":
            run()
        """
    )
