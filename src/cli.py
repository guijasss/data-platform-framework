from argparse import ArgumentParser, Namespace
from pathlib import Path
from re import sub
from textwrap import dedent
from typing import Sequence


DEFAULT_CONTRACTS_DIR = Path("metadata/contracts")
DEFAULT_TASKS_DIR = Path("tasks")
DEFAULT_READ_METHOD = "FULL_LOAD"
DEFAULT_WRITE_METHOD = "OVERWRITE"


def _parse_table_name(table_name: str) -> tuple[str, str]:
    parts = table_name.split(".")

    if len(parts) != 2 or not all(parts):
        raise ValueError("Table name must use the format <schema>.<table>")

    return parts[0], parts[1]


def _safe_python_name(name: str) -> str:
    safe_name = sub(r"\W+", "_", name).strip("_").lower()

    if not safe_name:
        raise ValueError(f"Cannot create a safe Python file name from: {name}")

    if safe_name[0].isdigit():
        safe_name = f"_{safe_name}"

    return safe_name


def _contract_template(target_table: str) -> str:
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


def _task_template(
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


def _write_file(path: Path, content: str, force: bool) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"File already exists: {path}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def init_command(args: Namespace) -> list[Path]:
    schema, table = _parse_table_name(args.target_table)
    safe_table = _safe_python_name(table)
    source_table = args.source_table or f"raw.{table}"

    contract_path = args.contracts_dir / schema / f"{table}.yml"
    task_path = args.tasks_dir / schema / f"{safe_table}.py"

    _write_file(
        contract_path,
        _contract_template(target_table=args.target_table),
        force=args.force,
    )
    _write_file(
        task_path,
        _task_template(
            source_table=source_table,
            target_table=args.target_table,
        ),
        force=args.force,
    )

    return [contract_path, task_path]


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="fw",
        description="Data platform framework CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser(
        "init",
        help="Create a data contract and starter task.",
    )
    init_parser.add_argument(
        "target_table",
        help="Target table using the format <schema>.<table>.",
    )
    init_parser.add_argument(
        "--source-table",
        help="Source table for the generated read config. Defaults to raw.<table>.",
    )
    init_parser.add_argument(
        "--contracts-dir",
        default=DEFAULT_CONTRACTS_DIR,
        type=Path,
        help="Directory where contracts are created.",
    )
    init_parser.add_argument(
        "--tasks-dir",
        default=DEFAULT_TASKS_DIR,
        type=Path,
        help="Directory where task files are created.",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing generated files.",
    )
    init_parser.set_defaults(func=init_command)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        created_paths = args.func(args)
    except (FileExistsError, ValueError) as error:
        parser.error(str(error))

    for path in created_paths:
        print(f"created {path}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
