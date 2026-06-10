from argparse import Namespace

import pytest

from src import cli


def test_parse_table_name_accepts_schema_and_table():
    assert cli._parse_table_name("silver.users") == ("silver", "users")


@pytest.mark.parametrize(
    "table_name",
    [
        "users",
        "silver.",
        ".users",
        "catalog.silver.users",
    ],
)
def test_parse_table_name_rejects_invalid_format(table_name):
    with pytest.raises(ValueError, match="Table name must use the format"):
        cli._parse_table_name(table_name)


def test_safe_python_name_normalizes_table_name():
    assert cli._safe_python_name("Users-2024") == "users_2024"
    assert cli._safe_python_name("123 users") == "_123_users"


def test_safe_python_name_rejects_empty_result():
    with pytest.raises(ValueError, match="Cannot create a safe Python file name"):
        cli._safe_python_name("!!!")


def test_contract_template_uses_target_table_and_default_write_method():
    content = cli._contract_template(target_table="silver.users")

    assert "table: silver.users" in content
    assert "owner: engineering@acme.com" in content
    assert "method: OVERWRITE" in content
    assert "quality_rules:" in content


def test_task_template_uses_default_read_and_write_configs():
    content = cli._task_template(
        source_table="raw.users",
        target_table="silver.users",
    )

    assert 'source_table="raw.users"' in content
    assert 'method="FULL_LOAD"' in content
    assert 'target_table="silver.users"' in content
    assert 'method="OVERWRITE"' in content
    assert "write(write_config)" in content


def test_write_file_creates_parent_directory(tmp_path):
    path = tmp_path / "metadata" / "contracts" / "silver" / "users.yml"

    cli._write_file(path, "content", force=False)

    assert path.read_text(encoding="utf-8") == "content"


def test_write_file_rejects_existing_file_without_force(tmp_path):
    path = tmp_path / "users.yml"
    path.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="File already exists"):
        cli._write_file(path, "new", force=False)

    assert path.read_text(encoding="utf-8") == "existing"


def test_write_file_overwrites_existing_file_with_force(tmp_path):
    path = tmp_path / "users.yml"
    path.write_text("existing", encoding="utf-8")

    cli._write_file(path, "new", force=True)

    assert path.read_text(encoding="utf-8") == "new"


def test_init_command_creates_contract_and_task(tmp_path):
    args = Namespace(
        target_table="silver.users",
        source_table=None,
        contracts_dir=tmp_path / "metadata" / "contracts",
        tasks_dir=tmp_path / "tasks",
        force=False,
    )

    created_paths = cli.init_command(args)

    contract_path = tmp_path / "metadata" / "contracts" / "silver" / "users.yml"
    task_path = tmp_path / "tasks" / "silver" / "users.py"

    assert created_paths == [contract_path, task_path]
    assert "table: silver.users" in contract_path.read_text(encoding="utf-8")
    assert 'source_table="raw.users"' in task_path.read_text(encoding="utf-8")
    assert 'method="FULL_LOAD"' in task_path.read_text(encoding="utf-8")
    assert 'method="OVERWRITE"' in task_path.read_text(encoding="utf-8")


def test_init_command_uses_custom_source_table_and_safe_task_name(tmp_path):
    args = Namespace(
        target_table="silver.users-2024",
        source_table="raw.crm_users",
        contracts_dir=tmp_path / "contracts",
        tasks_dir=tmp_path / "tasks",
        force=False,
    )

    cli.init_command(args)

    task_path = tmp_path / "tasks" / "silver" / "users_2024.py"
    task_content = task_path.read_text(encoding="utf-8")

    assert 'source_table="raw.crm_users"' in task_content
    assert 'method="FULL_LOAD"' in task_content
    assert 'method="OVERWRITE"' in task_content


def test_build_parser_parses_init_defaults():
    parser = cli.build_parser()

    args = parser.parse_args(["init", "silver.users"])

    assert args.command == "init"
    assert args.target_table == "silver.users"
    assert args.source_table is None
    assert args.contracts_dir == cli.DEFAULT_CONTRACTS_DIR
    assert args.tasks_dir == cli.DEFAULT_TASKS_DIR
    assert args.force is False
    assert args.func == cli.init_command


def test_main_creates_files_and_prints_paths(tmp_path, capsys):
    result = cli.main(
        [
            "init",
            "silver.users",
            "--contracts-dir",
            str(tmp_path / "contracts"),
            "--tasks-dir",
            str(tmp_path / "tasks"),
        ]
    )

    assert result == 0
    output = capsys.readouterr().out
    assert f"created {tmp_path / 'contracts' / 'silver' / 'users.yml'}" in output
    assert f"created {tmp_path / 'tasks' / 'silver' / 'users.py'}" in output


def test_main_exits_for_invalid_table_name():
    with pytest.raises(SystemExit) as error:
        cli.main(["init", "users"])

    assert error.value.code == 2


def test_main_exits_when_file_exists(tmp_path):
    contracts_dir = tmp_path / "contracts"
    contract_path = contracts_dir / "silver" / "users.yml"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text("existing", encoding="utf-8")

    with pytest.raises(SystemExit) as error:
        cli.main(
            [
                "init",
                "silver.users",
                "--contracts-dir",
                str(contracts_dir),
                "--tasks-dir",
                str(tmp_path / "tasks"),
            ]
        )

    assert error.value.code == 2
