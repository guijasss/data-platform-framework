from src.protocols import contract_template, task_template


def test_contract_template_uses_target_table_and_default_write_method():
    content = contract_template(target_table="silver.users")

    assert "table: silver.users" in content
    assert "owner: engineering@acme.com" in content
    assert "method: OVERWRITE" in content
    assert "quality_rules:" in content


def test_task_template_uses_default_read_and_write_configs():
    content = task_template(
        source_table="raw.users",
        target_table="silver.users",
    )

    assert 'source_table="raw.users"' in content
    assert 'method="FULL_LOAD"' in content
    assert 'target_table="silver.users"' in content
    assert 'method="OVERWRITE"' in content
    assert "write(write_config)" in content
