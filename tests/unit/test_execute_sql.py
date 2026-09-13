import pytest
from llm_sql import execute_sql

@pytest.mark.unit
def test_execute_sql_read_only(tmp_db_path):
    # Simple SELECT should succeed and return columns and rows
    sql = "SELECT COUNT(*) FROM vertical_wear_data"
    cols, rows = execute_sql(sql, str(tmp_db_path))
    # Column name may vary based on SQLite version
    assert cols and isinstance(cols, list)
    assert isinstance(rows, list)
    assert len(rows) == 1
    # Ensure row value is an integer count
    assert isinstance(rows[0][0], int)

@pytest.mark.unit
def test_execute_sql_write_blocked(tmp_db_path):
    # Attempt to execute a write statement should raise ValueError
    with pytest.raises(ValueError):
        execute_sql("INSERT INTO sod_data VALUES (NULL)", str(tmp_db_path))
