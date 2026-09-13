import pytest
from llm_sql import validate_sql

@pytest.mark.unit
def test_validate_sql_safe_cases():
    safe_queries = [
        "SELECT * FROM vertical_wear_data LIMIT 10",
        "SELECT section_name, MAX(CAST(NULLIF(max_vertical_wear_mm,'') AS REAL)) FROM vertical_wear_data GROUP BY section_name",
        "WITH cte AS (SELECT * FROM sod_data) SELECT * FROM cte LIMIT 5",
        "SELECT COUNT(*) FROM sleeper_defects_data",
    ]
    for sql in safe_queries:
        ok, reason = validate_sql(sql)
        assert ok, f"Safe query rejected: {sql} – {reason}"

@pytest.mark.unit
def test_validate_sql_unsafe_cases():
    unsafe_queries = [
        ("INSERT INTO sod_data VALUES (1)", "INSERT"),
        ("UPDATE sod_data SET s_no='x'", "UPDATE"),
        ("DELETE FROM sod_data", "DELETE"),
        ("DROP TABLE sod_data", "DROP"),
        ("ALTER TABLE sod_data ADD COLUMN foo TEXT", "ALTER"),
        ("CREATE TABLE evil (x TEXT)", "CREATE"),
        ("PRAGMA table_info('sod_data')", "PRAGMA"),
        ("SELECT 1; DROP TABLE sod_data", "multi-statement"),
        ("SELECT * FROM sod_data -- comment", "line comment"),
        ("SELECT /* evil */ * FROM sod_data", "block comment"),
        ("", "empty"),
        ("ATTACH DATABASE 'x' AS y", "ATTACH"),
        ("VACUUM", "VACUUM"),
    ]
    for sql, label in unsafe_queries:
        ok, reason = validate_sql(sql)
        assert not ok, f"Unsafe query accepted ({label}): {sql}"
