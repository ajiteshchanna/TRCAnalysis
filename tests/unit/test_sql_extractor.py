import pytest
from llm_sql import extract_sql

@pytest.mark.unit
def test_extract_sql_code_fence():
    raw = """```sql\nSELECT 1\n```"""
    assert extract_sql(raw) == "SELECT 1"

@pytest.mark.unit
def test_extract_sql_no_fence_plain():
    raw = "Here is the query:\nSELECT column FROM table"
    assert extract_sql(raw).startswith("SELECT column FROM table")

@pytest.mark.unit
def test_extract_sql_without_select():
    raw = "No SQL here"
    assert extract_sql(raw) == "No SQL here"
