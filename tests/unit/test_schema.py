import pytest
from llm_sql import get_database_schema, invalidate_schema_cache

@pytest.mark.unit
def test_schema_contains_expected_tables(tmp_db_path):
    # First call reads schema and caches it
    schema1 = get_database_schema(str(tmp_db_path))
    for tbl in [
        "vertical_wear_data",
        "lateral_wear_data",
        "sleeper_defects_data",
        "sod_data",
        "fittings_data",
        "rail_defects_data",
        "ballast_vegetation_data",
        "lip_flow_data",
    ]:
        assert f"TABLE: {tbl}" in schema1
    # Column hint example
    assert "max_vertical_wear_mm" in schema1
    # Cache hit
    schema2 = get_database_schema(str(tmp_db_path))
    assert schema1 is schema2
    # Invalidate and ensure new object
    invalidate_schema_cache()
    schema3 = get_database_schema(str(tmp_db_path))
    assert schema3 is not schema2
