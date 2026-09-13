"""
test_llm_sql.py — Validation and integration tests for the NL-to-SQL feature
=============================================================================
Run with:
    python test_llm_sql.py

Tests:
  A. Basic sort query (highest vertical wear)
  B. Aggregation / GROUP BY (most broken sleepers per section)
  C. Aggregate average (average lateral wear)
  D. Unsafe SQL rejection (INSERT, DROP, PRAGMA, multi-statement)
  E. Empty question → user-friendly error
  F. Ollama-unavailable simulation (bad base_url)
  G. Empty result set (impossible filter → 0 rows)
  H. Schema cache invalidation
  I. /api/llm-status distinguishes server vs model availability
"""

import sys
import json
import time
import sqlite3
import urllib.request
import urllib.error

# Ensure project root is on path
sys.path.insert(0, ".")
import llm_sql

DB = "railway.db"
PASS = "\033[92m  PASS\033[0m"
FAIL = "\033[91m  FAIL\033[0m"
INFO = "\033[94m  INFO\033[0m"


def section(title: str) -> None:
    print("\n" + "-" * 60)
    print(f"  {title}")
    print("-" * 60)


def check(label: str, condition: bool, detail: str = "") -> bool:
    tag = PASS if condition else FAIL
    print(f"{tag}  {label}")
    if detail:
        print(f"       -> {detail}")
    return condition

# ---------------------------------------------------------------------
# UNIT: validate_sql()
# ---------------------------------------------------------------------
section("UNIT: validate_sql()")

safe_queries = [
    "SELECT * FROM vertical_wear_data LIMIT 10",
    "SELECT section_name, MAX(CAST(NULLIF(max_vertical_wear_mm,'') AS REAL)) FROM vertical_wear_data GROUP BY section_name",
    "WITH cte AS (SELECT * FROM sod_data) SELECT * FROM cte LIMIT 5",
    "SELECT COUNT(*) FROM sleeper_defects_data",
]
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

for sql in safe_queries:
    ok, reason = llm_sql.validate_sql(sql)
    check(f"Safe SQL accepted: {sql[:55]}…", ok, reason if not ok else "")

for sql, label in unsafe_queries:
    ok, reason = llm_sql.validate_sql(sql)
    check(f"Unsafe SQL rejected [{label}]", not ok, reason)

# ---------------------------------------------------------------------
# UNIT: extract_sql()
# ---------------------------------------------------------------------
section("UNIT: extract_sql()")

cases = [
    ("```sql\nSELECT 1\n```",        "SELECT 1"),
    ("```\nSELECT 2\n```",           "SELECT 2"),
    ("Here is the query:\nSELECT 3", "SELECT 3"),
    ("SELECT 4 LIMIT 5",             "SELECT 4 LIMIT 5"),
]
for raw, expected in cases:
    got = llm_sql.extract_sql(raw)
    check(f"extract_sql: {repr(raw[:40])}", got.startswith(expected[:8]), f"got={repr(got[:60])}")

# ---------------------------------------------------------------------
# UNIT: get_database_schema()
# ---------------------------------------------------------------------
section("UNIT: get_database_schema()")

schema = llm_sql.get_database_schema(DB)
for tbl in ["vertical_wear_data", "lateral_wear_data", "sleeper_defects_data",
            "sod_data", "fittings_data", "rail_defects_data",
            "ballast_vegetation_data", "lip_flow_data"]:
    check(f"Schema contains table: {tbl}", tbl in schema)

check("Schema contains column hint", "Maximum vertical rail wear" in schema)

# Verify cache hit
schema2 = llm_sql.get_database_schema(DB)
check("Schema cache returns same object (mtime unchanged)", schema is schema2)

# Invalidate cache and ensure new read
llm_sql.invalidate_schema_cache()
schema3 = llm_sql.get_database_schema(DB)
check("After invalidation, schema re-read (new object)", schema3 is not schema2)

# ---------------------------------------------------------------------
# UNIT: execute_sql() — read-only execution
# ---------------------------------------------------------------------
section("UNIT: execute_sql() — read-only execution")

cols, rows = llm_sql.execute_sql(
    "SELECT section_name, CAST(NULLIF(max_vertical_wear_mm,'') AS REAL) AS wear "
    "FROM vertical_wear_data ORDER BY wear DESC LIMIT 5",
    DB,
)
check("execute_sql returns columns", bool(cols), str(cols))
check("execute_sql returns rows", len(rows) > 0, f"{len(rows)} rows")
check("execute_sql column count matches", len(cols) == len(rows[0]))

# Attempt write (should raise)
try:
    llm_sql.execute_sql("INSERT INTO sod_data VALUES (NULL)", DB)
    check("Write SQL blocked at execute level", False, "Should have raised")
except Exception as ex:
    check("Write SQL blocked at execute level", True, str(ex)[:80])

# ---------------------------------------------------------------------
# TEST E: Empty question
# ---------------------------------------------------------------------
section("TEST E: Empty question")

result = llm_sql.ask_question("", DB)
check("Empty question -> success=False", not result["success"])
check("Empty question → useful error message", bool(result.get("error")), result.get("error"))

# ---------------------------------------------------------------------
# TEST F: Ollama unreachable simulation
# ---------------------------------------------------------------------
section("TEST F: Ollama unreachable simulation")

result = llm_sql.ask_question(
    "Show all tables",
    DB,
    base_url="http://localhost:19999",
    timeout=3,
)
check("Ollama unreachable → success=False", not result["success"])
check("Ollama unreachable → error mentions Ollama", "ollama" in result.get("error","").lower() or "reach" in result.get("error","").lower(), result.get("error"))

# ---------------------------------------------------------------------
# TEST G: Empty result set handling
# ---------------------------------------------------------------------
section("TEST G: Empty result set handling")

cols, rows = llm_sql.execute_sql(
    "SELECT * FROM vertical_wear_data WHERE max_vertical_wear_mm = 'IMPOSSIBLE_VALUE_XYZ'",
    DB,
)
check("Empty result → columns still returned", isinstance(cols, list))
check("Empty result → rows is empty list", rows == [])

# ---------------------------------------------------------------------
# INTEGRATION TESTS — require live Ollama
# ---------------------------------------------------------------------
section("INTEGRATION TESTS – Ollama availability check")

ollama_ok = False
try:
    status = llm_sql.check_ollama_status()
    ollama_ok = status.get("server_available", False) and status.get("model_available", False)
except Exception:
    pass

if not ollama_ok:
    print(f"{INFO}  Ollama not available – integration tests skipped.")
else:
    # TEST A: Basic sort query
    section("TEST A (integration): Highest vertical wear (sort)")
    t0 = time.time()
    result = llm_sql.ask_question(
        "Show the 10 locations with the highest vertical wear",
        DB,
    )
    elapsed = time.time() - t0
    check("Success=True", result.get("success"), result.get("error",""))
    check("SQL starts with SELECT", result.get("sql","").strip().upper().startswith("SELECT"))
    check("vertical_wear_data referenced", "vertical_wear" in result.get("sql","").lower())
    check("Rows returned (>0)", len(result.get("rows", [])) > 0, f"{len(result.get('rows',[]))} rows")
    print(f"  Time: {elapsed:.1f}s  |  Rows: {result.get('row_count',0)}  |  Retried: {result.get('retried')}")

    # TEST B: Aggregation / GROUP BY
    section("TEST B (integration): Most broken sleepers per section")
    t0 = time.time()
    result = llm_sql.ask_question(
        "Which sections have the highest total number of broken sleepers?",
        DB,
    )
    elapsed = time.time() - t0
    check("Success=True", result.get("success"), result.get("error",""))
    check("sleeper_defects_data referenced", "sleeper" in result.get("sql","").lower())
    check("Rows returned", len(result.get("rows", [])) > 0)
    print(f"  Time: {elapsed:.1f}s  |  Rows: {result.get('row_count',0)}")

    # TEST C: Average lateral wear
    section("TEST C (integration): Average lateral wear per section")
    t0 = time.time()
    result = llm_sql.ask_question(
        "What is the average lateral wear (max_wear_mm) per section?",
        DB,
    )
    elapsed = time.time() - t0
    check("Success=True", result.get("success"), result.get("error",""))
    check("lateral_wear_data referenced", "lateral_wear" in result.get("sql","").lower())
    check("AVG in SQL", "avg" in result.get("sql","").lower())
    check("Rows returned", len(result.get("rows", [])) > 0)
    print(f"  Time: {elapsed:.1f}s  |  Rows: {result.get('row_count',0)}")

# ---------------------------------------------------------------------
# TEST I: LLM status endpoint
# ---------------------------------------------------------------------
section("TEST I: check_ollama_status()")

status = llm_sql.check_ollama_status(model="qwen2.5-coder:7b")
check("Status dict has server_available", "server_available" in status)
check("Status dict has model_available", "model_available" in status)
check("Status dict has model", "model" in status)
if status.get("server_available"):
    check("Server is up", status["server_available"])
    check("Model is available", status.get("model_available"), status.get("model_error",""))
else:
    print(f"{INFO}  Ollama not running – server_available=False (expected in offline env)")

# Bad URL
status_bad = llm_sql.check_ollama_status(base_url="http://localhost:29999")
check("Bad URL -> server_available=False", not status_bad.get("server_available"))
check("Bad URL -> model_available=False", not status_bad.get("model_available"))
check("Bad URL -> server_error present", bool(status_bad.get("server_error")))

# Missing model (if server up)
status_nomodel = llm_sql.check_ollama_status(model="does-not-exist:99b")
if status_nomodel.get("server_available"):
    check("Missing model -> model_available=False", not status_nomodel.get("model_available"))
    check("Missing model -> model_error present", bool(status_nomodel.get("model_error")))
else:
    print(f"{INFO}  Server not reachable, skipping missing-model check.")

print("\n" + "=" * 60)
print("  Test run complete. Review PASS/FAIL above.")
print("=" * 60 + "\n")
