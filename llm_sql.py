"""
llm_sql.py — Local Text-to-SQL module for tabular-data analytics
================================================================
Uses a locally-running Ollama instance (qwen2.5-coder:7b by default)
to convert natural-language questions into safe, read-only SQLite SQL.

Key guarantees
--------------
* Schema read dynamically from the configured SQLite file (never hardcoded).
* Schema cache is invalidated automatically when the database mtime changes.
* SQL validated with multi-layer approach: keyword allowlist/blocklist,
  comment detection, multi-statement detection, and a read-only SQLite
  connection at execution time.
* One automatic retry when the first generated SQL fails validation or
  execution; the corrected query is validated again before running.
* Never executes Python, shell commands, or write SQL.
* All exceptions are caught; only safe user-facing messages are returned.
"""

import os
import re
import json
import time
import logging
import sqlite3
import urllib.request
import urllib.error
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger(__name__)

# ── Configuration (environment variables with sane defaults) ──────────────────
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.environ.get("OLLAMA_MODEL",    "qwen2.5-coder:7b")
DATABASE_PATH   = os.environ.get("DATABASE_PATH",   "railway.db")
OLLAMA_TIMEOUT  = int(os.environ.get("OLLAMA_TIMEOUT", "120"))
MAX_ROWS        = 500          # hard cap on returned rows
MAX_QUESTION    = 1000         # max characters in a user question

# ── Optional semantic hints for the TRC demonstration dataset ─────────────────
# These hints improve interpretation of the railway demo's text-valued columns.
# They are layered on top of the dynamically inspected schema and are never
# used as the source of truth for table/column existence.
COLUMN_HINTS: dict[str, str] = {
    # vertical_wear_data
    "max_vertical_wear_mm":          "Maximum vertical rail wear in mm (numeric text)",
    "average_vertical_wear_mm":      "Average vertical rail wear in mm (numeric text)",
    # lateral_wear_data
    "max_wear_mm":                   "Maximum lateral rail wear in mm (numeric text)",
    "average_lateral_wear_mm":       "Average lateral rail wear in mm (numeric text)",
    # lip_flow_data
    "max_value_mm":                  "Maximum lip flow value in mm (numeric text)",
    "average_value_mm":              "Average lip flow value in mm (numeric text)",
    # sleeper_defects_data
    "nos_of_affected_sleepers_broken_sleeper":          "Count of broken sleepers (numeric text)",
    "nos_of_affected_sleepers_cracked_sleeper_2mm_100mm": "Count of cracked sleepers (numeric text)",
    "nos_of_affected_sleepers_spalling_in_sleeper_1000_mm2": "Count of spalling sleepers (numeric text)",
    "nos_of_affected_sleepers_misalignment_50":         "Count of misaligned sleepers (numeric text)",
    "nos_of_affected_sleepers_dancing_sleeper":         "Count of dancing sleepers (numeric text)",
    "nos_of_affected_sleepers_improper_spacing_20mm":   "Count of improperly spaced sleepers (numeric text)",
    # sod_data
    "size_of_obstacle_mm_l":  "Obstacle length in mm (numeric text)",
    "size_of_obstacle_mm_b":  "Obstacle breadth in mm (numeric text)",
    "size_of_obstacle_mm_h":  "Obstacle height in mm (numeric text)",
    # fittings_data
    "component_defects_left_rail_missing_loose_clip":   "Missing/loose clips on left rail (numeric text)",
    "component_defects_left_rail_missing_bolt_and_nut": "Missing bolts/nuts on left rail (numeric text)",
    "component_defects_right_rail_missing_loose_clip":  "Missing/loose clips on right rail (numeric text)",
    "component_defects_right_rail_missing_bolt_and_nut":"Missing bolts/nuts on right rail (numeric text)",
    # rail_defects_data
    "value_of_defect_mm":     "Rail gap/defect value in mm (numeric text)",
    # ballast_vegetation_data
    "length_of_track_having_m_defect_length": "Length of defective track in metres (numeric text)",
    # common
    "start_location_km":      "Starting kilometre marker (numeric text)",
    "location_km":            "Kilometre marker (numeric text)",
    "location_start_location_km": "Start kilometre marker (numeric text)",
    "location_meter":         "Metre position within a km (numeric text)",
    "length_m":               "Length in metres (numeric text)",
    "run_date":               "Date of TRC run (text, e.g. '15-Aug-2025')",
    "section_name":           "Railway section name",
    "line_direction":         "Track direction (UP / DN / IInd / IIIrd etc.)",
    "trc_no":                 "Track Recording Car number",
    "run_no":                 "Run number identifier",
}

# ── Schema Cache ──────────────────────────────────────────────────────────────
_schema_cache: dict = {}   # {"schema_text": str, "mtime": float, "db_path": str}


def _db_mtime(db_path: str) -> float:
    """Return file modification time, or 0.0 if not found."""
    try:
        return Path(db_path).stat().st_mtime
    except OSError:
        return 0.0


def get_database_schema(db_path: str = DATABASE_PATH, *, force_refresh: bool = False) -> str:
    """
    Read the SQLite schema dynamically using PRAGMA table_info.
    Results are cached and automatically invalidated when the database
    file modification time changes.

    Returns a human-readable schema text injected into the LLM prompt.
    """
    global _schema_cache
    mtime = _db_mtime(db_path)

    if (
        not force_refresh
        and _schema_cache.get("db_path") == db_path
        and _schema_cache.get("mtime") == mtime
        and _schema_cache.get("schema_text")
    ):
        log.debug("Schema cache hit for '%s'.", db_path)
        return _schema_cache["schema_text"]

    log.info("Reading schema from '%s' (mtime=%.0f).", db_path, mtime)
    lines = []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        for (tbl,) in tables:
            lines.append(f"\nTABLE: {tbl}")
            cols = conn.execute(f"PRAGMA table_info('{tbl}')").fetchall()
            for col in cols:
                col_name = col[1]
                col_type = col[2] or "TEXT"
                hint = COLUMN_HINTS.get(col_name, "")
                hint_str = f"  -- {hint}" if hint else ""
                lines.append(f"  - {col_name} {col_type}{hint_str}")
        conn.close()
    except Exception as ex:
        log.error("Schema read failed: %s", ex)
        raise RuntimeError(f"Cannot read database schema: {ex}") from ex

    schema_text = "\n".join(lines)
    _schema_cache = {"schema_text": schema_text, "mtime": mtime, "db_path": db_path}
    return schema_text


def invalidate_schema_cache() -> None:
    """Manually invalidate the schema cache (e.g. after a pipeline run)."""
    global _schema_cache
    _schema_cache = {}
    log.info("Schema cache invalidated.")


# ── Prompt Builder ────────────────────────────────────────────────────────────
def build_prompt(question: str, schema_text: str) -> str:
    """Build a strict, schema-aware Text-to-SQL prompt."""
    return f"""You are a SQLite SQL generator for a generic tabular-data analytics platform.
The database may contain any user-provided dataset. Infer the answer only from
the live schema below; do not assume railway tables, columns, or a particular
domain.

DATABASE SCHEMA
===============
{schema_text}

CRITICAL RULES — follow every rule exactly:
1. Output ONLY a single valid SQLite SELECT statement (or a WITH ... SELECT).
2. Do NOT output INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, REPLACE,
   ATTACH, DETACH, VACUUM, PRAGMA, REINDEX, TRUNCATE, or any write/admin SQL.
3. Use ONLY the exact table names and column names listed in the schema above.
   Do NOT invent, guess, or alias to nonexistent names.
4. Respect the declared SQLite types in the schema. For numeric operations
   (SUM, AVG, MIN, MAX, comparisons, numeric ORDER BY, arithmetic), use numeric
   columns directly. If a value is declared or clearly represented as TEXT but
   contains numbers, use:
       CAST(NULLIF(column_name, '') AS REAL)
   Do not apply railway-specific assumptions to other datasets.
5. Add LIMIT 100 unless the question asks for more or aggregates all rows.
6. Do NOT include SQL comments (-- or /* */).
7. Do NOT include any explanation, markdown, or surrounding text.
   Output ONLY the raw SQL query, nothing else.
8. Do NOT use semicolons inside the query (one trailing semicolon is allowed).

USER QUESTION
=============
{question}

SQL QUERY:"""


# ── Ollama HTTP Client ────────────────────────────────────────────────────────
def call_ollama(
    prompt: str,
    model: str = OLLAMA_MODEL,
    base_url: str = OLLAMA_BASE_URL,
    timeout: int = OLLAMA_TIMEOUT,
) -> str:
    """
    Call the local Ollama /api/generate endpoint.
    Returns the model's text response (stripped).
    Raises ConnectionError, TimeoutError, or ValueError on problems.
    Detailed logging of request/response sizes and HTTP status is performed.
    A single retry is performed for transient HTTP 500 server errors.
    """
    url = f"{base_url}/api/generate"
    payload_dict = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.0,   # deterministic output
            "num_predict": 512,
            "stop": ["\n\n", "USER QUESTION", "DATABASE SCHEMA"],
        },
    }
    payload_bytes = json.dumps(payload_dict).encode()

    # Log request diagnostics (avoid logging full prompt content)
    log.info(
        "Calling Ollama: model=%s, url=%s, prompt_len=%d, payload_bytes=%d",
        model,
        url,
        len(prompt),
        len(payload_bytes),
    )

    attempts = 0
    max_attempts = 2  # original attempt + one retry for HTTP 500
    while attempts < max_attempts:
        attempts += 1
        try:
            req = urllib.request.Request(
                url,
                data=payload_bytes,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status_code = resp.getcode()
                body_bytes = resp.read()
                body_text = body_bytes.decode(errors="ignore")
                log.info(
                    "Ollama response: status=%s, body_len=%d, attempt=%d",
                    status_code,
                    len(body_bytes),
                    attempts,
                )
                # If server returns a 5xx error, consider retrying (already captured via HTTPError, but some servers may return 200 with error text)
                if status_code >= 500:
                    log.warning(
                        "Ollama returned HTTP %s, will retry if attempts remain",
                        status_code,
                    )
                    if attempts < max_attempts:
                        continue
                    raise ConnectionError(
                        f"Ollama server error {status_code}: {body_text[:200]}"
                    )
                # Normal path: parse JSON
                data = json.loads(body_text)
                text = data.get("response", "").strip()
                if not text:
                    raise ValueError("Ollama returned an empty response.")
                log.debug("Ollama raw response (first 400 chars): %.400s", text)
                return text
        except urllib.error.HTTPError as ex:
            # HTTPError includes a status code and response body
            status_code = ex.code
            error_body = ex.read().decode(errors="ignore")
            log.error(
                "HTTPError from Ollama: status=%s, body_preview=%s", status_code, error_body[:200]
            )
            if status_code == 500 and attempts < max_attempts:
                log.info("Retrying Ollama call after HTTP 500 error (attempt %d)", attempts)
                continue
            raise ConnectionError(
                f"HTTP error {status_code} from Ollama: {ex.reason}"
            ) from ex
        except urllib.error.URLError as ex:
            log.error("URLError when contacting Ollama: %s", ex.reason)
            raise ConnectionError(
                f"Cannot reach Ollama at {base_url}: {ex.reason}"
            ) from ex
        except TimeoutError as ex:
            log.error("Ollama request timed out after %s seconds", timeout)
            raise TimeoutError(f"Ollama did not respond within {timeout}s.") from ex
        except json.JSONDecodeError as ex:
            log.error("Failed to decode Ollama JSON response: %s", ex)
            raise ValueError(f"Unexpected Ollama response format: {ex}") from ex
    # If we exit the loop without returning, raise a generic error
    raise ConnectionError("Failed to get a successful response from Ollama after retries.")



# ── SQL Extractor ─────────────────────────────────────────────────────────────
_CODE_BLOCK_RE = re.compile(
    r"```(?:sql)?\s*\n?(.*?)(?:\n?```|$)", re.DOTALL | re.IGNORECASE
)


def extract_sql(response_text: str) -> str:
    """
    Extract the SQL query from a model response.
    Priority: ```sql ... ``` block > plain text starting with SELECT/WITH.
    Returns the extracted SQL, stripped of leading/trailing whitespace.
    """
    # 1. Try fenced code block
    m = _CODE_BLOCK_RE.search(response_text)
    if m:
        sql = m.group(1).strip()
        if sql:
            return sql

    # 2. Find first SELECT or WITH and take everything from there
    upper = response_text.upper()
    for kw in ("SELECT", "WITH"):
        idx = upper.find(kw)
        if idx != -1:
            return response_text[idx:].strip()

    # 3. Fallback: return stripped response (validation will reject it if bad)
    return response_text.strip()


# ── SQL Validator ─────────────────────────────────────────────────────────────

# Keywords that must never appear in a safe read-only query
_WRITE_ADMIN_KEYWORDS = frozenset([
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "REPLACE", "ATTACH", "DETACH", "VACUUM", "PRAGMA",
    "REINDEX", "TRUNCATE", "GRANT", "REVOKE",
])

# Patterns to detect SQL comments
_COMMENT_LINE_RE  = re.compile(r"--")
_COMMENT_BLOCK_RE = re.compile(r"/\*")


def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Multi-layer SQL validation. Returns (is_valid: bool, reason: str).

    Layers:
      1. Non-empty check
      2. No SQL comment tokens (-- or /*)
      3. Must start with SELECT or WITH
      4. No dangerous keywords (using word-boundary matching, not just substring)
      5. Single-statement check (no ; except optionally at the very end)
    """
    if not sql or not sql.strip():
        return False, "Generated SQL is empty."

    # Layer 2: comment detection
    if _COMMENT_LINE_RE.search(sql):
        return False, "Generated SQL contains inline comments (--)."
    if _COMMENT_BLOCK_RE.search(sql):
        return False, "Generated SQL contains block comments (/* */)."

    # Normalize: remove trailing semicolons for analysis; we'll re-add if needed
    normalized = sql.strip().rstrip(";").strip()

    # Layer 3: must begin with SELECT or WITH (case-insensitive)
    first_token = normalized.split()[0].upper() if normalized.split() else ""
    if first_token not in ("SELECT", "WITH"):
        return False, (
            f"Generated SQL must start with SELECT or WITH, "
            f"but starts with '{first_token}'."
        )

    # Layer 4: dangerous keyword check (word boundary)
    upper_sql = normalized.upper()
    for kw in _WRITE_ADMIN_KEYWORDS:
        # Use word boundary: the keyword must be a whole token
        pattern = rf"\b{re.escape(kw)}\b"
        if re.search(pattern, upper_sql):
            return False, f"Generated SQL contains forbidden keyword: {kw}."

    # Layer 5: single statement — semicolons in the middle of the query
    # Split by ';' and check that at most one non-empty part exists
    parts = [p.strip() for p in normalized.split(";") if p.strip()]
    if len(parts) > 1:
        return False, "Generated SQL contains multiple statements (semicolon detected)."

    return True, "OK"


# ── SQL Executor ──────────────────────────────────────────────────────────────
def execute_sql(sql: str, db_path: str = DATABASE_PATH) -> tuple[list, list]:
    """
    Execute a validated SELECT query against the database in read-only mode.
    Returns (columns: list[str], rows: list[list]).
    Raises on any execution error.
    Hard caps result set at MAX_ROWS.
    """
    # Normalise: ensure no trailing semicolon confuses sqlite3
    clean_sql = sql.strip().rstrip(";").strip()

    # Inject LIMIT only for read‑only SELECT/ WITH queries
    first_tok = clean_sql.split()[0].upper() if clean_sql.split() else ""
    if first_tok in ("SELECT", "WITH") and "LIMIT" not in clean_sql.upper():
        clean_sql = f"{clean_sql} LIMIT {MAX_ROWS}"

    log.info("Executing SQL: %.300s", clean_sql)
    t0 = time.perf_counter()

    try:
        # Open in read-only URI mode — prevents any accidental writes
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = None  # return plain tuples
        cur = conn.execute(clean_sql)
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(MAX_ROWS)
        conn.close()
    except sqlite3.OperationalError as ex:
        raise ValueError(f"SQL execution error: {ex}") from ex

    elapsed = time.perf_counter() - t0
    log.info(
        "Query completed in %.3fs — %d rows, %d columns.",
        elapsed, len(rows), len(columns),
    )
    return columns, [list(r) for r in rows]


# ── Retry helper ──────────────────────────────────────────────────────────────
def _build_correction_prompt(
    original_question: str,
    failed_sql: str,
    error_message: str,
    schema_text: str,
) -> str:
    """Build a follow-up prompt asking the model to fix its previous SQL."""
    return f"""You are a SQLite SQL generator for a generic tabular-data analytics platform.
Your previous SQL query was rejected.

DATABASE SCHEMA
===============
{schema_text}

ORIGINAL QUESTION
=================
{original_question}

PREVIOUS (INCORRECT) SQL
=========================
{failed_sql}

ERROR
=====
{error_message}

CORRECTION RULES (same as before, plus):
- Fix ONLY the error described above.
- Keep the same query intent.
- Respect the declared SQLite types. Cast text-valued numeric columns with
  CAST(NULLIF(column_name, '') AS REAL) when needed, but do not assume every
  dataset stores numbers as text.
- Output ONLY the corrected raw SQL query. No comments, no explanation.

CORRECTED SQL QUERY:"""


# ── Main Orchestrator ─────────────────────────────────────────────────────────
def ask_question(
    question: str,
    db_path: str = DATABASE_PATH,
    model: str = OLLAMA_MODEL,
    base_url: str = OLLAMA_BASE_URL,
    timeout: int = OLLAMA_TIMEOUT,
) -> dict:
    """
    Full pipeline: natural-language question → validated SQL → query results.

    Returns a dict:
      On success:
        {success: True, question, sql, columns, rows, row_count, retried}
      On failure:
        {success: False, error: <user-friendly message>}
    """
    # ── Input validation ──────────────────────────────────────────────────────
    question = (question or "").strip()
    if not question:
        return {"success": False, "error": "Question cannot be empty."}
    if len(question) > MAX_QUESTION:
        return {
            "success": False,
            "error": f"Question is too long (max {MAX_QUESTION} characters).",
        }

    log.info("Question received: %s", question)

    # ── Schema ────────────────────────────────────────────────────────────────
    try:
        schema_text = get_database_schema(db_path)
    except RuntimeError as ex:
        return {"success": False, "error": str(ex)}

    # ── First attempt ─────────────────────────────────────────────────────────
    prompt = build_prompt(question, schema_text)
    try:
        raw_response = call_ollama(prompt, model=model, base_url=base_url, timeout=timeout)
    except ConnectionError as ex:
        return {"success": False, "error": f"Ollama is not reachable. {ex}"}
    except TimeoutError as ex:
        return {"success": False, "error": str(ex)}
    except ValueError as ex:
        return {"success": False, "error": str(ex)}

    sql = extract_sql(raw_response)
    log.info("Extracted SQL (attempt 1): %s", sql)

    valid, reason = validate_sql(sql)
    exec_error: str | None = None

    if valid:
        try:
            columns, rows = execute_sql(sql, db_path)
        except ValueError as ex:
            exec_error = str(ex)
            valid = False
            reason = exec_error

    # ── One retry if first attempt failed ────────────────────────────────────
    retried = False
    if not valid:
        log.warning("First attempt failed (%s). Attempting correction.", reason)
        retried = True
        correction_prompt = _build_correction_prompt(question, sql, reason, schema_text)
        try:
            raw_response2 = call_ollama(
                correction_prompt, model=model, base_url=base_url, timeout=timeout
            )
        except (ConnectionError, TimeoutError, ValueError) as ex:
            return {
                "success": False,
                "error": f"SQL generation failed and correction also failed: {ex}",
            }

        sql2 = extract_sql(raw_response2)
        log.info("Extracted SQL (attempt 2): %s", sql2)

        valid2, reason2 = validate_sql(sql2)
        if not valid2:
            log.error("Corrected SQL also failed validation: %s", reason2)
            return {
                "success": False,
                "error": (
                    f"Could not generate valid SQL for your question. "
                    f"Last validation error: {reason2}"
                ),
            }

        try:
            columns, rows = execute_sql(sql2, db_path)
            sql = sql2   # use corrected SQL in the response
        except ValueError as ex:
            log.error("Corrected SQL execution failed: %s", ex)
            return {
                "success": False,
                "error": f"SQL was generated but could not be executed: {ex}",
            }

    return {
        "success":   True,
        "question":  question,
        "sql":       sql,
        "columns":   columns,
        "rows":      rows,
        "row_count": len(rows),
        "retried":   retried,
    }


# ── Ollama Health Check ───────────────────────────────────────────────────────
def check_ollama_status(
    model: str = OLLAMA_MODEL,
    base_url: str = OLLAMA_BASE_URL,
) -> dict:
    """
    Check Ollama server availability AND configured model availability separately.
    Returns a status dict consumed by GET /api/llm-status.
    """
    status: dict = {"model": model, "base_url": base_url}

    # 1. Server reachable?
    try:
        req = urllib.request.Request(f"{base_url}/api/tags", method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode())
        status["server_available"] = True
    except Exception as ex:
        status["server_available"] = False
        status["server_error"]     = str(ex)
        status["model_available"]  = False
        return status

    # 2. Model present in the tag list?
    models_listed = [m.get("name", "") for m in body.get("models", [])]
    # Ollama tag matching: "qwen2.5-coder:7b" may appear as exact match or with digest
    model_found = any(m == model or m.startswith(model.split(":")[0]) for m in models_listed)
    status["model_available"] = model_found
    if not model_found:
        status["model_error"] = (
            f"Model '{model}' not found in Ollama. "
            f"Available: {', '.join(models_listed) or 'none'}. "
            f"Run: ollama pull {model}"
        )
    return status
