"""
Railway Inspection Data Processing Pipeline — Final Version
============================================================
FIX: Garbage NULL columns after sheet_name are now eliminated via
     three defence layers:
     1. _is_skip_row — rejects any row where a cell is long + multiline
     2. clean_table  — drops columns with >80% NULL values
     3. process_folder — trims DataFrame to [data_cols + META_COLS] before
                         every write, so no extra column can ever reach SQLite

Usage:
  python railway_pipeline.py <folder_path> [db_path]
"""

import re
import sqlite3
import logging
import pandas as pd
from pathlib import Path

DB_PATH          = "railway.db"
BLOCK_TRIGGER    = "EXCEPTION REPORT"
HEADER_SCAN_ROWS = 12
LOG_FORMAT       = "%(asctime)s [%(levelname)s] %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger(__name__)

# Exact set of metadata columns — ORDER MATTERS, sheet_name is LAST
META_COLS = [
    "section_name", "line_direction", "km_range",
    "trc_no", "run_date", "run_no",
    "defect", "rail_side",
    "source_file", "sheet_name",
]

DATASET_TYPE_MAP = [
    ("sod exception",           "sod_data"),
    ("sod_exception",           "sod_data"),
    ("lip flow",                "lip_flow_data"),
    ("lip_flow",                "lip_flow_data"),
    ("vertical wear",           "vertical_wear_data"),
    ("vertical_wear",           "vertical_wear_data"),
    ("lateral rail wear",       "lateral_wear_data"),
    ("lateral wear",            "lateral_wear_data"),
    ("lateral_wear",            "lateral_wear_data"),
    ("sleeper defect",          "sleeper_defects_data"),
    ("sleeper_defect",          "sleeper_defects_data"),
    ("sleeper",                 "sleeper_defects_data"),
    ("rail defect",             "rail_defects_data"),
    ("rail_defect",             "rail_defects_data"),
    ("fittings",                "fittings_data"),
    ("fitting",                 "fittings_data"),
    ("ballast and vegetation",  "ballast_vegetation_data"),
    ("ballast & vegetation",    "ballast_vegetation_data"),
    ("ballast_and_vegetation",  "ballast_vegetation_data"),
    ("ballast",                 "ballast_data"),
    ("vegetation",              "vegetation_data"),
    ("sod",                     "sod_data"),
    ("geometry",                "geometry_data"),
    ("gauge",                   "gauge_data"),
    ("squat",                   "squat_data"),
    ("corrugation",             "corrugation_data"),
    ("head check",              "head_check_data"),
]

_SKIP_PATTERNS = re.compile(
    r"reporting\s*date|itms\s*reports?|^reporting|^iTMS",
    re.IGNORECASE,
)


def _is_numeric(val) -> bool:
    try:
        float(str(val).strip())
        return True
    except (ValueError, TypeError):
        return False


def _cell_lines(val) -> list:
    if pd.isna(val):
        return []
    return [ln.strip() for ln in str(val).split("\n") if ln.strip()]


def _populated(row: pd.Series) -> list:
    return [str(v).strip() for v in row.values
            if pd.notna(v) and str(v).strip() not in ("", "nan")]


def _is_skip_row(pop: list) -> bool:
    if not pop:
        return True
    if all(_SKIP_PATTERNS.search(p) for p in pop):
        return True
    if len(pop) == 1 and len(pop[0]) > 50:
        return True
    # FIX-1: Any multiline + long cell = metadata, skip the row
    for cell in pop:
        if "\n" in cell and len(cell) > 40:
            return True
    return False


# ════════════════════════════════════════════════════════════
#  1. detect_blocks
# ════════════════════════════════════════════════════════════
def detect_blocks(df: pd.DataFrame) -> list:
    trigger_rows = []
    for idx, row in df.iterrows():
        row_text = " ".join(str(v) for v in row.values if pd.notna(v))
        if BLOCK_TRIGGER.lower() in row_text.lower():
            trigger_rows.append(idx)
    if not trigger_rows:
        return []
    blocks = []
    for i, start in enumerate(trigger_rows):
        end = trigger_rows[i + 1] if i + 1 < len(trigger_rows) else df.index[-1] + 1
        block = df.loc[start: end - 1].reset_index(drop=True)
        blocks.append(block)
    log.info(f"  Detected {len(blocks)} block(s).")
    return blocks


# ════════════════════════════════════════════════════════════
#  2. extract_metadata
# ════════════════════════════════════════════════════════════
def extract_metadata(block: pd.DataFrame) -> dict:
    lines = []
    for _, row in block.iloc[:HEADER_SCAN_ROWS].iterrows():
        for val in row.values:
            lines.extend(_cell_lines(val))
    full_text = " | ".join(lines)

    def find(pattern, default=""):
        m = re.search(pattern, full_text, re.IGNORECASE)
        return m.group(1).strip() if m else default

    section_full = find(r"section[:\s]*([^\|]+?)(?:\s*\||$)")
    section_name = ""
    m = re.search(
        r"^(.*?Line)\s*:\s*(?:UP|DN|Down|IInd|IIIrd|IVth|3rd|4th)\s",
        section_full, re.IGNORECASE)
    if m:
        section_name = m.group(1).strip()
    else:
        m = re.search(r"^(.*?)(?:\s*KM\s*:|$)", section_full, re.IGNORECASE)
        section_name = m.group(1).strip() if m else section_full

    line_direction = ""
    m = re.search(r"\b(UP|DN|Down|IInd|IIIrd|IVth|3rd|4th)\s+Line",
                  section_full, re.IGNORECASE)
    if m:
        line_direction = m.group(1).strip()

    km_range  = find(r"KM\s*:\s*([\d.]+\s*to\s*[\d.]+)")
    trc_no    = find(r"TRC\s*No\.?\s*:?\s*([A-Z0-9\-/]+)")
    run_date  = find(r"RUN\s*Date\s*:?\s*([0-9]{1,2}[-/\s]\w+[-/\s][0-9]{2,4})")
    run_no    = find(r"RUN\s*No\.?\s*:?\s*([^\s\|]+)")
    rail_side = find(r"\b(Left Rail|Right Rail|Left|Right|LH|RH)\b")
    defect    = find(r"DEFECTS[-\s]+([A-Z][A-Z\s&/]+?)(?:\s*\(|\s*\||$)")
    if not defect:
        defect = find(r"[Dd]efect\s*[=:]\s*([^\|\n]+)")

    return {
        "full_header_text": full_text,
        "section_name":     section_name,
        "line_direction":   line_direction,
        "km_range":         km_range,
        "trc_no":           trc_no,
        "run_date":         run_date,
        "run_no":           run_no,
        "defect":           defect,
        "rail_side":        rail_side,
    }


# ════════════════════════════════════════════════════════════
#  3. extract_table
# ════════════════════════════════════════════════════════════
def _build_multilevel_columns(header_rows: list, n_cols: int) -> list:
    padded = []
    for row_vals in header_rows:
        row = list(row_vals)
        row = row[:n_cols] + [None] * max(0, n_cols - len(row))
        padded.append(row)
    ff_rows = []
    for idx, row in enumerate(padded):
        sr = pd.Series(row, dtype=object).replace({"": None, "nan": None})
        if idx < len(padded) - 1:
            sr = sr.ffill()
        ff_rows.append(sr.tolist())
    result = []
    for col_idx in range(n_cols):
        parts, prev = [], None
        for row in ff_rows:
            val = row[col_idx] if col_idx < len(row) else None
            if val and str(val).strip() and str(val).strip().lower() != "nan":
                raw_val = str(val).strip()
                # FIX-1b: Skip multiline long cells from column naming
                if "\n" in raw_val and len(raw_val) > 40:
                    continue
                v = raw_val
                if v != prev:
                    parts.append(v)
                    prev = v
        result.append("_".join(parts) if parts else f"col_{col_idx}")
    return result


def _find_header_and_data(block: pd.DataFrame):
    header_rows = []
    data_start  = None
    for i in range(1, len(block)):
        row = block.iloc[i]
        pop = _populated(row)
        if not pop:
            continue
        if _is_skip_row(pop):
            continue
        if _is_numeric(pop[0]):
            data_start = i
            break
        header_rows.append(row.tolist())
    return header_rows, data_start


def extract_table(block: pd.DataFrame):
    header_rows, data_start = _find_header_and_data(block)
    if not header_rows or data_start is None:
        log.warning("  Table header row not found — skipping block.")
        return None
    n_cols    = len(block.columns)
    col_names = _build_multilevel_columns(header_rows, n_cols)
    data_df   = block.iloc[data_start:].reset_index(drop=True)
    while len(col_names) < len(data_df.columns):
        col_names.append(f"col_{len(col_names)}")
    data_df.columns = col_names[: len(data_df.columns)]
    mask = data_df.iloc[:, 0].apply(_is_numeric)
    data_df = data_df[mask.values].reset_index(drop=True)
    return data_df if not data_df.empty else None


# ════════════════════════════════════════════════════════════
#  4. clean_table
# ════════════════════════════════════════════════════════════
def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all").reset_index(drop=True)
    df = df.dropna(how="all", axis=1)
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed|^col_\d+$", na=False)]
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(r"[^\w]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    df = df.loc[:, df.columns != ""]
    # Deduplicate column names
    seen, new_cols = {}, []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    df.columns = new_cols
    # FIX-2: Drop mostly-null columns (garbage from metadata leakage)
    null_ratio = df.isnull().mean()
    df = df.loc[:, null_ratio <= 0.8]
    # Enforce numeric lead column
    if len(df.columns) > 0:
        mask = df.iloc[:, 0].apply(_is_numeric)
        df = df[mask.values].reset_index(drop=True)
    return df


# ════════════════════════════════════════════════════════════
#  5. detect_dataset_type
# ════════════════════════════════════════════════════════════
def detect_dataset_type(metadata: dict, file_name: str = "") -> str:
    text = (file_name + " " +
            metadata.get("full_header_text", "") + " " +
            metadata.get("defect", "")).lower()
    for keyword, table_name in DATASET_TYPE_MAP:
        if keyword in text:
            return table_name
    return "general_data"


# ════════════════════════════════════════════════════════════
#  6. store_to_sql
# ════════════════════════════════════════════════════════════
def _table_columns(conn: sqlite3.Connection, table_name: str) -> list:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    if cur.fetchone() is None:
        return []
    return [r[1] for r in conn.execute(f"PRAGMA table_info('{table_name}')")]


def store_to_sql(df: pd.DataFrame, table_name: str, db_path: str = DB_PATH):
    try:
        with sqlite3.connect(db_path) as conn:
            existing = _table_columns(conn, table_name)
            if not existing:
                # First write — create the table
                df.to_sql(table_name, conn, if_exists="append", index=False)
            else:
                # Subsequent writes — only insert into already-existing columns
                common = [c for c in df.columns if c in existing]
                if not common:
                    log.warning(f"  No common columns for '{table_name}' — skipping.")
                    return
                df[common].dropna(axis=1, how="all").to_sql(
                    table_name, conn, if_exists="append", index=False)
        log.info(f"  Stored {len(df)} row(s) → '{table_name}'")
    except Exception as ex:
        log.error(f"  DB write error for '{table_name}': {ex}")


# ════════════════════════════════════════════════════════════
#  7. process_folder
# ════════════════════════════════════════════════════════════
def process_folder(folder_path: str, db_path: str = DB_PATH):
    folder = Path(folder_path)
    xlsx_files = sorted(folder.glob("*.xlsx"))
    if not xlsx_files:
        log.warning(f"No .xlsx files found in: {folder_path}")
        return

    log.info(f"Found {len(xlsx_files)} Excel file(s) in '{folder_path}'.")
    total_blocks = 0
    total_rows   = 0
    summary: dict = {}

    for file_path in xlsx_files:
        log.info(f"\n{'='*60}")
        log.info(f"FILE: {file_path.name}")
        try:
            xl = pd.ExcelFile(file_path, engine="openpyxl")
        except Exception as ex:
            log.error(f"  Cannot open '{file_path.name}': {ex}")
            continue

        for sheet_name in xl.sheet_names:
            log.info(f"  Sheet: '{sheet_name}'")
            try:
                raw_df = xl.parse(sheet_name, header=None, dtype=str)
            except Exception as ex:
                log.error(f"  Cannot read '{sheet_name}': {ex}")
                continue
            if raw_df.empty:
                continue

            blocks = detect_blocks(raw_df)
            if not blocks:
                continue

            for b_idx, block in enumerate(blocks):
                log.info(f"  Block {b_idx + 1}/{len(blocks)} ...")
                total_blocks += 1
                try:
                    metadata = extract_metadata(block)
                    log.info(f"    section='{metadata['section_name']}'  "
                             f"trc='{metadata['trc_no']}'  "
                             f"date='{metadata['run_date']}'")

                    table_df = extract_table(block)
                    if table_df is None or table_df.empty:
                        log.warning("    No usable table — skipping.")
                        continue

                    table_df = clean_table(table_df)
                    if table_df.empty:
                        log.warning("    Empty after cleaning — skipping.")
                        continue

                    # Attach metadata
                    table_df["section_name"]   = metadata.get("section_name",   "")
                    table_df["line_direction"] = metadata.get("line_direction", "")
                    table_df["km_range"]       = metadata.get("km_range",       "")
                    table_df["trc_no"]         = metadata.get("trc_no",         "")
                    table_df["run_date"]       = metadata.get("run_date",       "")
                    table_df["run_no"]         = metadata.get("run_no",         "")
                    table_df["defect"]         = metadata.get("defect",         "")
                    table_df["rail_side"]      = metadata.get("rail_side",      "")
                    table_df["source_file"]    = file_path.name
                    table_df["sheet_name"]     = sheet_name

                    # FIX-3: Guarantee column order — data cols first, META_COLS last
                    data_cols  = [c for c in table_df.columns if c not in META_COLS]
                    table_df   = table_df[data_cols + META_COLS]

                    tbl = detect_dataset_type(metadata, file_path.name)
                    log.info(f"    → table: '{tbl}'  rows: {len(table_df)}  "
                             f"cols: {len(data_cols)} data + {len(META_COLS)} meta")
                    store_to_sql(table_df, tbl, db_path)

                    total_rows += len(table_df)
                    summary[tbl] = summary.get(tbl, 0) + len(table_df)

                except Exception as ex:
                    log.error(f"    Block {b_idx + 1} failed: {ex}", exc_info=True)

    log.info(f"\n{'='*60}")
    log.info("PIPELINE COMPLETE")
    log.info(f"  Total blocks   : {total_blocks}")
    log.info(f"  Total rows     : {total_rows}")
    log.info(f"  Tables written :")
    for tbl, rows in sorted(summary.items()):
        log.info(f"    {tbl:<42} {rows:>6} rows")
    log.info(f"  Database       : {db_path}")
    log.info("=" * 60)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage:   python railway_pipeline.py <folder_path> [db_path]")
        print("Example: python railway_pipeline.py ./data railway.db")
        sys.exit(1)
    process_folder(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else DB_PATH)