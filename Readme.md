# 📊 Tabular Data Analytics — Railway TRC Demonstration

**A generic natural-language analytics platform for tabular datasets.**
Inspects the live schema of a configured SQLite database, turns plain-English questions into safe read-only SQL, visualises results, and exports them. Indian Railways Track Recording Car (TRC) inspection data is the primary demonstration and use case.

---

## Table of Contents

- [Overview](#overview)
- [Generic Tabular Analytics](#generic-tabular-analytics)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Source](#data-source)
- [Setup & Installation](#setup--installation)
- [Usage](#usage)
  - [Step 1 — Run the Web App](#step-1--run-the-web-app)
  - [Step 2 — Ingest Excel Data (Pipeline)](#step-2--ingest-excel-data-pipeline)
  - [Step 3 — Run Analytical Queries](#step-3--run-analytical-queries)
- [Pipeline Deep Dive](#pipeline-deep-dive)
- [Analytical Queries (Q1–Q5)](#analytical-queries-q1q5)
- [Database Schema](#database-schema)
- [API Reference](#api-reference)
- [Experiments & Research](#experiments--research)
- [Requirements](#requirements)

---

## Overview

The platform has two layers:

### Generic platform capabilities

- Dynamic SQLite table and column discovery at query time
- Natural-language-to-SQL generation using the discovered schema
- Read-only SQL validation and execution
- Result tables, visualisation, and CSV/XLSX/PDF export
- A database summary showing the tables and row counts currently available

The AI Assistant is not limited to railway tables. It can query any tabular dataset that has been ingested into the selected SQLite database. The database must contain the data first: the generic assistant does not itself provide an automatic importer for every possible CSV, Excel, or other tabular format.

### Railway demonstration modules

`railway_pipeline.py` is a railway-specific ingestion pipeline for complex TRC workbooks. The Q1–Q5 dashboards in `app.py` are railway-specific analytical examples built around the resulting TRC tables. These modules remain available as the primary demonstration; they are separate from the generic database inspection and AI query layer.

## Generic Tabular Analytics

After loading a dataset into SQLite, ask questions such as:

- “Show the top 10 customers by revenue.”
- “What is the average sales amount by region?”
- “Find products with stock below 10.”
- “Count employees by department.”

The assistant uses the actual table and column names discovered from SQLite, so arbitrary table names and schemas are supported. It does not invent tables or columns that are absent from the selected database.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      app.py (Flask)                     │
│                                                         │
│   Generic database/AI layer │ Railway demonstration     │
│   schema discovery          │ pipeline + Q1–Q5          │
│   /api/tables, /api/ask     │ railway_pipeline.py       │
│            │                │ /api/worst20 … /repeated  │
│            └───────────────►│ configured SQLite DB      │
└─────────────────────────────────────────────────────────┘
        ▲
        │ any ingested tabular data
        │ (TRC Excel pipeline is one supported path)
```

The web UI is a single-page application embedded directly in `app.py` as a Python string. It uses vanilla HTML/CSS/JS with no build step required.

---

## Project Structure

```
IndianRailwaysProject/
│
├── app.py                       # Flask web application + embedded SPA frontend
├── railway_pipeline.py          # Excel → SQLite ingestion pipeline
├── railway.db                   # SQLite database (generated after first pipeline run)
├── requirements.txt             # Python dependencies
├── .gitignore                   # Excludes the data/ folder from version control
│
├── data/                        # Raw TRC Excel reports (NOT committed to git)
│   ├── 1 SOD exception.xlsx
│   ├── 2 lip flow exception.xlsx
│   ├── 3 Vertical wear exception.xlsx
│   ├── 4 lateral rail wear.xlsx
│   ├── 5 Sleeper defects.xlsx
│   ├── 6 Rail defects.xlsx
│   ├── 7 fittings.xlsx
│   └── 8 ballast and vegetation.xlsx
│
├── experiments.ipynb            # Early exploration notebook
├── experiments_new.ipynb        # Iteration 2 experiments
├── experiments_new_day.ipynb    # Day-level analysis experiments
├── experiments_brand_new.ipynb  # Latest standalone experiments
├── gpu_usage.ipynb              # GPU / CUDA testing notebook
│
└── testing.py                   # Quick CUDA availability / matrix multiply benchmark
```

> **Note:** The `data/` directory is excluded from Git via `.gitignore`. Excel source files must be provided separately.

---

## Data Source

Eight TRC exception report types are supported, mapped to their corresponding SQLite tables:

| # | Excel File | SQLite Table |
|---|------------|-------------|
| 1 | `1 SOD exception.xlsx` | `sod_data` |
| 2 | `2 lip flow exception.xlsx` | `lip_flow_data` |
| 3 | `3 Vertical wear exception.xlsx` | `vertical_wear_data` |
| 4 | `4 lateral rail wear.xlsx` | `lateral_wear_data` |
| 5 | `5 Sleeper defects.xlsx` | `sleeper_defects_data` |
| 6 | `6 Rail defects.xlsx` | `rail_defects_data` |
| 7 | `7 fittings.xlsx` | `fittings_data` |
| 8 | `8 ballast and vegetation.xlsx` | `ballast_vegetation_data` |

---

## Setup & Installation

### Prerequisites

- Python 3.8+
- `pip`
- A SQLite database containing the tabular data you want to query
- The 8 TRC Excel files in `data/` only when using the railway demonstration pipeline

### Install Dependencies

```bash
pip install flask pandas openpyxl
```

Or using the requirements file (covers pipeline dependencies only):

```bash
pip install -r requirements.txt
pip install flask  # add Flask for the web app
```

> `requirements.txt` currently lists `pandas>=2.0.0` and `openpyxl>=3.1.0`.  
> Flask is additionally required to run `app.py`.

---

## Usage

### Step 1 — Run the Web App

```bash
cd IndianRailwaysProject
python app.py
```

Open your browser at **http://localhost:5000**

### Step 2 — Load data into SQLite

**Railway demonstration (supported ingestion path):**

1. In the sidebar, confirm the **Data Folder** path points to your `data/` directory  
   (default: `D:\ENGINEER\IndianRailwaysProject\data`)
2. Confirm the **Database** path (default: `railway.db`)
3. Click **Run Pipeline**
4. Watch real-time log output in the terminal panel on screen

**Via command line (alternative):**

```bash
python railway_pipeline.py ./data railway.db
```

The pipeline is designed for the supported TRC workbook layouts and maps the eight railway report types to railway-specific tables. It is not a universal CSV/Excel importer. To use another dataset, ingest it into SQLite using the dataset's appropriate loader or SQLite tooling, then set the database path in the web UI.

### Step 3 — Run queries and dashboards

Click **Ask Your Data** to query any loaded tables in natural language, or click **DB Summary** to inspect the discovered tables and row counts. When the railway tables are present, the railway-specific **Q1–Q5** dashboards are also available.

---

## Pipeline Deep Dive

`railway_pipeline.py` handles the railway-specific ETL lifecycle:

### Key Processing Steps

| Step | Function | Description |
|------|----------|-------------|
| 1 | `detect_blocks()` | Scans each Excel sheet for `"EXCEPTION REPORT"` triggers to identify report boundaries |
| 2 | `extract_metadata()` | Regex-parses the header rows (up to 12 rows) to extract section name, line direction, KM range, TRC number, run date/number, rail side, and defect type |
| 3 | `extract_table()` | Detects multilevel column headers; reconstructs column names by forward-filling merged cells |
| 4 | `clean_table()` | Normalises column names; drops all-null rows/columns; removes columns with >80% null (garbage guard); deduplicates column names |
| 5 | `detect_dataset_type()` | Maps each block to a target SQLite table name by keyword-matching the filename and header text |
| 6 | `store_to_sql()` | Appends data to SQLite; on subsequent writes, aligns to existing schema (no schema drift) |
| 7 | `process_folder()` | Orchestrates all of the above across every `.xlsx` file in the given folder |

### Metadata Columns (appended to every row)

Every row in every table carries these 10 provenance columns:

```
section_name, line_direction, km_range, trc_no, run_date, run_no,
defect, rail_side, source_file, sheet_name
```

### Robustness Fixes

The pipeline includes three explicit defences against garbage data from metadata leakage into table columns:

- **`_is_skip_row`** — Rejects rows where any cell is both multiline and longer than 40 characters
- **`clean_table`** — Drops columns with >80% null values
- **`process_folder`** — Enforces strict column order `[data_cols + META_COLS]` before every DB write

---

## Analytical Queries (Q1–Q5)

All queries are accessible from the web sidebar and return results instantly from the SQLite database.

### Q1 — Worst 20% Locations

Identifies the **top 20% worst locations** (by severity score) across all defect categories:

| Category | Severity Metric |
|----------|----------------|
| Vertical Wear | Max vertical wear (mm) |
| Lateral Wear | Max lateral wear (mm) |
| Lip Flow | Max value (mm) |
| Rail Defects | Gap value (mm) |
| Ballast / Vegetation | Defect length (m) |
| SOD | Obstacle volume (L × B × H mm³) |
| Sleeper Defects | Sum of all defect sub-types |
| Fittings | Sum of missing/loose clips and bolts |

Results are filterable by section. Severity is color-coded: 🔴 Critical / 🟠 High / 🟡 Medium / 🟢 OK.

---

### Q2 — Resource Deployment

Identifies which locations need immediate material supply:

| Category | Logic |
|----------|-------|
| **Rail Supply** | Top 20% wear locations (≥80th percentile) — priority rail replacement |
| **Sleepers Supply** | Blocks with most unserviceable sleepers (broken + cracked + spalling) |
| **Fitting Recoupment** | Blocks with most missing/loose clips and bolts |
| **Rail Gap Adjustment** | Largest rail gaps requiring immediate closure |

---

### Q3 — False Alert Detection

Flags TRC alerts that likely represent **false positives** by cross-referencing across multiple run numbers:

- **Likely False (1 run)** — Alert appeared in only one TRC run → probable false positive or already resolved
- **Persistent (2+ runs)** — Alert recurred across two or more runs → confirmed problem

Covers: SOD (Size of Obstacle) alerts and Vegetation alerts.

---

### Q4 — Consecutive Defects

Detects **contiguous track segments** with unserviceable sleepers spanning 2+ or 3+ consecutive kilometre locations. Results are ranked by sequence length and total defect count. A bar visualization shows relative severity.

---

### Q5 — Repeated in 3+ TRC Runs

Identifies **chronic problem locations** — KM points deficient across three or more separate TRC run numbers, for each defect category. These locations should be treated as systemic failures requiring engineering intervention.

---

## Database Schema

The generic AI layer reads every table and column directly from the selected SQLite database using SQLite schema inspection. It does not require the railway table names below. Railway tables produced by the demonstration pipeline contain defect-specific measurement columns plus the standard 10 metadata columns:

```sql
-- Example: vertical_wear_data
start_location_km, start_location_meter,
max_vertical_wear_mm, average_vertical_wear_mm,
... (additional measurement columns)
section_name, line_direction, km_range,
trc_no, run_date, run_no,
defect, rail_side, source_file, sheet_name
```

The default demonstration database file is `railway.db`.

---

## API Reference

All endpoints are served by the Flask app on `http://localhost:5000`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Serves the single-page analytics dashboard |
| `GET` | `/api/script_status` | Checks if `railway_pipeline.py` is discoverable |
| `POST` | `/run` | Starts the ingestion pipeline in a background thread |
| `GET` | `/stream` | Server-Sent Events stream of live pipeline log output |
| `GET` | `/api/tables?db=<path>` | Lists all tables and row counts in the database |
| `GET` | `/api/worst20?db=<path>&scope=<section>` | Q1 — Worst 20% locations |
| `GET` | `/api/resources?db=<path>` | Q2 — Resource deployment priorities |
| `GET` | `/api/false_alerts?db=<path>` | Q3 — False alert detection |
| `GET` | `/api/consecutive?db=<path>&n=<min>` | Q4 — Consecutive defect sequences |
| `GET` | `/api/repeated?db=<path>` | Q5 — Locations repeated in 3+ runs |
| `POST` | `/api/ask` | Accepts a natural-language question, discovers the selected SQLite schema, generates safe SQL via the local Ollama model, executes it read-only, and returns a JSON result |

---

## Experiments & Research

The project contains Jupyter notebooks used during development and analysis:

| Notebook | Purpose |
|----------|---------|
| `experiments.ipynb` | Initial exploration of Excel parsing strategies |
| `experiments_new.ipynb` | Improved parsing iteration; column detection refinements |
| `experiments_new_day.ipynb` | Day-level run comparison analysis |
| `experiments_brand_new.ipynb` | Latest standalone experiments (largest notebook, ~208 KB) |
| `gpu_usage.ipynb` | GPU environment check |

`testing.py` is a minimal script to verify CUDA availability and benchmark matrix multiplication on a GPU.

---

## Requirements

```
pandas>=2.0.0
openpyxl>=3.1.0
flask           # for app.py (not listed in requirements.txt)
```

To run the notebooks, additionally install:

```bash
pip install jupyter
```

---

## Notes

- `app.py` and `railway_pipeline.py` **must reside in the same directory** for the web UI's pipeline detection to work automatically.
- The pipeline is idempotent on schema: re-running on the same DB will append data but will not add new columns. Clear `railway.db` before a fresh full reload.
- The generic assistant can query tables already present in the selected SQLite database, but it does not replace a general-purpose CSV/Excel ingestion service. The railway pipeline only supports the documented TRC workbook layouts.
- Natural-language SQL quality depends on the local Ollama model and the clarity of the discovered schema. Queries must be read-only and are capped at 500 returned rows.
- Q1–Q5 are railway-specific dashboards; they may return no results or errors when their expected TRC tables and columns are not present, while **Ask Your Data** remains the generic query surface.
- The maintained automated test suite is located in `tests/` and currently passes all 11 tests. The older `test_llm_sql.py` script is retained as a legacy/manual validation utility and is not part of the maintained pytest suite.
- The web app runs on port **5000** with threading enabled (`threaded=True`). Debug mode is off by default.
- The `data/` folder is git-ignored to prevent large Excel files from being committed.

---

*Built for BSL Division · Central Railway · Indian Railways TRC Inspection Intelligence*
