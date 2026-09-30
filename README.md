# econ_analyst_DB

A clean, queryable mirror of BLS and Census data. Raw files are downloaded
once, cleaned into tidy tables, and served from Postgres, so analysis starts
from tables instead of re-scraping flat files. Every download is kept, so you
can see what the data said on any past fetch date.

Currently covers **CES** (Current Employment Statistics). CPS and ACS will
follow the same pattern.

```
BLS website ──ingest──> R2 raw/ces/{date}/      (files exactly as downloaded)
                          │
                      transform (DuckDB)
                          ▼
                        R2 clean/ces/{date}/    (tidy Parquet, every date kept)
                          │
                         load
                          ▼
                        Neon Postgres           (latest data, ready to query)
```

- **R2**: Cloudflare file storage (bucket `econ-raw`).
- **Parquet**: a compressed, typed file format for tables; R and Python read it directly.
- **Neon**: hosted Postgres database, reachable from any machine.

## Set up on a new machine (Windows)

1. Install [Python 3.12+](https://www.python.org/downloads/) and (for R) R 4.3+.
2. Get the project folder onto the machine (git clone or copy).
3. Copy `.env` into the project folder. It holds the passwords and is not in
   git, so move it by a secure route (password manager, USB stick). It needs:
   `R2_KEY_ID`, `R2_SECRET`, `R2_ACCOUNT_ID`, `BLS_CONTACT_EMAIL`, `DATABASE_URL`
   (and optionally `DATABASE_URL_UNPOOLED`).
4. Create the virtual environment (a private folder of Python packages for this
   project) and install packages. In PowerShell, from the project folder:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   python -m pip install -r requirements.txt
   ```

   If PowerShell refuses to run `Activate.ps1`, run this once, then retry:
   `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.
   Your prompt shows `(.venv)` when the environment is active.

5. For R: `install.packages(c("DBI", "RPostgres", "duckdb"))`

## Run the pipeline

With the virtual environment active:

```powershell
python run_pipeline.py
```

This downloads today's CES files (skipped if already done today), builds clean
Parquet, and replaces the Postgres tables. A full run takes about 1–3 minutes.

Each step can also be run alone:

```powershell
python -m ingest.ces                      # BLS -> R2 raw
python -m transform.ces [YYYY-MM-DD]      # raw -> clean Parquet (default: latest fetch)
python -m serve.load_ces [YYYY-MM-DD]     # clean -> Postgres (default: latest fetch)
```

## Query the data

**Postgres** has the latest fetch: tables `ces_monthly`, `ces_annual`,
`ces_series_meta`, and `pipeline_loads` (which fetch date is being served).
**R2 Parquet** has full history for every fetch date.

### R

```r
source("R/connect.R")    # run with the project folder as working directory

pg <- neon_connect()
payems <- dbGetQuery(pg, "
  SELECT m.date, m.value
  FROM ces_monthly m
  WHERE m.series_id = 'CES0000000001'
  ORDER BY m.date")

# Compare vintages: one row per (fetch date, month)
duck <- r2_connect()
vintages <- dbGetQuery(duck, "
  SELECT regexp_extract(filename, '[0-9]{4}-[0-9]{2}-[0-9]{2}') AS fetch_date, date, value
  FROM read_parquet('r2://econ-raw/clean/ces/*/ces_monthly.parquet', filename = true)
  WHERE series_id = 'CES0000000001' AND date >= '2025-01-01'
  ORDER BY date, fetch_date")
```

### Python

```python
from common import duckdb_connect   # run from the project folder
import os

con = duckdb_connect()              # can read r2:// paths
con.execute("INSTALL postgres; LOAD postgres;")
con.execute(f"ATTACH '{os.environ['DATABASE_URL']}' AS pg (TYPE postgres, READ_ONLY)")

payems = con.sql("""
    SELECT date, value FROM pg.ces_monthly
    WHERE series_id = 'CES0000000001' ORDER BY date
""").df()

vintages = con.sql("""
    SELECT regexp_extract(filename, '[0-9]{4}-[0-9]{2}-[0-9]{2}') AS fetch_date, date, value
    FROM read_parquet('r2://econ-raw/clean/ces/*/ces_monthly.parquet', filename = true)
    WHERE series_id = 'CES0000000001' AND date >= '2025-01-01'
    ORDER BY date, fetch_date
""").df()
```

## Storage limit (Neon Free plan)

Neon's Free plan allows 0.5 GB. Full CES monthly history (1939 onward, 8
million rows) needs about 0.85 GB in Postgres, plus the same again briefly
while a reload swaps tables. So Postgres currently holds **2020 onward**
(about 0.2 GB). Full history is always available from the R2 Parquet files.

To serve full history after upgrading Neon to a paid plan, set
`SERVE_START_YEAR = None` in `serve/load_ces.py` and rerun the pipeline.

## Files

| Path | What it does |
|---|---|
| `run_pipeline.py` | Runs ingest → transform → load |
| `common.py` | Shared helpers: `.env` loading, R2 client, DuckDB connection |
| `ingest/ces.py` | Downloads CES files from BLS to R2 `raw/ces/{date}/` |
| `transform/ces.py` | Cleans raw files into Parquet at R2 `clean/ces/{date}/` |
| `serve/load_ces.py` | Loads latest clean Parquet into Postgres |
| `R/connect.R` | `neon_connect()` and `r2_connect()` for R sessions |
| `neon.ts`, `package.json` | Neon CLI project config (`neon deploy`) |
| `test_r2.py` | Scratch script: reads a file from R2 with DuckDB |
| `CLAUDE.md` | Project notes for Claude Code sessions |
