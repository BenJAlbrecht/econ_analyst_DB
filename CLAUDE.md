# CLAUDE.md

## Project goals
- Build a clean, queryable mirror of BLS and Census data so analysis starts from
  tidy tables instead of re-scraping and re-cleaning flat files every session.
- Make the data reachable from any of my machines (home PC, laptop, work PC, all
  Windows) and from both R (my primary language) and Python.
- Preserve every release so revisions and vintages can be compared
  ("what did the data say at the time").
- Start with CES, then extend to CPS and ACS using the same raw -> clean -> serve
  pattern.
- Longer term: a public-facing analytics website for labor and demographic data,
  served from the Postgres layer, with refreshes automated on the BLS/Census
  release calendar.
- I'm learning this stack as I go, so favor simple, readable, well-documented code
  over clever abstractions.

## How to communicate with me
- Be concise when summarizing code or reporting results.
- Use plain language. When a technical term is necessary, explain it briefly the
  first time it comes up.

## Architecture

Three layers, one module per dataset in each:

| Layer | Where | What | Code |
|---|---|---|---|
| Raw | R2 `econ-raw/raw/{dataset}/{fetch_date}/{filename}` | Source files byte-for-byte as downloaded | `ingest/{dataset}.py` |
| Clean | R2 `econ-raw/clean/{dataset}/{fetch_date}/{table}.parquet` | Tidy, typed Parquet | `transform/{dataset}.py` |
| Serve | Neon Postgres (project `round-fog-78081219`, branch `production`) | Latest clean data only | `serve/load_{dataset}.py` |

- `fetch_date` is the local date of the download, `YYYY-MM-DD`. Every fetch date
  is kept in R2 forever; this is how vintages are preserved. Postgres holds only
  the latest.
- DuckDB is used as a local Python library (not MotherDuck). It reads and writes
  `r2://` paths directly via the `httpfs` extension and an R2 secret.
- `common.py` holds shared helpers: `.env` loading, `r2_client()` (boto3),
  `list_r2_keys()`, `list_r2_dates()`, `duckdb_connect()`.
- `run_pipeline.py` runs ingest -> transform -> load. Each step exposes
  `run(fetch_date=None) -> fetch_date` and can run alone via `python -m`.
- No scheduling/GitHub Actions yet (deliberately). Runs are manual.
- `R/connect.R` gives R users `neon_connect()` and `r2_connect()`.

### Step contracts
- **Ingest** is idempotent: files already present in today's raw folder are
  skipped, so a same-day rerun does nothing and a partial run is completed.
  Downloads retry with exponential backoff and check size against
  Content-Length.
- **Transform** reads raw for a fetch date (default latest), validates, writes
  clean Parquet (overwriting that date's clean files), and checks row totals.
  Raise an error on anything unexpected rather than silently dropping rows.
- **Load** reads clean Parquet for a fetch date (default latest) and replaces
  Postgres tables atomically: in one transaction, build `<table>_new` for every
  table, then drop old + rename new. Readers never see partial data; failure
  rolls back cleanly. It logs each load to `pipeline_loads`.

## Postgres tables (CES)

Data currently served: monthly and annual rows from `SERVE_START_YEAR` (2020)
onward; see "Neon storage limit" below. `ces_series_meta` has all series.

**ces_monthly** — PK `(series_id, date)`
| column | type | notes |
|---|---|---|
| series_id | text | e.g. `CES0000000001` |
| date | date | first of month (from period M01–M12) |
| value | numeric | exact decimal; NULL when BLS published `-` (not collected) |
| footnote_codes | text | e.g. `P` = preliminary, `A` = shutdown gap; NULL if none |
| seasonal | text | `S` seasonally adjusted, `U` not adjusted |

**ces_annual** — PK `(series_id, year)`. Same columns, but `year integer`
instead of `date`. Built from period M13 (annual averages).

**ces_series_meta** — PK `series_id`. One row per series from `ce.series`
with names joined in: `series_title, seasonal, supersector_code,
supersector_name, industry_code, naics_code, industry_name, display_level,
publishing_status, data_type_code, data_type_text, footnote_codes, begin_year,
begin_period, end_year, end_period`.

**pipeline_loads** — append-only log: `dataset, fetch_date, loaded_at,
row_counts (jsonb)`. Latest row per dataset = what's being served.

Indexes: only the primary keys. The `(series_id, date)` PK serves
`WHERE series_id = ...` lookups (verified with EXPLAIN); a separate
series_id index would duplicate it and waste storage.

## Conventions
- Read source files with `all_varchar = true`, `trim()` every value, and
  `NULLIF(..., '')`. Never let a CSV reader guess types: codes like
  `00000000` must keep leading zeros. Cast only true numbers (years, values).
- Values: `DECIMAL(18,3)` in Parquet, `numeric` in Postgres (exact). The load
  step strips trailing zeros so Postgres shows `29923`, not `29923.000`.
- Codes and IDs stay `text`. Column names are lowercase snake_case.
- Table names are `{dataset}_{grain}`, e.g. `ces_monthly`, `cps_monthly`.
- Secrets live only in `.env` (never hardcode, never commit). The BLS contact
  email comes from `BLS_CONTACT_EMAIL`.
- Postgres bulk loads use `DATABASE_URL_UNPOOLED` (direct connection), falling
  back to `DATABASE_URL`.
- Plain, commented code. Explicit SQL over abstractions. Each file starts with
  a docstring saying what it does and how to run it.

## Adding a dataset (CPS, ACS, ...)
1. `ingest/{dataset}.py`: download to `raw/{dataset}/{fetch_date}/`, same
   skip-if-present + retry pattern as `ingest/ces.py`.
2. `transform/{dataset}.py`: raw -> `clean/{dataset}/{fetch_date}/*.parquet`
   with validation and row-count checks.
3. `serve/load_{dataset}.py`: copy `TABLES` + the build-then-swap pattern from
   `serve/load_ces.py`; log to `pipeline_loads`.
4. Add the three calls to `run_pipeline.py` (or a dataset argument), and
   document tables here and in README.
5. Check Neon storage first (see below). CPS/ACS microdata will not fit the
   free plan; serve aggregates in Postgres and keep microdata in R2 Parquet.

## Gotchas
- **BLS 403s** without a browser-like User-Agent containing contact info.
  `ingest/ces.py` builds one from `BLS_CONTACT_EMAIL`.
- **BLS gzips responses** unless asked not to. Ingest sends
  `Accept-Encoding: identity` so Content-Length equals the real file size.
- **BLS `-` values**: e.g. Oct 2025 (federal shutdown) has value `-` and
  footnote `A`. These become NULL, keeping the row and footnote.
- **Neon storage limit**: the Free plan caps the project at 0.5 GB; writes fail
  beyond that (the load transaction just rolls back). Measured cost is ~102
  bytes per `ces_monthly` row including the PK index; full CES history is
  ~0.85 GB, and a reload briefly needs old + new copies. Hence
  `SERVE_START_YEAR = 2020` in `serve/load_ces.py` (~0.2 GB, ~0.4 GB peak).
  Set it to `None` after upgrading the plan.
- **Table swap drops dependent views.** The load does `DROP TABLE` on the old
  table. If views are added later (e.g. for the website), recreate them inside
  the load transaction after the swap.
- **R + Postgres**: `RPostgres` does not accept a full URL in `dbname`;
  `R/connect.R` parses `DATABASE_URL`. R's DuckDB build on Windows has no
  `postgres` extension, so in R use RPostgres for Postgres and DuckDB for R2.
  (Python's DuckDB *can* `ATTACH` Postgres.)
- Run Python modules from the project root (`python -m transform.ces`), so the
  `common` import works.
- `neon link` / `neon deploy` rewrite `DATABASE_URL`, `DATABASE_URL_UNPOOLED`,
  and `NEON_BRANCH` in `.env`; other keys are left alone.

## Source file reference (CES)
From https://download.bls.gov/pub/time.series/ce/ (all tab-separated, header row):
- `ce.data.0.AllCESSeries`: series_id, year, period (M01–M13), value, footnote_codes (~8.4M rows)
- `ce.series`: series_id, supersector_code, industry_code, data_type_code, seasonal, series_title, footnote_codes, begin/end year/period (~22K rows)
- `ce.industry`: industry_code, naics_code, publishing_status, industry_name, display_level, selectable, sort_sequence
- `ce.supersector`: supersector_code, supersector_name
- `ce.datatype`: data_type_code, data_type_text
- `ce.period`, `ce.footnote`: lookup tables (stored raw; not yet loaded to Postgres)
