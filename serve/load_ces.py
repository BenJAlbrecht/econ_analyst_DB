"""Load step for CES: copy the latest clean Parquet files from R2 into Neon Postgres.

Tables:
  ces_monthly      PK (series_id, date)
  ces_annual       PK (series_id, year)
  ces_series_meta  PK (series_id)
  pipeline_loads   one row per successful load (which fetch date is being served)

How the swap works (so readers never see a half-loaded table):
  Everything happens in ONE transaction. For each table we build a new copy
  named <table>_new, fill it, and add its primary key. Only at the end do we
  drop the old table and rename <table>_new into its place. Until COMMIT,
  other connections keep seeing the old tables; after COMMIT they see all
  three new tables at once. If anything fails, the transaction rolls back and
  the old tables are untouched.

Index note: a primary key on (series_id, date) is itself an index whose first
column is series_id, so "WHERE series_id = ..." queries already use it. A
second index on series_id alone would duplicate it and cost storage.

Run on its own with:  python -m serve.load_ces [fetch_date]
"""

import os
import sys
import tempfile
import time
from pathlib import Path

import psycopg

from common import R2_BUCKET, duckdb_connect, list_r2_dates

# Earliest year of monthly data to put in Postgres. None = full history.
# The Neon Free plan caps storage at 0.5 GB, and full CES history needs ~1 GB
# (plus room for the old copy during a reload). Full history is always in the
# R2 Parquet files either way. After upgrading Neon, set this to None.
SERVE_START_YEAR = 2020

# Postgres column definitions for each table, in the same order as the Parquet
# columns. "numeric" stores exact decimals; "text" keeps codes like "00000000".
TABLES = {
    "ces_series_meta": {
        "columns": """
            series_id         text PRIMARY KEY,
            series_title      text,
            seasonal          text NOT NULL,
            supersector_code  text,
            supersector_name  text,
            industry_code     text,
            naics_code        text,
            industry_name     text,
            display_level     integer,
            publishing_status text,
            data_type_code    text,
            data_type_text    text,
            footnote_codes    text,
            begin_year        integer,
            begin_period      text,
            end_year          integer,
            end_period        text
        """,
        "where": "",
    },
    "ces_monthly": {
        "columns": """
            series_id      text NOT NULL,
            date           date NOT NULL,
            value          numeric,
            footnote_codes text,
            seasonal       text NOT NULL,
            PRIMARY KEY (series_id, date)
        """,
        "where": f"WHERE year(date) >= {SERVE_START_YEAR}" if SERVE_START_YEAR else "",
    },
    "ces_annual": {
        "columns": """
            series_id      text NOT NULL,
            year           integer NOT NULL,
            value          numeric,
            footnote_codes text,
            seasonal       text NOT NULL,
            PRIMARY KEY (series_id, year)
        """,
        "where": f"WHERE year >= {SERVE_START_YEAR}" if SERVE_START_YEAR else "",
    },
}


def database_url() -> str:
    # Neon gives two URLs. The "unpooled" one is a direct connection, which is
    # what Neon recommends for long bulk loads; fall back to DATABASE_URL.
    return os.environ.get("DATABASE_URL_UNPOOLED") or os.environ["DATABASE_URL"]


def export_csv(duck, parquet_path: str, where: str, csv_path: Path) -> None:
    """Write a Parquet file (optionally filtered) to a local CSV for Postgres COPY."""
    columns = [row[0] for row in duck.execute(f"DESCRIBE SELECT * FROM '{parquet_path}'").fetchall()]
    # Parquet stores value as DECIMAL(18,3), which prints as "29923.000".
    # Strip the trailing zeros so Postgres stores and shows 29923 / 34.2.
    select = ", ".join(
        "rtrim(rtrim(CAST(value AS VARCHAR), '0'), '.') AS value" if c == "value" else c
        for c in columns
    )
    duck.execute(f"""
        COPY (SELECT {select} FROM '{parquet_path}' {where})
        TO '{csv_path.as_posix()}' (FORMAT csv, HEADER false)
    """)


def copy_csv(cur, table: str, csv_path: Path) -> None:
    """Stream a CSV file into a Postgres table with COPY (much faster than INSERTs)."""
    with cur.copy(f"COPY {table} FROM STDIN (FORMAT csv)") as copy:
        with open(csv_path, "rb") as f:
            while chunk := f.read(8 * 1024 * 1024):
                copy.write(chunk)


def run(fetch_date: str | None = None) -> str:
    """Replace the CES tables in Postgres with clean data from `fetch_date` (default: latest)."""
    fetch_date = fetch_date or list_r2_dates("clean/ces/")[-1]
    clean = f"r2://{R2_BUCKET}/clean/ces/{fetch_date}/"
    print(f"[load] loading {clean} into Postgres (monthly from {SERVE_START_YEAR or 'start'})")

    duck = duckdb_connect()
    row_counts = {}
    with tempfile.TemporaryDirectory() as tmp, psycopg.connect(database_url()) as conn:
        # `with psycopg.connect(...)` wraps everything below in one transaction:
        # it COMMITs if the block finishes and ROLLs BACK if an error is raised.
        cur = conn.cursor()

        # 1. Build and fill <table>_new for every table. Old tables stay readable.
        for table, spec in TABLES.items():
            started = time.time()
            csv_path = Path(tmp) / f"{table}.csv"
            export_csv(duck, f"{clean}{table}.parquet", spec["where"], csv_path)
            cur.execute(f"DROP TABLE IF EXISTS {table}_new")
            cur.execute(f"CREATE TABLE {table}_new ({spec['columns']})")
            copy_csv(cur, f"{table}_new", csv_path)
            cur.execute(f"ANALYZE {table}_new")  # refresh stats the query planner uses
            row_counts[table] = cur.execute(f"SELECT count(*) FROM {table}_new").fetchone()[0]
            print(f"[load] {table}_new: {row_counts[table]:,} rows ({time.time() - started:.0f}s)")

        # 2. Swap: drop each old table and rename the new one into its place.
        #    The primary-key index is renamed too so its name matches the table.
        for table in TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
            cur.execute(f"ALTER INDEX {table}_new_pkey RENAME TO {table}_pkey")

        # 3. Record what is now being served.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_loads (
                dataset    text NOT NULL,
                fetch_date date NOT NULL,
                loaded_at  timestamptz NOT NULL DEFAULT now(),
                row_counts jsonb
            )
        """)
        cur.execute(
            "INSERT INTO pipeline_loads (dataset, fetch_date, row_counts) VALUES ('ces', %s, %s)",
            (fetch_date, psycopg.types.json.Jsonb(row_counts)),
        )

    print(f"[load] committed: Postgres now serves CES fetched {fetch_date}")
    return fetch_date


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else None)
