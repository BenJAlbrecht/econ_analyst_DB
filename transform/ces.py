"""Transform step for CES.

Reads the raw BLS flat files for one fetch date from R2, cleans them with
DuckDB, and writes three Parquet files back to R2 at clean/ces/{fetch_date}/:

  ces_monthly.parquet      series_id, date, value, footnote_codes, seasonal
  ces_annual.parquet       series_id, year, value, footnote_codes, seasonal
  ces_series_meta.parquet  one row per series, with industry/supersector/
                           data-type names joined in

Parquet is a compressed, typed, column-oriented file format; R (arrow, duckdb)
and Python (pandas, polars, duckdb) can all read it directly.

Cleaning rules:
  - Every column is read as text first, so codes like "00000000" keep their
    leading zeros. Only year and value are converted to numbers.
  - All values are whitespace-trimmed (BLS pads its columns with spaces).
  - Periods M01-M12 become the first day of that month; M13 (annual average)
    goes to the annual table.
  - A value of "-" (data not collected, e.g. Oct 2025 shutdown) becomes NULL;
    the footnote code on that row says why.

Re-running for the same fetch date overwrites that date's clean files.

Run on its own with:  python -m transform.ces [fetch_date]
"""

import sys

from common import R2_BUCKET, duckdb_connect, list_r2_dates

# Values are stored as exact decimals with up to 3 decimal places (BLS uses at
# most 2 today). The check in validate() stops the run if that ever changes.
VALUE_TYPE = "DECIMAL(18,3)"


def read_bls_file(con, path: str, table: str) -> None:
    """Load a tab-separated BLS file into a DuckDB table: all text, all trimmed."""
    con.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_csv('{path}', delim = '\t', header = true, all_varchar = true)
    """)
    columns = [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()]
    # trim() the padding; NULLIF turns cells that were only spaces into NULL.
    trimmed = ", ".join(f"NULLIF(trim({c}), '') AS {c}" for c in columns)
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT {trimmed} FROM {table}")


def validate(con) -> None:
    """Stop with an error if the raw data looks different from what we expect."""
    bad_values = con.execute(r"""
        SELECT count(*) FROM raw_data
        WHERE value <> '-' AND NOT regexp_full_match(value, '-?[0-9]+(\.[0-9]{1,3})?')
    """).fetchone()[0]
    if bad_values:
        raise ValueError(f"{bad_values} values are not numbers with <= 3 decimals")

    bad_periods = con.execute(r"""
        SELECT count(*) FROM raw_data WHERE NOT regexp_full_match(period, 'M(0[1-9]|1[0-3])')
    """).fetchone()[0]
    if bad_periods:
        raise ValueError(f"{bad_periods} rows have a period outside M01-M13")

    unknown_series = con.execute("""
        SELECT count(DISTINCT series_id) FROM raw_data
        WHERE series_id NOT IN (SELECT series_id FROM raw_series)
    """).fetchone()[0]
    if unknown_series:
        raise ValueError(f"{unknown_series} series in ce.data are missing from ce.series")


def run(fetch_date: str | None = None) -> str:
    """Build clean Parquet files for `fetch_date` (default: latest raw fetch)."""
    fetch_date = fetch_date or list_r2_dates("raw/ces/")[-1]
    raw = f"r2://{R2_BUCKET}/raw/ces/{fetch_date}/"
    clean = f"r2://{R2_BUCKET}/clean/ces/{fetch_date}/"
    print(f"[transform] reading {raw}")

    con = duckdb_connect()
    read_bls_file(con, raw + "ce.data.0.AllCESSeries", "raw_data")
    read_bls_file(con, raw + "ce.series", "raw_series")
    read_bls_file(con, raw + "ce.industry", "raw_industry")
    read_bls_file(con, raw + "ce.supersector", "raw_supersector")
    read_bls_file(con, raw + "ce.datatype", "raw_datatype")
    validate(con)

    # Shared pieces of the monthly and annual queries.
    value = f"CAST(NULLIF(d.value, '-') AS {VALUE_TYPE}) AS value"
    join_seasonal = "JOIN raw_series s USING (series_id)"

    con.execute(f"""
        COPY (
            SELECT d.series_id,
                   make_date(CAST(d.year AS INTEGER), CAST(substr(d.period, 2) AS INTEGER), 1) AS date,
                   {value},
                   d.footnote_codes,
                   s.seasonal
            FROM raw_data d {join_seasonal}
            WHERE d.period <> 'M13'
            ORDER BY series_id, date
        ) TO '{clean}ces_monthly.parquet' (FORMAT parquet)
    """)

    con.execute(f"""
        COPY (
            SELECT d.series_id,
                   CAST(d.year AS INTEGER) AS year,
                   {value},
                   d.footnote_codes,
                   s.seasonal
            FROM raw_data d {join_seasonal}
            WHERE d.period = 'M13'
            ORDER BY series_id, year
        ) TO '{clean}ces_annual.parquet' (FORMAT parquet)
    """)

    con.execute(f"""
        COPY (
            SELECT s.series_id,
                   s.series_title,
                   s.seasonal,
                   s.supersector_code,
                   ss.supersector_name,
                   s.industry_code,
                   i.naics_code,
                   i.industry_name,
                   CAST(i.display_level AS INTEGER) AS display_level,
                   i.publishing_status,
                   s.data_type_code,
                   dt.data_type_text,
                   s.footnote_codes,
                   CAST(s.begin_year AS INTEGER) AS begin_year,
                   s.begin_period,
                   CAST(s.end_year AS INTEGER) AS end_year,
                   s.end_period
            FROM raw_series s
            LEFT JOIN raw_industry i USING (industry_code)
            LEFT JOIN raw_supersector ss USING (supersector_code)
            LEFT JOIN raw_datatype dt USING (data_type_code)
            ORDER BY s.series_id
        ) TO '{clean}ces_series_meta.parquet' (FORMAT parquet)
    """)

    # Sanity check: every raw data row landed in exactly one of the two tables.
    raw_rows = con.execute("SELECT count(*) FROM raw_data").fetchone()[0]
    monthly_rows = con.execute(f"SELECT count(*) FROM '{clean}ces_monthly.parquet'").fetchone()[0]
    annual_rows = con.execute(f"SELECT count(*) FROM '{clean}ces_annual.parquet'").fetchone()[0]
    meta_rows = con.execute(f"SELECT count(*) FROM '{clean}ces_series_meta.parquet'").fetchone()[0]
    if monthly_rows + annual_rows != raw_rows:
        raise ValueError(f"row mismatch: {monthly_rows} + {annual_rows} != {raw_rows} raw rows")

    print(f"[transform] wrote {clean}: monthly={monthly_rows:,} annual={annual_rows:,} series={meta_rows:,}")
    return fetch_date


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else None)
