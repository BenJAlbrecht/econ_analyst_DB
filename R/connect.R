# Connection helpers for analysis in R.
#
# Usage (with the working directory set to the project folder):
#   source("R/connect.R")
#   pg   <- neon_connect()   # Postgres: latest data, ready-to-query tables
#   duck <- r2_connect()     # DuckDB: Parquet files in R2 (full history, every vintage)
#
# Needs the DBI, RPostgres and duckdb packages:
#   install.packages(c("DBI", "RPostgres", "duckdb"))

library(DBI)

# Load .env (R2 keys, DATABASE_URL) into environment variables.
readRenviron(".env")

# Connect to Neon Postgres. RPostgres can't take a full "postgresql://..." URL,
# so we split DATABASE_URL into user, password, host, and database name.
neon_connect <- function(url = Sys.getenv("DATABASE_URL")) {
  parts <- regmatches(url, regexec("^postgres(?:ql)?://([^:]+):([^@]+)@([^/]+)/([^?]+)", url, perl = TRUE))[[1]]
  if (length(parts) != 5) stop("DATABASE_URL is missing or not a postgresql:// URL")
  dbConnect(
    RPostgres::Postgres(),
    user     = URLdecode(parts[2]),
    password = URLdecode(parts[3]),
    host     = parts[4],
    dbname   = parts[5],
    sslmode  = "require"
  )
}

# Open an in-memory DuckDB database that can read r2://econ-raw/... paths.
r2_connect <- function() {
  con <- dbConnect(duckdb::duckdb())
  dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
  dbExecute(con, sprintf(
    "CREATE SECRET r2 (TYPE r2, KEY_ID '%s', SECRET '%s', ACCOUNT_ID '%s')",
    Sys.getenv("R2_KEY_ID"), Sys.getenv("R2_SECRET"), Sys.getenv("R2_ACCOUNT_ID")
  ))
  con
}
