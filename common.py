"""Shared helpers used by every pipeline step.

- Loads settings from .env (R2 keys, BLS contact email, DATABASE_URL).
- Creates an R2 client. R2 is Cloudflare's file storage; it speaks the same
  protocol as Amazon S3, so we use boto3 (the standard S3 library) to talk to it.
- Creates a DuckDB connection that can read and write files in R2 directly
  using r2://econ-raw/... paths.
"""

import os
from pathlib import Path

import boto3
import duckdb
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

R2_BUCKET = "econ-raw"


def r2_client():
    """Return a boto3 S3 client pointed at our Cloudflare R2 account."""
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET"],
        region_name="auto",
    )


def list_r2_keys(prefix: str) -> list[str]:
    """List every object key in the bucket that starts with `prefix`."""
    paginator = r2_client().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def list_r2_dates(prefix: str) -> list[str]:
    """Return the sorted fetch-date folder names under a prefix like 'raw/ces/'."""
    dates = {key[len(prefix):].split("/")[0] for key in list_r2_keys(prefix)}
    return sorted(d for d in dates if d)


def duckdb_connect() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection that can read/write r2:// paths."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE SECRET r2 (
            TYPE r2,
            KEY_ID '{os.environ["R2_KEY_ID"]}',
            SECRET '{os.environ["R2_SECRET"]}',
            ACCOUNT_ID '{os.environ["R2_ACCOUNT_ID"]}'
        )
    """)
    return con
