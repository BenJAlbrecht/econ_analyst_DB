"""Ingest step for CES (Current Employment Statistics).

Downloads the CES flat files from BLS and uploads them, byte-for-byte
unmodified, to R2 at raw/ces/{fetch_date}/{filename}.

Idempotent: if a file is already in today's raw folder it is not downloaded
again, so re-running on the same day does nothing (or only finishes a run that
failed partway).

Run on its own with:  python -m ingest.ces
"""

import os
import tempfile
import time
from datetime import date
from pathlib import Path

import requests

from common import R2_BUCKET, list_r2_keys, r2_client

BLS_BASE_URL = "https://download.bls.gov/pub/time.series/ce/"

FILES = [
    "ce.series",
    "ce.industry",
    "ce.datatype",
    "ce.supersector",
    "ce.period",
    "ce.footnote",
    "ce.data.0.AllCESSeries",
]

MAX_ATTEMPTS = 5


def bls_headers() -> dict:
    """BLS blocks requests (HTTP 403) that lack a browser-like User-Agent with contact info."""
    email = os.environ["BLS_CONTACT_EMAIL"]
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        f"econ_analyst_DB/1.0 (contact: {email})"
    )
    # "identity" = send the file uncompressed, so Content-Length matches the
    # file size on disk and our truncation check below is meaningful.
    return {"User-Agent": user_agent, "Accept-Encoding": "identity"}


def download_file(url: str, dest: Path) -> None:
    """Download `url` to `dest`, retrying with increasing waits if anything fails."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with requests.get(url, headers=bls_headers(), stream=True, timeout=60) as resp:
                resp.raise_for_status()
                expected_size = int(resp.headers.get("Content-Length", -1))
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            # A dropped connection can leave a truncated file without raising an error.
            actual_size = dest.stat().st_size
            if expected_size != -1 and actual_size != expected_size:
                raise IOError(f"got {actual_size} bytes, expected {expected_size}")
            return
        except (requests.RequestException, IOError) as err:
            if attempt == MAX_ATTEMPTS:
                raise
            wait = 2 ** attempt  # 2, 4, 8, 16 seconds
            print(f"  attempt {attempt} failed ({err}); retrying in {wait}s")
            time.sleep(wait)


def run(fetch_date: str | None = None) -> str:
    """Fetch any CES files missing from raw/ces/{fetch_date}/. Returns the fetch date."""
    fetch_date = fetch_date or date.today().isoformat()
    prefix = f"raw/ces/{fetch_date}/"

    already_there = {key[len(prefix):] for key in list_r2_keys(prefix)}
    missing = [name for name in FILES if name not in already_there]
    if not missing:
        print(f"[ingest] {prefix} already complete in R2; skipping download.")
        return fetch_date

    s3 = r2_client()
    with tempfile.TemporaryDirectory() as tmp:
        for name in missing:
            local_path = Path(tmp) / name
            print(f"[ingest] downloading {name}")
            download_file(BLS_BASE_URL + name, local_path)
            size_mb = local_path.stat().st_size / 1e6
            print(f"[ingest] uploading {name} ({size_mb:.1f} MB) -> r2://{R2_BUCKET}/{prefix}{name}")
            s3.upload_file(str(local_path), R2_BUCKET, prefix + name)

    print(f"[ingest] done: {len(missing)} file(s) uploaded to {prefix}")
    return fetch_date


if __name__ == "__main__":
    run()
