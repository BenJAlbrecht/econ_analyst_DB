"""Run the CES pipeline end to end: ingest -> transform -> load.

Usage (from the project folder, with the virtual environment active):
    python run_pipeline.py

Each step can also be run on its own:
    python -m ingest.ces
    python -m transform.ces [fetch_date]
    python -m serve.load_ces [fetch_date]
"""

import time

from ingest import ces as ingest_ces
from serve import load_ces
from transform import ces as transform_ces


def main() -> None:
    started = time.time()
    fetch_date = ingest_ces.run()
    transform_ces.run(fetch_date)
    load_ces.run(fetch_date)
    print(f"[pipeline] CES done for fetch date {fetch_date} in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
