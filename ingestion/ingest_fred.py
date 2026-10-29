"""
Ingest FRED economic series into the raw schema of a DuckDB warehouse.

This script:
  1. Creates raw.fred_observations and raw.fred_series if they don't exist.
  2. For each series, finds the latest date already stored and fetches only newer data, plus a lookback window 
  to capture FRED's revisions.
  3. Upserts observations (updates existing rows, inserts new ones).
  4. Refreshes each series' metadata (title, units, frequency, etc.).

Run by using this command: python ingestion/ingest_fred.py
"""

import logging
import os
import sys
import time
from datetime import UTC, datetime, timedelta

import duckdb
import pandas as pd
import requests
from dotenv import load_dotenv

'''
Configuration ---- 
'''
load_dotenv() 

API_KEY = os.environ.get("FRED_API_KEY")
BASE_URL = "https://api.stlouisfed.org/fred"
DB_PATH = os.getenv("DUCKDB_PATH", "fred.duckdb")

# reference: https://browser-use.com/skills/fred 
SERIES = [
    "CPIAUCSL",       # CPI (all urban consumers)
    "CES0500000003",  # Average hourly earnings, total private
    "UNRATE", # Unemployment rate
    "FEDFUNDS", # Effective federal funds rate 
    "PAYEMS", # Total nonfarm payrolls
    "GDP", # Gross domestic product (quarterly basis)
    "MAUR", # MA unemployment rate
    "CAUR", # CA unemployment rate
    "TXUR", # TX unemployment rate
    "NYUR", # NY unemployment rate
]

DEFAULT_START = "1990-01-01" 
LOOKBACK_DAYS = 180 # look back 6ish months each run to capture revisions
REQUEST_PAUSE_SECONDS = 0.6 # fall back to stay under FRED's rate limit

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("ingest_fred")

'''
API Helper ---- 
'''
def fred_get(endpoint: str, **params) -> dict:
    """Call a FRED endpoint and return parsed JSON, retrying on rate limits."""
    params.update(api_key=API_KEY, file_type="json")
    url = f"{BASE_URL}/{endpoint}"

    # limit to 3 attempts
    for attempt in range(3):
        resp = requests.get(url, params=params, timeout=30)

        # 429 ("too many requests"), wait and try again but double the wait each time (5s, 10s, 20s, etc).
        if resp.status_code == 429:
            wait = 5 * (2 ** attempt)
            log.warning("Rate limited on %s, retrying in %ss", endpoint, wait)
            time.sleep(wait)
            continue
        # If any other error status:
        resp.raise_for_status()
        return resp.json()

    # Only reached if everything fails
    raise RuntimeError(f"FRED request to {endpoint} failed after 3 attempts")


'''
DuckDB Helper ---- 
'''
def run_sql(con: duckdb.DuckDBPyConnection, sql: str, params=None) -> list:

    return con.execute(sql, params or []).fetchall()


'''
Warehouse setup ---- 
'''
def setup_tables(con: duckdb.DuckDBPyConnection) -> None:
    # Create the raw schema and tables if they don't already exist
    run_sql(con, "CREATE SCHEMA IF NOT EXISTS raw")


    run_sql(con, """
        CREATE TABLE IF NOT EXISTS raw.fred_observations (
            series_id        VARCHAR NOT NULL,
            observation_date DATE    NOT NULL,
            value            VARCHAR,
            realtime_start   DATE,
            realtime_end     DATE,
            _loaded_at       TIMESTAMP NOT NULL,
            PRIMARY KEY (series_id, observation_date)
        )
    """)

    run_sql(con, """
        CREATE TABLE IF NOT EXISTS raw.fred_series (
            series_id           VARCHAR PRIMARY KEY,
            title               VARCHAR,
            frequency           VARCHAR,
            units               VARCHAR,
            seasonal_adjustment VARCHAR,
            last_updated        VARCHAR,
            _loaded_at          TIMESTAMP NOT NULL
        )
    """)


'''
Incramental Logic ----
'''
def get_start_date(con: duckdb.DuckDBPyConnection, series_id: str) -> str:
    # Return the date to start fetching from for this series.
    latest = run_sql(
        con,
        "SELECT max(observation_date) FROM raw.fred_observations WHERE series_id = ?",
        [series_id],
    )[0][0]

    # If this is the first load, pull the full history.
    if latest is None: 
        return DEFAULT_START

    # Data already exists: start LOOKBACK_DAYS before the latest date, so recent values get fetched again and any revisions are picked up.
    return (latest - timedelta(days=LOOKBACK_DAYS)).isoformat()


'''
Loaders ---
'''
def ingest_observations(
    con: duckdb.DuckDBPyConnection, series_id: str, loaded_at: datetime
) -> int:
    """Fetch observations for one series and upsert them. Returns row count."""
    start = get_start_date(con, series_id)
    data = fred_get(
        "series/observations", series_id=series_id, observation_start=start
    )
    observations = data.get("observations", [])
    if not observations:
        log.warning("%s: no observations returned since %s", series_id, start)
        return 0
    df = pd.DataFrame(observations)
    df["series_id"] = series_id
    df["_loaded_at"] = loaded_at
    con.register("incoming_obs", df)
    run_sql(con, """
        INSERT OR REPLACE INTO raw.fred_observations
        SELECT
            series_id,
            CAST(date AS DATE),
            value,
            CAST(realtime_start AS DATE),
            CAST(realtime_end AS DATE),
            _loaded_at
        FROM incoming_obs
    """)
    con.unregister("incoming_obs")
    log.info("%s: upserted %d rows (from %s)", series_id, len(df), start)
    return len(df)


def ingest_metadata(
    con: duckdb.DuckDBPyConnection, series_id: str, loaded_at: datetime
) -> None:
    # Fetch and upsert descriptive metadata for one series
    info = fred_get("series", series_id=series_id)["seriess"][0]
    run_sql(
        con,
        "INSERT OR REPLACE INTO raw.fred_series VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            series_id,
            info.get("title"),
            info.get("frequency"),
            info.get("units"),
            info.get("seasonal_adjustment"),
            info.get("last_updated"),
            loaded_at,
        ],
    )


def run_ingestion() -> dict:
    # Run the full ingestion and return results
    if not API_KEY:
        raise RuntimeError("FRED_API_KEY is not set. Add it to your .env file.")

    loaded_at = datetime.now(UTC).replace(tzinfo=None)
    con = duckdb.connect(DB_PATH)
    rows_by_series = {}
    failures = []

    try:
        setup_tables(con)
        for series_id in SERIES:
            try:
                ingest_metadata(con, series_id, loaded_at)
                time.sleep(REQUEST_PAUSE_SECONDS)
                rows_by_series[series_id] = ingest_observations(con, series_id, loaded_at)
                time.sleep(REQUEST_PAUSE_SECONDS)
            except (requests.RequestException, duckdb.Error, KeyError, ValueError) as exc:
                log.error("%s: failed with %s", series_id, exc)
                failures.append(series_id)
    finally:
        con.close()

    return {
        "rows_by_series": rows_by_series,
        "total_rows": sum(rows_by_series.values()),
        "failures": failures,
    }


def main() -> int:
    result = run_ingestion()
    log.info(
        "Done. %d rows upserted. Failures: %s",
        result["total_rows"], result["failures"] or "none",
    )
    return 1 if result["failures"] else 0


# Runs only when the file is executed directly (python ingestion/ingest_fred.py)
if __name__ == "__main__":
    sys.exit(main())