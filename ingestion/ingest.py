#!/usr/bin/env python3
"""
Bronze layer ingestion: fetches yearly XLS files from ESALQ and loads
them as-is into DuckDB (one table per source document).

Usage:
    python ingestion/ingest.py --env dev
    python ingestion/ingest.py --env prod
    python ingestion/ingest.py --env dev --year 2024     # single year
    python ingestion/ingest.py --env dev --force         # re-ingest all years
"""

import argparse
import re
import sys
from datetime import UTC, date, datetime

import duckdb
import numpy as np
import pandas as pd


SOURCE_URL = "http://www.leb.esalq.usp.br/leb/automatica/diario{year}.xls"
FIRST_YEAR = 1997

DB_PATHS = {
    "dev": "data/piraweather_dev.duckdb",
    "prod": "data/piraweather_prod.duckdb",
}


#
# Helpers for column related tasks
#


def header_row(year: int) -> int:
    """Row index of the column header in the raw Excel file, by year."""
    if year < 2003:
        return 11
    elif year < 2017:
        return 14
    elif year < 2024:
        return 6
    else:
        return 2


def sanitize_name(column, index: int) -> str:
    """Convert a raw column header to a valid SQL identifier."""
    if column is np.nan or str(column).strip() == "nan":
        return f"_col_{index}"
    sanitized = re.sub(r"[^a-zA-Z0-9]", "_", str(column).strip())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or f"_col_{index}"


def make_columns(raw_columns: list) -> list:
    """Sanitize headers and append positional index to duplicates."""
    sanitized = [sanitize_name(column, i) for i, column in enumerate(raw_columns)]
    counts = {name: sanitized.count(name) for name in sanitized}
    return [
        f"{name}_{i}" if counts[name] > 1 else name for i, name in enumerate(sanitized)
    ]


def load_section(
    data_raw: pd.DataFrame,
    header_idx: int,
    row_start: int,
    row_end: int | None,
    year: int,
    url: str,
) -> pd.DataFrame:
    """Slice the raw Excel sheet into a DataFrame, adding metadata columns."""
    columns = make_columns(data_raw.iloc[header_idx].tolist())

    section = (
        data_raw.iloc[row_start:row_end].copy()
        if row_end is not None
        else data_raw.iloc[row_start:].copy()
    )
    section.columns = columns
    section = section.reset_index(drop=True)

    # cast all raw data columns to string — type casting happens in silver
    data_cols = [c for c in section.columns if not c.startswith("_")]
    for col in data_cols:
        section[col] = section[col].astype(str).replace("nan", None)

    section["_source_year"] = year
    section["_source_url"] = url
    section["_ingested_at"] = datetime.now(UTC).isoformat()
    return section


#
# Core ingestion
#


def ingest_year(
    con: duckdb.DuckDBPyConnection,
    year: int,
    force: bool,
) -> None:
    current_year = date.today().year
    url = SOURCE_URL.format(year=year)
    tables = ["raw_2016_s1", "raw_2016_s2"] if year == 2016 else [f"raw_{year}"]

    # Incremental: skip completed years that are already loaded
    if not force and year < current_year:
        existing = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'bronze'"
            ).fetchall()
        }
        if all(t in existing for t in tables):
            print(f"  {year}: already loaded, skipping")
            return

    print(f"  {year}: fetching {url}")
    try:
        data_raw = pd.read_excel(url)
    except Exception as e:
        print(f"  {year}: ERROR — {e}", file=sys.stderr)
        return

    header_idx = header_row(year)

    if year == 2016:
        # Section 1 — era-2 format (rows 17 → 6094)
        section_1 = load_section(
            data_raw, header_idx=14, row_start=17, row_end=6095, year=year, url=url
        )
        con.execute("DROP TABLE IF EXISTS bronze.raw_2016_s1")
        con.execute("CREATE TABLE bronze.raw_2016_s1 AS SELECT * FROM section_1")
        print(f"         raw_2016_s1: {len(section_1)} rows")

        # Section 2 — era-3 format (rows 6102 → end)
        section_2 = load_section(
            data_raw, header_idx=6099, row_start=6102, row_end=None, year=year, url=url
        )
        con.execute("DROP TABLE IF EXISTS bronze.raw_2016_s2")
        con.execute("CREATE TABLE bronze.raw_2016_s2 AS SELECT * FROM section_2")
        print(f"         raw_2016_s2: {len(section_2)} rows")
    else:
        dataframe = load_section(
            data_raw,
            header_idx=header_idx,
            row_start=header_idx + 3,
            row_end=None,
            year=year,
            url=url,
        )
        table = f"bronze.raw_{year}"
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM dataframe")
        print(f"         raw_{year}: {len(dataframe)} rows")


def ingest(env: str, years: list[int] | None = None, force: bool = False) -> None:
    db_path = DB_PATHS[env]
    print(f"Environment : {env}")
    print(f"Database    : {db_path}")
    print(f"Force reload: {force}\n")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    for year in years or range(FIRST_YEAR, date.today().year + 1):
        ingest_year(con, year, force=force)

    con.close()
    print("\nDone.")


# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PiraWeather bronze ingestion")
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    parser.add_argument("--year", type=int, help="Ingest a single year only")
    parser.add_argument(
        "--force", action="store_true", help="Re-ingest already loaded years"
    )
    args = parser.parse_args()

    ingest(
        env=args.env,
        years=[args.year] if args.year else None,
        force=args.force,
    )
