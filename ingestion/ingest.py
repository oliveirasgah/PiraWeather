#!/usr/bin/env python3
"""
Bronze layer ingestion: fetches yearly XLS files from ESALQ and loads
them as-is into PostgreSQL using Citus columnar storage (one table per
source document).

Usage:
    python ingestion/ingest.py [--env dev|prod] [--year N] [--force]
"""

import argparse
import io
import re
import sys
from datetime import UTC, date, datetime

import numpy as np
import pandas as pd
import psycopg2
from pydantic_settings import BaseSettings, SettingsConfigDict


SOURCE_URL = "http://www.leb.esalq.usp.br/leb/automatica/diario{year}.xls"
FIRST_YEAR = 1997


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str
    postgres_password: str
    postgres_db: str = "piraweather"


settings = Settings()


def _get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname=settings.postgres_db,
    )


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

    section["_source_year"] = str(year)
    section["_source_url"] = url
    section["_ingested_at"] = datetime.now(UTC).isoformat()
    return section


#
# Storage
#


def _write_table(cur, table_name: str, df: pd.DataFrame) -> None:
    """Drop, recreate as columnar, and bulk-load a bronze table via COPY."""
    full_name = f"bronze.{table_name}"
    cur.execute(f"DROP TABLE IF EXISTS {full_name}")
    cols_ddl = ", ".join(f'"{col}" TEXT' for col in df.columns)
    cur.execute(f"CREATE EXTENSION IF NOT EXISTS citus_columnar; CREATE TABLE {full_name} ({cols_ddl}) USING columnar")
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert(f"COPY {full_name} FROM STDIN WITH (FORMAT CSV, NULL '')", buf)


#
# Core ingestion
#


def ingest_year(
    con: psycopg2.extensions.connection,
    year: int,
    force: bool,
) -> None:
    current_year = date.today().year
    url = SOURCE_URL.format(year=year)
    tables = ["raw_2016_s1", "raw_2016_s2"] if year == 2016 else [f"raw_{year}"]

    # Incremental: skip completed past years that are already loaded
    if not force and year < current_year:
        with con.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'bronze'"
            )
            existing = {row[0] for row in cur.fetchall()}
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

    try:
        with con.cursor() as cur:
            if year == 2016:
                # Section 1 — era-2 format (rows 17 → 6094)
                section_1 = load_section(
                    data_raw, header_idx=14, row_start=17, row_end=6095, year=year, url=url
                )
                _write_table(cur, "raw_2016_s1", section_1)
                print(f"         raw_2016_s1: {len(section_1)} rows")

                # Section 2 — era-3 format (rows 6102 → end)
                section_2 = load_section(
                    data_raw, header_idx=6099, row_start=6102, row_end=None, year=year, url=url
                )
                _write_table(cur, "raw_2016_s2", section_2)
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
                _write_table(cur, f"raw_{year}", dataframe)
                print(f"         raw_{year}: {len(dataframe)} rows")
        con.commit()
    except Exception as e:
        con.rollback()
        print(f"  {year}: DB ERROR — {e}", file=sys.stderr)


def ingest(years: list[int] | None = None, force: bool = False) -> None:
    print(f"Database    : {settings.postgres_host}/{settings.postgres_db}")
    print(f"Force reload: {force}\n")

    con = _get_conn()
    try:
        for year in years or range(FIRST_YEAR, date.today().year + 1):
            ingest_year(con, year, force=force)
    finally:
        con.close()
    print("\nDone.")


# Main execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PiraWeather bronze ingestion")
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="prod",
        help="Environment label (passed through by run.sh to select dbt target)",
    )
    parser.add_argument("--year", type=int, help="Ingest a single year only")
    parser.add_argument(
        "--force", action="store_true", help="Re-ingest already loaded years"
    )
    args = parser.parse_args()

    ingest(
        years=[args.year] if args.year else None,
        force=args.force,
    )
