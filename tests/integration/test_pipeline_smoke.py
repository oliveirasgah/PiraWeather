"""End-to-end smoke test against a real Postgres.

Proves that:
  1. ingest_year() can write a fresh bronze table to a real Citus columnar DB
     using the production code path (DDL + COPY).
  2. The FastAPI read endpoints serve that table correctly when pointed at
     the same DB.

Mocks are limited to the ESALQ HTTP fetch (replaced with a local fixture
XLS) and the runtime postgres_db setting. Everything else is real.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import psycopg2
import pytest
from fastapi.testclient import TestClient

from ingestion import ingest as ingest_mod
from ingestion.ingest import ingest_year


pytestmark = pytest.mark.integration


@pytest.fixture
def patched_settings(test_db_params, monkeypatch):
    """Point ingestion + API at the test DB instead of `piraweather`."""
    monkeypatch.setattr(ingest_mod.settings, "postgres_db", test_db_params["dbname"])
    monkeypatch.setattr(ingest_mod.settings, "postgres_host", test_db_params["host"])
    return test_db_params


@pytest.fixture
def fixture_read_excel(fixture_xlsx_path):
    """Replace pd.read_excel with a function that always returns the fixture."""
    real_read = pd.read_excel

    def fake_read_excel(_url, *args, **kwargs):
        return real_read(fixture_xlsx_path, *args, **kwargs)

    with patch.object(ingest_mod.pd, "read_excel", side_effect=fake_read_excel) as m:
        yield m


def test_ingest_year_writes_bronze_table(patched_settings, fixture_read_excel):
    con = psycopg2.connect(**patched_settings)
    try:
        ingest_year(con, year=2020, force=True)
    finally:
        con.close()

    verify = psycopg2.connect(**patched_settings)
    try:
        with verify.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema='bronze' AND table_name='raw_2020'"
            )
            assert cur.fetchone()[0] == 1, "bronze.raw_2020 not created"

            cur.execute("SELECT count(*) FROM bronze.raw_2020")
            assert cur.fetchone()[0] == 3, "expected 3 fixture rows in bronze.raw_2020"

            # Round-trip a known data value end-to-end through CSV→COPY→TEXT cast.
            # bronze stores everything as TEXT; pandas' astype(str) drops the
            # trailing .0 for whole floats, so 25.0 round-trips as "25".
            cur.execute(
                'SELECT "Tar_AVG" FROM bronze.raw_2020 '
                'WHERE "TIMESTAMP" = %s',
                ("2020-06-01 13:00:00",),
            )
            assert float(cur.fetchone()[0]) == 25.0
    finally:
        verify.close()


def test_api_serves_ingested_year(patched_settings, fixture_read_excel):
    """After ingestion, the FastAPI bronze endpoints must reflect the new table."""
    con = psycopg2.connect(**patched_settings)
    try:
        ingest_year(con, year=2020, force=True)
    finally:
        con.close()

    # Force the API's lifespan to build a fresh pool against the test DB.
    from api import main as api_main

    monkeypatched = {}
    monkeypatched["postgres_db"] = api_main.settings.postgres_db
    monkeypatched["postgres_host"] = api_main.settings.postgres_host
    api_main.settings.postgres_db = patched_settings["dbname"]
    api_main.settings.postgres_host = patched_settings["host"]
    try:
        with TestClient(api_main.app) as client:
            years_resp = client.get("/bronze/years")
            assert years_resp.status_code == 200
            assert "2020" in years_resp.json()["years"]

            stats_resp = client.get("/bronze/stats")
            assert stats_resp.status_code == 200
            tables = {row["table"]: row["rows"] for row in stats_resp.json()["tables"]}
            assert tables.get("raw_2020") == 3
    finally:
        api_main.settings.postgres_db = monkeypatched["postgres_db"]
        api_main.settings.postgres_host = monkeypatched["postgres_host"]
