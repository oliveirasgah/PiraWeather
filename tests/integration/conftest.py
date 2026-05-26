"""Session-scoped fixtures for integration tests.

These tests connect to a real Postgres (the docker-compose `postgres`
service). The fixtures here drop and recreate a `piraweather_test` database
per session and apply `postgres/init.sql` so the schemas + Citus extension
are present.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2
import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
INIT_SQL = REPO_ROOT / "postgres" / "init.sql"


def _admin_params() -> dict:
    """Connection params for the maintenance database (used to manage test DB)."""
    return dict(
        host=os.environ.get("POSTGRES_HOST_LOCAL", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ.get("POSTGRES_DB", "piraweather"),
    )


@pytest.fixture(scope="session")
def test_db_params():
    """Drop+create piraweather_test, apply init.sql, yield connection params."""
    test_db = os.environ.get("POSTGRES_TEST_DB", "piraweather_test")

    admin = psycopg2.connect(**_admin_params())
    admin.autocommit = True
    with admin.cursor() as cur:
        # Disconnect any sessions still on the test DB so DROP succeeds.
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (test_db,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{test_db}"')
        cur.execute(f'CREATE DATABASE "{test_db}"')
    admin.close()

    test_params = {**_admin_params(), "dbname": test_db}
    init_con = psycopg2.connect(**test_params)
    init_con.autocommit = True
    with init_con.cursor() as cur:
        cur.execute(INIT_SQL.read_text())
    init_con.close()

    yield test_params

    admin = psycopg2.connect(**_admin_params())
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (test_db,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{test_db}"')
    admin.close()


@pytest.fixture(scope="session")
def fixture_xlsx_path(tmp_path_factory) -> Path:
    """Generate a minimal Era-3-shape XLS in a tmp dir.

    Era 3 (2017–2023): header at row index 6, data starts at index 9. Real
    files have ~30k rows; we only need a handful to prove the round-trip.
    """
    headers = [
        "TIMESTAMP", "Tar_AVG", "UR_inst", "Vvento_ms_AVG", "Dvento_G",
        "Qg_AVG", "PAR_AVG", "Rn_Avg", "Chuva_mm", "Dvento_SD1_WVT",
        "BattV_Avg", "Patm_kPa_AVG", "rQg_AVG", "Qatm_AVG", "Qsup_AVG",
        "Boc_AVG", "Bol_AVG", "Albedo_Avg", "QatmC_AVG", "QsupC_AVG",
        "Vvento_ms_S_WVT", "Dvento_D1_WVT", "PainelT",
    ]
    n = len(headers)
    # pd.read_excel(default) consumes file-row 0 as the column-index, so
    # everything we want at DF iloc K must sit at file-row K+1. Prepend an
    # all-None pseudo-header to absorb that consumption.
    pseudo_header = [[None] * n]
    pre_header = [[None] * n] * 6
    units_or_blank = [[None] * n] * 2
    data = [
        ["2020-06-01 12:00:00", 24.5, 65, 2.1, 180, 800, 1500, 400, 0,
         0.5, 12.5, 94.5, 100, 300, 350, 10, 5, 0.3, 320, 360, 2.0, 180, 30],
        ["2020-06-01 13:00:00", 25.0, 60, 2.5, 190, 850, 1600, 420, 0,
         0.5, 12.5, 94.5, 110, 310, 360, 12, 6, 0.3, 330, 370, 2.4, 190, 31],
        ["2020-06-01 14:00:00", 26.0, 55, 2.8, 200, 900, 1700, 440, 0,
         0.5, 12.5, 94.5, 120, 320, 370, 14, 7, 0.3, 340, 380, 2.7, 200, 32],
    ]
    rows = pseudo_header + pre_header + [headers] + units_or_blank + data
    df = pd.DataFrame(rows)
    out = tmp_path_factory.mktemp("xls") / "diario2020.xlsx"
    df.to_excel(out, index=False, header=False, engine="openpyxl")
    return out
