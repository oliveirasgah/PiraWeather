"""
PiraWeather API — serves silver and gold layer data from PostgreSQL.

Bronze data is exposed via /bronze/years for the dashboard's explorer tab.
Silver and gold endpoints return empty results until dbt models are added.
"""

import re
from contextlib import asynccontextmanager
from typing import Annotated

import psycopg2
import psycopg2.pool
from fastapi import Depends, FastAPI, HTTPException
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str
    postgres_password: str
    postgres_db: str = "piraweather"


settings = Settings()
_pool: psycopg2.pool.ThreadedConnectionPool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    try:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=settings.postgres_host,
            port=settings.postgres_port,
            user=settings.postgres_user,
            password=settings.postgres_password,
            dbname=settings.postgres_db,
        )
    except Exception as e:
        print(f"WARNING: Could not connect to database: {e}")
    yield
    if _pool:
        _pool.closeall()


app = FastAPI(title="PiraWeather API", version="0.1.0", lifespan=lifespan)


def _get_conn():
    if _pool is None:
        raise HTTPException(503, detail="Database not available")
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)


DB = Annotated[psycopg2.extensions.connection, Depends(_get_conn)]


def _valid_identifier(name: str) -> str:
    """Reject names that aren't safe SQL identifiers."""
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise HTTPException(400, detail=f"Invalid identifier: {name!r}")
    return name


def _list_tables(conn: psycopg2.extensions.connection, schema: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (schema,),
        )
        return [row[0] for row in cur.fetchall()]


def _fetch_rows(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    limit: int,
    offset: int,
) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        )
        if cur.fetchone() is None:
            raise HTTPException(404, detail=f"Table {schema}.{table} not found")
        cur.execute(
            f'SELECT * FROM {schema}."{table}" LIMIT %s OFFSET %s',
            (limit, offset),
        )
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return {"columns": cols, "rows": rows, "count": len(rows)}


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "db_connected": _pool is not None}


@app.get("/bronze/years")
def bronze_years(conn: DB):
    tables = _list_tables(conn, "bronze")
    # Extract unique year strings from names like raw_1997, raw_2016_s1
    years = sorted({t.split("_")[1] for t in tables if t.startswith("raw_")})
    return {"years": years}


@app.get("/bronze/stats")
def bronze_stats(conn: DB):
    """Row counts per bronze table — used by the dashboard overview."""
    tables = _list_tables(conn, "bronze")
    stats = []
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f'SELECT COUNT(*) FROM bronze."{table}"')
            stats.append({"table": table, "rows": cur.fetchone()[0]})
    return {"tables": stats}


@app.get("/silver/tables")
def silver_tables(conn: DB):
    return {"tables": _list_tables(conn, "silver")}


@app.get("/gold/tables")
def gold_tables(conn: DB):
    return {"tables": _list_tables(conn, "gold")}


@app.get("/silver/{table}")
def silver_table(table: str, conn: DB, limit: int = 100, offset: int = 0):
    return _fetch_rows(conn, "silver", _valid_identifier(table), limit, offset)


@app.get("/gold/{table}")
def gold_table(table: str, conn: DB, limit: int = 100, offset: int = 0):
    return _fetch_rows(conn, "gold", _valid_identifier(table), limit, offset)
