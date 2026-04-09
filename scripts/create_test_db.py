#!/usr/bin/env python3
"""
Create the test database as a snapshot of prod, if it doesn't already exist.

Uses PostgreSQL's CREATE DATABASE ... TEMPLATE which copies both schema and
data. Because this requires no active connections on the source database, the
script terminates all sessions on prod and retries up to 3 times with
exponential backoff.

Called from entrypoint.sh after the initial pipeline run.
"""

import time

import psycopg2
import psycopg2.errors
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str
    postgres_password: str
    postgres_db: str = "piraweather"
    postgres_test_db: str = ""


settings = Settings()


def main() -> None:
    prod_db = settings.postgres_db
    test_db = settings.postgres_test_db or f"{prod_db}_test"

    # Must connect to the postgres meta-database to run CREATE DATABASE
    con = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname="postgres",
    )
    con.autocommit = True  # CREATE DATABASE cannot run inside a transaction

    try:
        with con.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (test_db,))
            if cur.fetchone() is not None:
                print(f"Test database '{test_db}' already exists, skipping.")
                return

            print(f"Creating '{test_db}' from template '{prod_db}'...")
            _create_from_template(cur, prod_db, test_db)
            print(f"Test database '{test_db}' created successfully.")
    finally:
        con.close()


def _create_from_template(cur, prod_db: str, test_db: str) -> None:
    """Attempt CREATE DATABASE TEMPLATE, retrying after clearing connections.

    A TEMPLATE copy requires zero active connections on the source database.
    We terminate ALL non-backend sessions (idle, active, idle in transaction)
    so that connection-pooling services (e.g. the API) don't block the copy.
    """
    for attempt, wait in enumerate([3, 6, 12], start=1):
        try:
            cur.execute(f"CREATE DATABASE {test_db} TEMPLATE {prod_db}")
            return
        except psycopg2.errors.ObjectInUse:
            cur.execute(
                "SELECT COUNT(*), pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (prod_db,),
            )
            remaining = cur.fetchone()[0]
            print(
                f"  '{prod_db}' has {remaining} active connection(s) "
                f"(attempt {attempt}/3). Retrying in {wait}s..."
            )
            time.sleep(wait)

    print(
        f"WARNING: Could not create '{test_db}' — '{prod_db}' still has active "
        "connections after all retries. Create it manually:\n"
        f"  docker compose exec postgres psql -U $POSTGRES_USER -c "
        f"'CREATE DATABASE {test_db} TEMPLATE {prod_db}'"
    )


if __name__ == "__main__":
    main()
