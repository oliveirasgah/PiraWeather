# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Stack

- **Database:** PostgreSQL (Citus Columnar) — single store for bronze/silver/gold schemas. Bronze tables use `USING columnar`; silver/gold are partitioned/regular Postgres tables and views.
- **Pipeline:** Python `ingestion/ingest.py` (psycopg2 COPY) → dbt (`dbt-postgres==1.9.*`).
- **API:** FastAPI on :8000 (`api/main.py`), reads silver/gold; exposes bronze listing.
- **Dashboard:** Streamlit on :8501 (`dashboard/app.py`), reads bronze directly, hits API for silver/gold.
- **Orchestration:** Docker Compose; `entrypoint.sh` runs `run.sh` once on container start, then schedules `0 6 * * * UTC` via cron.

## Common commands

```bash
# Local end-to-end (dev target, uses localhost Postgres)
./run.sh --env dev                        # ingest + dbt run
./run.sh --env prod --force               # in-container target + re-ingest loaded years

# Ingestion only
python ingestion/ingest.py --year 2020    # one year
python ingestion/ingest.py --force        # all years, re-ingest

# dbt (--profiles-dir . is required; profiles.yml lives in repo root)
dbt deps --profiles-dir .                 # one-time after cloning
dbt run --profiles-dir . --target dev
dbt build --profiles-dir . --target dev   # run + tests (L2 + L3)
dbt run --profiles-dir . --select silver.weather_data+  # rebuild one model and downstream

# Tests — five layers (see README "Testing")
pytest                                    # L1 + L5; default excludes integration
pytest tests/test_ingestion.py::test_name # single test
pytest -m integration                     # L4, needs `docker compose up -d postgres`
bandit -r ingestion api dashboard -ll     # L5 static scan

# Docker stack
docker compose up -d postgres             # just the DB (for local pytest -m integration)
docker compose up                         # full stack with dev hot-reload overrides
docker compose -f docker-compose.yml up   # production-mode (no bind mounts, no --reload)
docker logs pipeline                      # tails cron log (entrypoint.sh streams it to stdout)
```

The dev `docker-compose.override.yml` bind-mounts source dirs so changes to `api/main.py`, `dashboard/app.py`, `ingestion/`, `models/`, and `macros/` take effect without rebuilds. The API runs `uvicorn --reload`.

## Architecture

### Medallion layout
- **bronze** — one `bronze.raw_{year}` table per source XLS, written as TEXT columns (no type casting). 2016 is split into `raw_2016_s1` (Era 2 format) and `raw_2016_s2` (Era 3 format) because ESALQ changed schema mid-year.
- **silver** — `silver.weather_data` is the unified, typed, bounds-checked table. Built from three ephemeral models (`era1`, `era2`, `era3`) that NULL-fill columns absent in earlier eras and rename them to Era 3 conventions (canonical schema). Each era model reconstructs `recorded_at` from its era-specific time format and tags `equipment_era`.
- **gold** — analytical views built on silver (`daily_summary`, `daily_climatology`, `extreme_events`, `growing_degree_days`, `reference_et`, `drought_index`).

### Era handling (load-bearing)
Sensor schema changed four times across the station's history. `ingestion/ingest.py:header_row()` returns the right XLS header row by year, and `models/silver/era*.sql` map era-specific raw column names (e.g. `Vento_4`, `Rad_Solar`) onto Era 3 names (`Vvento_ms_AVG`, `Qg_AVG`). When adding silver/gold logic, treat Era 3 names as canonical and use `_source_url`/`equipment_era` to debug provenance. Era 1 and Era 2 lack many columns (Patm, Rn, radiation correctness, etc.) — gold models that need those must handle NULL gracefully or filter to Era 3.

### Custom dbt machinery
- **`macros/generate_schema_name.sql`** — overrides dbt's default `{target}_{custom}` concatenation. Whatever string you set in `+schema:` (bronze/silver/gold) becomes the literal schema name. Don't reintroduce dev-vs-prod schema prefixes without updating this.
- **`macros/materializations/partitioned_table.sql`** — custom materialization used by `silver.weather_data`. Creates a parent `PARTITION BY RANGE (recorded_at)`, child partitions per calendar year from 1997 through `current_year + 1`, BRIN index per partition, then `TRUNCATE` + `INSERT`. The hard-coded column list in the macro must match the SELECT column order in `weather_data.sql` — adding a column to silver means editing both.
- **`macros/safe_float.sql` / `bounded_float.sql`** — every numeric cast in silver must go through `bounded_float(col, lo, hi)`. `safe_float` alone handles `'NaN'`/`'None'`/blank text; bounds are required because sensors emit numeric sentinels (-6999, 7999) that look like valid floats. The same `[lo, hi]` values are duplicated as `dbt_utils.expression_is_true` checks in `models/silver/schema.yml` — keep them in sync.
- **`tests/dbt_singular/`** — dbt's `test-paths` is intentionally pointed at this subdir (not `tests/`) so pytest's `.py` files and dbt's `.sql` tests don't collide.

### Timezone
Source timestamps are local Piracicaba time; Era 1 silver reconstructs them as `AT TIME ZONE 'America/Sao_Paulo'` → TIMESTAMPTZ. `postgres/init.sql` sets the database timezone to America/Sao_Paulo so dashboard SELECTs render in source time. Don't compare TIMESTAMPTZ values against naive timestamps without `AT TIME ZONE`.

### SQL identifier safety
All endpoints/functions that interpolate a table or column name into SQL must go through `_valid_identifier` (`api/main.py:68`) or the `_VALID_IDENT` regex (`dashboard/app.py:88`) — the `[a-zA-Z_][a-zA-Z0-9_]*` allowlist. Ingestion has `_validate_year` (`ingestion/ingest.py:145`) which doubles as the SQL-safety bound for `raw_{year}`. `tests/security/test_sql_injection.py` is the contract: don't loosen the regex without updating it. The `# nosec B608` comments on f-strings mark places where this validation is the mitigation — Bandit will flag any new bare f-string interpolation.

### Test mocking
Root `conftest.py` sets `POSTGRES_USER` / `POSTGRES_PASSWORD` *before* any module-level `Settings()` instantiation, and mocks `streamlit` + `plotly` with passthrough decorators so `dashboard/app.py` can be imported in a plain pytest session. If you add a new module-level Streamlit call to the dashboard, the mock in `conftest.py` may need a matching `side_effect` (see `st.tabs`, `st.columns`, `st.selectbox`).

## What's out of scope
The README's "Production Considerations" section lists deliberate gaps (no auth, root containers, single Postgres role, HTTP fetch of source data, committed `.env`). Don't add hardening for those unless explicitly asked — they're tracked there because they can't be asserted by automated tests.
