/*
  silver.measurements
  ────────────────────
  Unified meteorological observations from 1997 to present.
  Materialized as a PostgreSQL PARTITION BY RANGE ("TIMESTAMP") table
  with one child partition per calendar year (handled by the custom
  partitioned_table materialization in macros/materializations/).

  Column schema follows Era 3 (2017+) — the most complete equipment version.
  Older eras have NULL for columns that didn't exist yet (e.g. BattV_Avg,
  Patm_kPa_AVG and other Era-3-only sensors).

  To add a new year: just run  dbt run --select silver  — stg_era3 picks up
  the new bronze.raw_YYYY table automatically via its Jinja year loop.
*/
{{ config(materialized='partitioned_table') }}

select * from {{ ref('stg_era1') }}
union all
select * from {{ ref('stg_era2') }}
union all
select * from {{ ref('stg_era3') }}
