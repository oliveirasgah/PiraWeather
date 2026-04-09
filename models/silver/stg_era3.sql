/*
  stg_era3 — Era 3: raw_2016_s2 and 2017–present
  ─────────────────────────────────────────────────
  Column names already match the target schema — only TEXT → typed casts needed.
  The TIMESTAMP column is a parseable string; no reconstruction required.

  raw_2016_s2: second section of the 2016 XLS (rows after the station upgrade),
  already in Era 3 format.

  New years are included automatically at compile time via the Jinja loop
  up to modules.datetime.date.today().year — no manual updates needed when a
  new bronze table is added.

  Note: raw_2016_s2 may lack some Era 3-only columns (e.g. Patm_kPa_AVG).
  If dbt raises a column-not-found error for that table, add a fallback
  select that casts NULL for the missing columns.
*/
{{ config(materialized='ephemeral') }}

{% set era3_start = 2017 %}
{% set current_year = modules.datetime.date.today().year %}

with src as (
    select * from bronze."raw_2016_s2"
    {% for year in range(era3_start, current_year + 1) %}
    union all select * from bronze."raw_{{ year }}"
    {% endfor %}
)

select
    cast("TIMESTAMP"        as timestamptz)             as "TIMESTAMP",
    'Era 3'                                             as equipment_era,
    cast("Tar_AVG"          as double precision)        as "Tar_AVG",
    cast("UR_inst"          as double precision)        as "UR_inst",
    cast("Vvento_ms_AVG"    as double precision)        as "Vvento_ms_AVG",
    cast("Dvento_G"         as double precision)        as "Dvento_G",
    cast("Qg_AVG"           as double precision)        as "Qg_AVG",
    cast("PAR_AVG"          as double precision)        as "PAR_AVG",
    cast("Rn_Avg"           as double precision)        as "Rn_Avg",
    cast("Chuva_mm"         as double precision)        as "Chuva_mm",
    cast("Dvento_SD1_WVT"   as double precision)        as "Dvento_SD1_WVT",
    cast("BattV_Avg"        as double precision)        as "BattV_Avg",
    cast("Patm_kPa_AVG"     as double precision)        as "Patm_kPa_AVG",
    cast("rQg_AVG"          as double precision)        as "rQg_AVG",
    cast("Qatm_AVG"         as double precision)        as "Qatm_AVG",
    cast("Qsup_AVG"         as double precision)        as "Qsup_AVG",
    cast("Boc_AVG"          as double precision)        as "Boc_AVG",
    cast("Bol_AVG"          as double precision)        as "Bol_AVG",
    cast("Albedo_Avg"       as double precision)        as "Albedo_Avg",
    cast("QatmC_AVG"        as double precision)        as "QatmC_AVG",
    cast("QsupC_AVG"        as double precision)        as "QsupC_AVG",
    cast("Vvento_ms_S_WVT"  as double precision)        as "Vvento_ms_S_WVT",
    cast("Dvento_D1_WVT"    as double precision)        as "Dvento_D1_WVT",
    cast("PainelT"          as double precision)        as "PainelT",
    cast(_source_year       as integer)                 as _source_year,
    _source_url
from src
where "TIMESTAMP" is not null
  and trim("TIMESTAMP") not in ('', 'NaN', 'nan', 'None')
