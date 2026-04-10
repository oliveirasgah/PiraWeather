/*
  stg_era3 — Era 3: raw_2016_s2 and 2017–present
  ─────────────────────────────────────────────────
  Column names already match the target schema — only TEXT → typed casts needed.
  The TIMESTAMP column is a parseable string; no reconstruction required.

  raw_2016_s2 is the second half of the 2016 XLS (after the station upgrade).
  It uses Era 3 column names but predates some sensors added later, so
  Era-3-only columns that may be absent are set to NULL.

  2017+ tables all have the full Era 3 schema. Each year is listed explicitly
  (no SELECT *) so that extra columns added to the source XLS over time don't
  cause UNION column-count mismatches.

  New years are included automatically at compile time via the Jinja loop.
*/
{{ config(materialized='ephemeral') }}

{% set era3_start = 2017 %}
{% set current_year = modules.datetime.date.today().year %}

-- raw_2016_s2: conservative NULL for sensors that may not yet exist
select
    cast("TIMESTAMP"        as timestamptz)     as "TIMESTAMP",
    'Era 3'                                      as equipment_era,
    cast("Tar_AVG"          as double precision) as "Tar_AVG",
    cast("UR_inst"          as double precision) as "UR_inst",
    cast("Vvento_ms_AVG"    as double precision) as "Vvento_ms_AVG",
    cast("Dvento_G"         as double precision) as "Dvento_G",
    cast("Qg_AVG"           as double precision) as "Qg_AVG",
    cast("PAR_AVG"          as double precision) as "PAR_AVG",
    cast("Rn_Avg"           as double precision) as "Rn_Avg",
    cast("Chuva_mm"         as double precision) as "Chuva_mm",
    cast("Dvento_SD1_WVT"   as double precision) as "Dvento_SD1_WVT",
    null::double precision                       as "BattV_Avg",
    null::double precision                       as "Patm_kPa_AVG",
    null::double precision                       as "rQg_AVG",
    null::double precision                       as "Qatm_AVG",
    null::double precision                       as "Qsup_AVG",
    null::double precision                       as "Boc_AVG",
    null::double precision                       as "Bol_AVG",
    null::double precision                       as "Albedo_Avg",
    null::double precision                       as "QatmC_AVG",
    null::double precision                       as "QsupC_AVG",
    null::double precision                       as "Vvento_ms_S_WVT",
    null::double precision                       as "Dvento_D1_WVT",
    null::double precision                       as "PainelT",
    cast(_source_year       as integer)          as _source_year,
    _source_url
from bronze."raw_2016_s2"
where "TIMESTAMP" is not null
  and trim("TIMESTAMP") not in ('', 'NaN', 'nan', 'None')

{% for year in range(era3_start, current_year + 1) %}
union all
select
    cast("TIMESTAMP"        as timestamptz)     as "TIMESTAMP",
    'Era 3'                                      as equipment_era,
    cast("Tar_AVG"          as double precision) as "Tar_AVG",
    cast("UR_inst"          as double precision) as "UR_inst",
    cast("Vvento_ms_AVG"    as double precision) as "Vvento_ms_AVG",
    cast("Dvento_G"         as double precision) as "Dvento_G",
    cast("Qg_AVG"           as double precision) as "Qg_AVG",
    cast("PAR_AVG"          as double precision) as "PAR_AVG",
    cast("Rn_Avg"           as double precision) as "Rn_Avg",
    cast("Chuva_mm"         as double precision) as "Chuva_mm",
    cast("Dvento_SD1_WVT"   as double precision) as "Dvento_SD1_WVT",
    cast("BattV_Avg"        as double precision) as "BattV_Avg",
    cast("Patm_kPa_AVG"     as double precision) as "Patm_kPa_AVG",
    cast("rQg_AVG"          as double precision) as "rQg_AVG",
    cast("Qatm_AVG"         as double precision) as "Qatm_AVG",
    cast("Qsup_AVG"         as double precision) as "Qsup_AVG",
    cast("Boc_AVG"          as double precision) as "Boc_AVG",
    cast("Bol_AVG"          as double precision) as "Bol_AVG",
    cast("Albedo_Avg"       as double precision) as "Albedo_Avg",
    cast("QatmC_AVG"        as double precision) as "QatmC_AVG",
    cast("QsupC_AVG"        as double precision) as "QsupC_AVG",
    cast("Vvento_ms_S_WVT"  as double precision) as "Vvento_ms_S_WVT",
    cast("Dvento_D1_WVT"    as double precision) as "Dvento_D1_WVT",
    cast("PainelT"          as double precision) as "PainelT",
    cast(_source_year       as integer)          as _source_year,
    _source_url
from bronze."raw_{{ year }}"
where "TIMESTAMP" is not null
  and trim("TIMESTAMP") not in ('', 'NaN', 'nan', 'None')
{% endfor %}
