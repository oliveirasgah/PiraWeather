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
    {{ safe_float('"Tar_AVG"') }}                as "Tar_AVG",
    {{ safe_float('"UR_inst"') }}                as "UR_inst",
    {{ safe_float('"Vvento_ms_AVG"') }}          as "Vvento_ms_AVG",
    {{ safe_float('"Dvento_G"') }}               as "Dvento_G",
    {{ safe_float('"Qg_AVG"') }}                 as "Qg_AVG",
    {{ safe_float('"PAR_AVG"') }}                as "PAR_AVG",
    {{ safe_float('"Rn_Avg"') }}                 as "Rn_Avg",
    {{ safe_float('"Chuva_mm"') }}               as "Chuva_mm",
    {{ safe_float('"Dvento_SD1_WVT"') }}         as "Dvento_SD1_WVT",
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
    {{ safe_float('"Tar_AVG"') }}                as "Tar_AVG",
    {{ safe_float('"UR_inst"') }}                as "UR_inst",
    {{ safe_float('"Vvento_ms_AVG"') }}          as "Vvento_ms_AVG",
    {{ safe_float('"Dvento_G"') }}               as "Dvento_G",
    {{ safe_float('"Qg_AVG"') }}                 as "Qg_AVG",
    {{ safe_float('"PAR_AVG"') }}                as "PAR_AVG",
    {{ safe_float('"Rn_Avg"') }}                 as "Rn_Avg",
    {{ safe_float('"Chuva_mm"') }}               as "Chuva_mm",
    {{ safe_float('"Dvento_SD1_WVT"') }}         as "Dvento_SD1_WVT",
    {{ safe_float('"BattV_Avg"') }}              as "BattV_Avg",
    {{ safe_float('"Patm_kPa_AVG"') }}           as "Patm_kPa_AVG",
    {{ safe_float('"rQg_AVG"') }}                as "rQg_AVG",
    {{ safe_float('"Qatm_AVG"') }}               as "Qatm_AVG",
    {{ safe_float('"Qsup_AVG"') }}               as "Qsup_AVG",
    {{ safe_float('"Boc_AVG"') }}                as "Boc_AVG",
    {{ safe_float('"Bol_AVG"') }}                as "Bol_AVG",
    {{ safe_float('"Albedo_Avg"') }}             as "Albedo_Avg",
    {{ safe_float('"QatmC_AVG"') }}              as "QatmC_AVG",
    {{ safe_float('"QsupC_AVG"') }}              as "QsupC_AVG",
    {{ safe_float('"Vvento_ms_S_WVT"') }}        as "Vvento_ms_S_WVT",
    {{ safe_float('"Dvento_D1_WVT"') }}          as "Dvento_D1_WVT",
    {{ safe_float('"PainelT"') }}                as "PainelT",
    cast(_source_year       as integer)          as _source_year,
    _source_url
from bronze."raw_{{ year }}"
where "TIMESTAMP" is not null
  and trim("TIMESTAMP") not in ('', 'NaN', 'nan', 'None')
{% endfor %}
