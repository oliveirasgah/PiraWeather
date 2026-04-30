-- Era 3 (raw_2016_s2 post-upgrade + 2017-present).
-- Source columns already match the canonical schema; only typed casts needed.
-- raw_2016_s2 NULLs sensors added after the initial upgrade.
-- New years auto-included via the Jinja loop.
-- TIMESTAMP strings are local Piracicaba time; tagged as America/Sao_Paulo.
{{ config(materialized='ephemeral') }}

{% set era3_start = 2017 %}
{% set current_year = modules.datetime.date.today().year %}

SELECT
  CAST("TIMESTAMP" AS TIMESTAMP) AT TIME ZONE 'America/Sao_Paulo' AS recorded_at,
  'Era 3' AS equipment_era,
  {{ bounded_float('"Tar_AVG"', -5, 50) }} AS "Tar_AVG",
  {{ bounded_float('"UR_inst"', 0, 100) }} AS "UR_inst",
  {{ bounded_float('"Vvento_ms_AVG"', 0, 60) }} AS "Vvento_ms_AVG",
  {{ bounded_float('"Dvento_G"', 0, 360) }} AS "Dvento_G",
  {{ bounded_float('"Qg_AVG"', 0, 1500) }} AS "Qg_AVG",
  {{ bounded_float('"PAR_AVG"', 0, 3000) }} AS "PAR_AVG",
  {{ bounded_float('"Rn_Avg"', -200, 1500) }} AS "Rn_Avg",
  {{ bounded_float('"Chuva_mm"', 0, 200) }} AS "Chuva_mm",
  {{ bounded_float('"Dvento_SD1_WVT"', 0, 360) }} AS "Dvento_SD1_WVT",
  NULL::DOUBLE PRECISION AS "BattV_Avg",
  NULL::DOUBLE PRECISION AS "Patm_kPa_AVG",
  NULL::DOUBLE PRECISION AS "rQg_AVG",
  NULL::DOUBLE PRECISION AS "Qatm_AVG",
  NULL::DOUBLE PRECISION AS "Qsup_AVG",
  NULL::DOUBLE PRECISION AS "Boc_AVG",
  NULL::DOUBLE PRECISION AS "Bol_AVG",
  NULL::DOUBLE PRECISION AS "Albedo_Avg",
  NULL::DOUBLE PRECISION AS "QatmC_AVG",
  NULL::DOUBLE PRECISION AS "QsupC_AVG",
  NULL::DOUBLE PRECISION AS "Vvento_ms_S_WVT",
  NULL::DOUBLE PRECISION AS "Dvento_D1_WVT",
  NULL::DOUBLE PRECISION AS "PainelT",
  _source_url
FROM bronze."raw_2016_s2"
WHERE
  "TIMESTAMP" IS NOT NULL
  AND TRIM("TIMESTAMP") NOT IN ('', 'NaN', 'nan', 'None')
  AND "TIMESTAMP" ~ '^\d{4}-\d{2}-\d{2}'

{% for year in range(era3_start, current_year + 1) %}
UNION ALL
SELECT
  CAST("TIMESTAMP" AS TIMESTAMP) AT TIME ZONE 'America/Sao_Paulo' AS recorded_at,
  'Era 3' AS equipment_era,
  {{ bounded_float('"Tar_AVG"', -5, 50) }} AS "Tar_AVG",
  {{ bounded_float('"UR_inst"', 0, 100) }} AS "UR_inst",
  {{ bounded_float('"Vvento_ms_AVG"', 0, 60) }} AS "Vvento_ms_AVG",
  {{ bounded_float('"Dvento_G"', 0, 360) }} AS "Dvento_G",
  {{ bounded_float('"Qg_AVG"', 0, 1500) }} AS "Qg_AVG",
  {{ bounded_float('"PAR_AVG"', 0, 3000) }} AS "PAR_AVG",
  {{ bounded_float('"Rn_Avg"', -200, 1500) }} AS "Rn_Avg",
  {{ bounded_float('"Chuva_mm"', 0, 200) }} AS "Chuva_mm",
  {{ bounded_float('"Dvento_SD1_WVT"', 0, 360) }} AS "Dvento_SD1_WVT",
  {{ bounded_float('"BattV_Avg"', 0, 20) }} AS "BattV_Avg",
  {{ bounded_float('"Patm_kPa_AVG"', 90, 105) }} AS "Patm_kPa_AVG",
  {{ bounded_float('"rQg_AVG"', 0, 1500) }} AS "rQg_AVG",
  {{ bounded_float('"Qatm_AVG"', 0, 700) }} AS "Qatm_AVG",
  {{ bounded_float('"Qsup_AVG"', 0, 700) }} AS "Qsup_AVG",
  {{ bounded_float('"Boc_AVG"', -500, 500) }} AS "Boc_AVG",
  {{ bounded_float('"Bol_AVG"', -500, 500) }} AS "Bol_AVG",
  {{ bounded_float('"Albedo_Avg"', 0, 1) }} AS "Albedo_Avg",
  {{ bounded_float('"QatmC_AVG"', 0, 700) }} AS "QatmC_AVG",
  {{ bounded_float('"QsupC_AVG"', 0, 700) }} AS "QsupC_AVG",
  {{ bounded_float('"Vvento_ms_S_WVT"', 0, 60) }} AS "Vvento_ms_S_WVT",
  {{ bounded_float('"Dvento_D1_WVT"', 0, 360) }} AS "Dvento_D1_WVT",
  {{ bounded_float('"PainelT"', -10, 80) }} AS "PainelT",
  _source_url
FROM bronze."raw_{{ year }}"
WHERE
  "TIMESTAMP" IS NOT NULL
  AND TRIM("TIMESTAMP") NOT IN ('', 'NaN', 'nan', 'None')
  AND "TIMESTAMP" ~ '^\d{4}-\d{2}-\d{2}'
{% endfor %}
