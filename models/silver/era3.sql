-- Era 3 (raw_2016_s2 post-upgrade + 2017-present).
-- Source columns already match the canonical schema; only typed casts needed.
-- raw_2016_s2 NULLs sensors added after the initial upgrade.
-- New years auto-included via the Jinja loop.
{{ config(materialized='ephemeral') }}

{% set era3_start = 2017 %}
{% set current_year = modules.datetime.date.today().year %}

SELECT
  CAST("TIMESTAMP" AS TIMESTAMPTZ) AS recorded_at,
  'Era 3' AS equipment_era,
  {{ safe_float('"Tar_AVG"') }} AS "Tar_AVG",
  {{ safe_float('"UR_inst"') }} AS "UR_inst",
  {{ safe_float('"Vvento_ms_AVG"') }} AS "Vvento_ms_AVG",
  {{ safe_float('"Dvento_G"') }} AS "Dvento_G",
  {{ safe_float('"Qg_AVG"') }} AS "Qg_AVG",
  {{ safe_float('"PAR_AVG"') }} AS "PAR_AVG",
  {{ safe_float('"Rn_Avg"') }} AS "Rn_Avg",
  {{ safe_float('"Chuva_mm"') }} AS "Chuva_mm",
  {{ safe_float('"Dvento_SD1_WVT"') }} AS "Dvento_SD1_WVT",
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
  CAST("TIMESTAMP" AS TIMESTAMPTZ) AS recorded_at,
  'Era 3' AS equipment_era,
  {{ safe_float('"Tar_AVG"') }} AS "Tar_AVG",
  {{ safe_float('"UR_inst"') }} AS "UR_inst",
  {{ safe_float('"Vvento_ms_AVG"') }} AS "Vvento_ms_AVG",
  {{ safe_float('"Dvento_G"') }} AS "Dvento_G",
  {{ safe_float('"Qg_AVG"') }} AS "Qg_AVG",
  {{ safe_float('"PAR_AVG"') }} AS "PAR_AVG",
  {{ safe_float('"Rn_Avg"') }} AS "Rn_Avg",
  {{ safe_float('"Chuva_mm"') }} AS "Chuva_mm",
  {{ safe_float('"Dvento_SD1_WVT"') }} AS "Dvento_SD1_WVT",
  {{ safe_float('"BattV_Avg"') }} AS "BattV_Avg",
  {{ safe_float('"Patm_kPa_AVG"') }} AS "Patm_kPa_AVG",
  {{ safe_float('"rQg_AVG"') }} AS "rQg_AVG",
  {{ safe_float('"Qatm_AVG"') }} AS "Qatm_AVG",
  {{ safe_float('"Qsup_AVG"') }} AS "Qsup_AVG",
  {{ safe_float('"Boc_AVG"') }} AS "Boc_AVG",
  {{ safe_float('"Bol_AVG"') }} AS "Bol_AVG",
  {{ safe_float('"Albedo_Avg"') }} AS "Albedo_Avg",
  {{ safe_float('"QatmC_AVG"') }} AS "QatmC_AVG",
  {{ safe_float('"QsupC_AVG"') }} AS "QsupC_AVG",
  {{ safe_float('"Vvento_ms_S_WVT"') }} AS "Vvento_ms_S_WVT",
  {{ safe_float('"Dvento_D1_WVT"') }} AS "Dvento_D1_WVT",
  {{ safe_float('"PainelT"') }} AS "PainelT",
  _source_url
FROM bronze."raw_{{ year }}"
WHERE
  "TIMESTAMP" IS NOT NULL
  AND TRIM("TIMESTAMP") NOT IN ('', 'NaN', 'nan', 'None')
  AND "TIMESTAMP" ~ '^\d{4}-\d{2}-\d{2}'
{% endfor %}
