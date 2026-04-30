-- Era 2 (2003-2015 + raw_2016_s1, pre-upgrade).
-- Source columns: Dia, Horas, Precip, Dir_Ven, Desv_Pad. Vento has no dedup suffix.
-- Same recorded_at formula as Era 1; tagged as America/Sao_Paulo.
{{ config(materialized='ephemeral') }}

WITH src AS (
  {% for year in range(2003, 2016) %}
  SELECT
    '{{ year }}' AS _year_str,
    _source_url,
    {{ bounded_float('"Dia"', 1, 366) }} AS _julian_day,
    {{ bounded_float('"Horas"', 0, 2400) }} AS _hhmm,
    {{ bounded_float('"Tar"', -5, 50) }} AS "Tar_AVG",
    {{ bounded_float('"UR"', 0, 100) }} AS "UR_inst",
    {{ bounded_float('"Vento"', 0, 60) }} AS "Vvento_ms_AVG",
    {{ bounded_float('"Dir_Ven"', 0, 360) }} AS "Dvento_G",
    {{ bounded_float('"Desv_Pad"', 0, 360) }} AS "Dvento_SD1_WVT",
    {{ bounded_float('"Rad_Solar"', 0, 1500) }} AS "Qg_AVG",
    {{ bounded_float('"PAR"', 0, 3000) }} AS "PAR_AVG",
    {{ bounded_float('"Rad_Liq"', -200, 1500) }} AS "Rn_Avg",
    {{ bounded_float('"Precip"', 0, 200) }} AS "Chuva_mm"
  FROM bronze."raw_{{ year }}"
  UNION ALL
  {% endfor %}
  SELECT
    '2016' AS _year_str,
    _source_url,
    {{ bounded_float('"Dia"', 1, 366) }} AS _julian_day,
    {{ bounded_float('"Horas"', 0, 2400) }} AS _hhmm,
    {{ bounded_float('"Tar"', -5, 50) }} AS "Tar_AVG",
    {{ bounded_float('"UR"', 0, 100) }} AS "UR_inst",
    {{ bounded_float('"Vento"', 0, 60) }} AS "Vvento_ms_AVG",
    {{ bounded_float('"Dir_Ven"', 0, 360) }} AS "Dvento_G",
    {{ bounded_float('"Desv_Pad"', 0, 360) }} AS "Dvento_SD1_WVT",
    {{ bounded_float('"Rad_Solar"', 0, 1500) }} AS "Qg_AVG",
    {{ bounded_float('"PAR"', 0, 3000) }} AS "PAR_AVG",
    {{ bounded_float('"Rad_Liq"', -200, 1500) }} AS "Rn_Avg",
    {{ bounded_float('"Precip"', 0, 200) }} AS "Chuva_mm"
  FROM bronze."raw_2016_s1"
)
SELECT
  (
    MAKE_TIMESTAMP(CAST(_year_str AS INT), 1, 1, 0, 0, 0)
    + (_julian_day - 1) * INTERVAL '1 day'
    + (_hhmm - FLOOR(_hhmm / 100) * 40) * INTERVAL '1 minute'
  ) AT TIME ZONE 'America/Sao_Paulo' AS recorded_at,
  'Era 2' AS equipment_era,
  "Tar_AVG",
  "UR_inst",
  "Vvento_ms_AVG",
  "Dvento_G",
  "Qg_AVG",
  "PAR_AVG",
  "Rn_Avg",
  "Chuva_mm",
  "Dvento_SD1_WVT",
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
FROM src
WHERE
  _julian_day IS NOT NULL
  AND _hhmm IS NOT NULL
