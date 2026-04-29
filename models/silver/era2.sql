-- Era 2 (2003-2015 + raw_2016_s1, pre-upgrade).
-- Source columns: Dia, Horas, Precip, Dir_Ven, Desv_Pad. Vento has no dedup suffix.
-- Same recorded_at formula as Era 1; tagged as America/Sao_Paulo.
{{ config(materialized='ephemeral') }}

WITH src AS (
  {% for year in range(2003, 2016) %}
  SELECT
    '{{ year }}' AS _year_str,
    _source_url,
    {{ safe_float('"Dia"') }} AS _julian_day,
    {{ safe_float('"Horas"') }} AS _hhmm,
    {{ safe_float('"Tar"') }} AS "Tar_AVG",
    {{ safe_float('"UR"') }} AS "UR_inst",
    {{ safe_float('"Vento"') }} AS "Vvento_ms_AVG",
    {{ safe_float('"Dir_Ven"') }} AS "Dvento_G",
    {{ safe_float('"Desv_Pad"') }} AS "Dvento_SD1_WVT",
    {{ safe_float('"Rad_Solar"') }} AS "Qg_AVG",
    {{ safe_float('"PAR"') }} AS "PAR_AVG",
    {{ safe_float('"Rad_Liq"') }} AS "Rn_Avg",
    {{ safe_float('"Precip"') }} AS "Chuva_mm"
  FROM bronze."raw_{{ year }}"
  UNION ALL
  {% endfor %}
  SELECT
    '2016' AS _year_str,
    _source_url,
    {{ safe_float('"Dia"') }} AS _julian_day,
    {{ safe_float('"Horas"') }} AS _hhmm,
    {{ safe_float('"Tar"') }} AS "Tar_AVG",
    {{ safe_float('"UR"') }} AS "UR_inst",
    {{ safe_float('"Vento"') }} AS "Vvento_ms_AVG",
    {{ safe_float('"Dir_Ven"') }} AS "Dvento_G",
    {{ safe_float('"Desv_Pad"') }} AS "Dvento_SD1_WVT",
    {{ safe_float('"Rad_Solar"') }} AS "Qg_AVG",
    {{ safe_float('"PAR"') }} AS "PAR_AVG",
    {{ safe_float('"Rad_Liq"') }} AS "Rn_Avg",
    {{ safe_float('"Precip"') }} AS "Chuva_mm"
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
