/*
  silver.era2 — Era 2 (2003–2015) and raw_2016_s1
  ──────────────────────────────────────────────────
  Same recorded_at reconstruction as Era 1 but with different column names:
    Dia      → julian day
    Horas    → HHMM time
    Precip   → Chuva_mm
    Dir_Ven  → Dvento_G
    Desv_Pad → Dvento_SD1_WVT

  Vento has no duplicate in Era 2 so no positional suffix needed.
  raw_2016_s1 uses Era 2 format (rows before the station upgrade mid-2016).

  Dropped columns (no equivalent): F_C_S_, es, ea, Tu, To, DPV, Niv_Tanq, Rad_Epp
*/
{{ config(materialized='table') }}

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
  -- raw_2016_s1: first section of 2016 file, still in Era 2 format
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
  MAKE_TIMESTAMP(CAST(_year_str AS INT), 1, 1, 0, 0, 0)
  + (_julian_day - 1) * INTERVAL '1 day'
  + (_hhmm - FLOOR(_hhmm / 100) * 40) * INTERVAL '1 minute' AS recorded_at,
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
  _source_url
FROM src
WHERE
  _julian_day IS NOT NULL
  AND _hhmm IS NOT NULL
