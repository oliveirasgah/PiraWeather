-- Era 1a (1997-1999) and Era 1b (2000-2002).
-- Vento dedup index shifts because UR's position relative to Vento differs:
--   Era 1a: Vento_4 = speed, Vento_5 = direction
--   Era 1b: Vento_3 = speed, Vento_4 = direction
-- recorded_at = Jan 1 + (julian_day - 1) days + (HHMM - floor(HHMM/100)*40) minutes
-- Source timestamps are local Piracicaba time; tagged as America/Sao_Paulo.
{{ config(materialized='ephemeral') }}

WITH era1a AS (
  {% for year in [1997, 1998, 1999] %}
  SELECT
    '{{ year }}' AS _year_str,
    _source_url,
    {{ bounded_float('"Dia_Juliano"', 1, 366) }} AS _julian_day,
    {{ bounded_float('"Horario"', 0, 2400) }} AS _hhmm,
    {{ bounded_float('"Tar"', -5, 50) }} AS "Tar_AVG",
    {{ bounded_float('"UR"', 0, 100) }} AS "UR_inst",
    {{ bounded_float('"Vento_4"', 0, 60) }} AS "Vvento_ms_AVG",
    {{ bounded_float('"Vento_5"', 0, 360) }} AS "Dvento_G",
    {{ bounded_float('"Rad_Solar"', 0, 1500) }} AS "Qg_AVG",
    {{ bounded_float('"PAR"', 0, 3000) }} AS "PAR_AVG",
    {{ bounded_float('"Rad_Liq"', -200, 1500) }} AS "Rn_Avg",
    {{ bounded_float('"Chuva"', 0, 200) }} AS "Chuva_mm",
    'Era 1a' AS equipment_era
  FROM bronze."raw_{{ year }}"
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
),
era1b AS (
  {% for year in [2000, 2001, 2002] %}
  SELECT
    '{{ year }}' AS _year_str,
    _source_url,
    {{ bounded_float('"Dia_Juliano"', 1, 366) }} AS _julian_day,
    {{ bounded_float('"Horario"', 0, 2400) }} AS _hhmm,
    {{ bounded_float('"Tar"', -5, 50) }} AS "Tar_AVG",
    {{ bounded_float('"UR"', 0, 100) }} AS "UR_inst",
    {{ bounded_float('"Vento_3"', 0, 60) }} AS "Vvento_ms_AVG",
    {{ bounded_float('"Vento_4"', 0, 360) }} AS "Dvento_G",
    {{ bounded_float('"Rad_Solar"', 0, 1500) }} AS "Qg_AVG",
    {{ bounded_float('"PAR"', 0, 3000) }} AS "PAR_AVG",
    {{ bounded_float('"Rad_Liq"', -200, 1500) }} AS "Rn_Avg",
    {{ bounded_float('"Chuva"', 0, 200) }} AS "Chuva_mm",
    'Era 1b' AS equipment_era
  FROM bronze."raw_{{ year }}"
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
),
combined AS (
  SELECT * FROM era1a
  UNION ALL
  SELECT * FROM era1b
)
SELECT
  (
    MAKE_TIMESTAMP(CAST(_year_str AS INT), 1, 1, 0, 0, 0)
    + (_julian_day - 1) * INTERVAL '1 day'
    + (_hhmm - FLOOR(_hhmm / 100) * 40) * INTERVAL '1 minute'
  ) AT TIME ZONE 'America/Sao_Paulo' AS recorded_at,
  equipment_era,
  "Tar_AVG",
  "UR_inst",
  "Vvento_ms_AVG",
  "Dvento_G",
  "Qg_AVG",
  "PAR_AVG",
  "Rn_Avg",
  "Chuva_mm",
  NULL::DOUBLE PRECISION AS "Dvento_SD1_WVT",
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
FROM combined
WHERE
  _julian_day IS NOT NULL
  AND _hhmm IS NOT NULL
