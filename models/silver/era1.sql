/*
  silver.era1 — Era 1a (1997–1999) and Era 1b (2000–2002)
  ─────────────────────────────────────────────────────────
  Both sub-eras use Julian day + HHMM time to reconstruct recorded_at.
  After the sanitize_name Unicode-normalization fix, both use "Horario".

  Sub-era difference: UR appears BEFORE Vento in 1a, AFTER in 1b,
  which changes the positional deduplication suffix on the two Vento columns.
    Era 1a: Vento_4 = wind speed,  Vento_5 = wind direction
    Era 1b: Vento_3 = wind speed,  Vento_4 = wind direction

  Dropped columns (no equivalent): Niv_Tanq, RS_EPP, Eppley, es, ea, Tu, To, DPV, F_C_S_

  recorded_at formula:
    minutes = HHMM - FLOOR(HHMM / 100) * 40   (e.g. 1230 → 750 min = 12h30)
    recorded_at = Jan 1 of year + (julian_day - 1) days + minutes
*/
{{ config(materialized='table') }}

WITH era1a AS (
  -- 1997-1999: UR before Vento → Vento_4 = speed, Vento_5 = direction
  {% for year in [1997, 1998, 1999] %}
  SELECT
    '{{ year }}' AS _year_str,
    _source_url,
    {{ safe_float('"Dia_Juliano"') }} AS _julian_day,
    {{ safe_float('"Horario"') }} AS _hhmm,
    {{ safe_float('"Tar"') }} AS "Tar_AVG",
    {{ safe_float('"UR"') }} AS "UR_inst",
    {{ safe_float('"Vento_4"') }} AS "Vvento_ms_AVG",
    {{ safe_float('"Vento_5"') }} AS "Dvento_G",
    {{ safe_float('"Rad_Solar"') }} AS "Qg_AVG",
    {{ safe_float('"PAR"') }} AS "PAR_AVG",
    {{ safe_float('"Rad_Liq"') }} AS "Rn_Avg",
    {{ safe_float('"Chuva"') }} AS "Chuva_mm",
    'Era 1a' AS equipment_era
  FROM bronze."raw_{{ year }}"
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
),
era1b AS (
  -- 2000-2002: UR after Vento → Vento_3 = speed, Vento_4 = direction
  {% for year in [2000, 2001, 2002] %}
  SELECT
    '{{ year }}' AS _year_str,
    _source_url,
    {{ safe_float('"Dia_Juliano"') }} AS _julian_day,
    {{ safe_float('"Horario"') }} AS _hhmm,
    {{ safe_float('"Tar"') }} AS "Tar_AVG",
    {{ safe_float('"UR"') }} AS "UR_inst",
    {{ safe_float('"Vento_3"') }} AS "Vvento_ms_AVG",
    {{ safe_float('"Vento_4"') }} AS "Dvento_G",
    {{ safe_float('"Rad_Solar"') }} AS "Qg_AVG",
    {{ safe_float('"PAR"') }} AS "PAR_AVG",
    {{ safe_float('"Rad_Liq"') }} AS "Rn_Avg",
    {{ safe_float('"Chuva"') }} AS "Chuva_mm",
    'Era 1b' AS equipment_era
  FROM bronze."raw_{{ year }}"
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
),
combined AS (
  SELECT *
  FROM era1a
  UNION ALL
  SELECT *
  FROM era1b
)
SELECT
  MAKE_TIMESTAMP(CAST(_year_str AS INT), 1, 1, 0, 0, 0)
  + (_julian_day - 1) * INTERVAL '1 day'
  + (_hhmm - FLOOR(_hhmm / 100) * 40) * INTERVAL '1 minute' AS recorded_at,
  equipment_era,
  "Tar_AVG",
  "UR_inst",
  "Vvento_ms_AVG",
  "Dvento_G",
  "Qg_AVG",
  "PAR_AVG",
  "Rn_Avg",
  "Chuva_mm",
  _source_url
FROM combined
WHERE
  _julian_day IS NOT NULL
  AND _hhmm IS NOT NULL
