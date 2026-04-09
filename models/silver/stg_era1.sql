/*
  stg_era1 — Era 1a (1997–1999) and Era 1b (2000–2002)
  ───────────────────────────────────────────────────────
  Both sub-eras use Julian day + HHMM time to reconstruct TIMESTAMP.
  After the sanitize_name Unicode-normalization fix, both use "Horario"
  (the á in Horário is now mapped to a).

  Sub-era difference: UR appears BEFORE Vento in 1a, AFTER in 1b,
  which changes the positional deduplication suffix on the two Vento columns.
    Era 1a: Vento_4 = wind speed,  Vento_5 = wind direction
    Era 1b: Vento_3 = wind speed,  Vento_4 = wind direction

  Dropped columns (no Era 3 equivalent):
    Niv_Tanq, RS_EPP, Eppley, es, ea, Tu, To, DPV, F_C_S_

  TIMESTAMP formula:
    minutes = HHMM - FLOOR(HHMM / 100) * 40   (e.g. 1230 → 750 min = 12h30)
    TS = Jan 1 of year + (julian_day - 1) days + minutes
*/
{{ config(materialized='ephemeral') }}

with era1a as (
    -- 1997-1999: UR before Vento → Vento_4 = speed, Vento_5 = direction
    {% for year in [1997, 1998, 1999] %}
    select
        '{{ year }}'                                    as _year_str,
        _source_url,
        cast("Dia_Juliano"  as numeric)                 as _julian_day,
        cast("Horario"      as numeric)                 as _hhmm,
        cast("Tar"          as double precision)        as "Tar_AVG",
        cast("UR"           as double precision)        as "UR_inst",
        cast("Vento_4"      as double precision)        as "Vvento_ms_AVG",
        cast("Vento_5"      as double precision)        as "Dvento_G",
        cast("Rad_Solar"    as double precision)        as "Qg_AVG",
        cast("PAR"          as double precision)        as "PAR_AVG",
        cast("Rad_Liq"      as double precision)        as "Rn_Avg",
        cast("Chuva"        as double precision)        as "Chuva_mm",
        'Era 1a'                                        as equipment_era
    from bronze."raw_{{ year }}"
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

era1b as (
    -- 2000-2002: UR after Vento → Vento_3 = speed, Vento_4 = direction
    {% for year in [2000, 2001, 2002] %}
    select
        '{{ year }}'                                    as _year_str,
        _source_url,
        cast("Dia_Juliano"  as numeric)                 as _julian_day,
        cast("Horario"      as numeric)                 as _hhmm,
        cast("Tar"          as double precision)        as "Tar_AVG",
        cast("UR"           as double precision)        as "UR_inst",
        cast("Vento_3"      as double precision)        as "Vvento_ms_AVG",
        cast("Vento_4"      as double precision)        as "Dvento_G",
        cast("Rad_Solar"    as double precision)        as "Qg_AVG",
        cast("PAR"          as double precision)        as "PAR_AVG",
        cast("Rad_Liq"      as double precision)        as "Rn_Avg",
        cast("Chuva"        as double precision)        as "Chuva_mm",
        'Era 1b'                                        as equipment_era
    from bronze."raw_{{ year }}"
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

combined as (
    select * from era1a
    union all
    select * from era1b
)

select
    make_timestamp(cast(_year_str as int), 1, 1, 0, 0, 0)
        + (_julian_day - 1)                             * interval '1 day'
        + (_hhmm - floor(_hhmm / 100) * 40)            * interval '1 minute'
                                                        as "TIMESTAMP",
    equipment_era,
    "Tar_AVG",
    "UR_inst",
    "Vvento_ms_AVG",
    "Dvento_G",
    "Qg_AVG",
    "PAR_AVG",
    "Rn_Avg",
    "Chuva_mm",
    -- Era 3-only columns: NULL for all Era 1 rows
    null::double precision                              as "Dvento_SD1_WVT",
    null::double precision                              as "BattV_Avg",
    null::double precision                              as "Patm_kPa_AVG",
    null::double precision                              as "rQg_AVG",
    null::double precision                              as "Qatm_AVG",
    null::double precision                              as "Qsup_AVG",
    null::double precision                              as "Boc_AVG",
    null::double precision                              as "Bol_AVG",
    null::double precision                              as "Albedo_Avg",
    null::double precision                              as "QatmC_AVG",
    null::double precision                              as "QsupC_AVG",
    null::double precision                              as "Vvento_ms_S_WVT",
    null::double precision                              as "Dvento_D1_WVT",
    null::double precision                              as "PainelT",
    cast(_year_str as integer)                          as _source_year,
    _source_url
from combined
where _julian_day is not null
  and _hhmm is not null
