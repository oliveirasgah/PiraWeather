/*
  stg_era2 — Era 2 (2003–2015) and raw_2016_s1
  ──────────────────────────────────────────────
  Same TIMESTAMP reconstruction as Era 1 but with different column names:
    Dia      → julian day
    Horas    → HHMM time
    Precip   → Chuva_mm
    Dir_Ven  → Dvento_G
    Desv_Pad → Dvento_SD1_WVT
    Rad_Solar → Qg_AVG  (note: may be "Rad__Solar" in some years — verify)

  Vento has no duplicate in Era 2 so no positional suffix needed.
  raw_2016_s1 uses Era 2 format (rows before the station upgrade mid-2016).

  Dropped columns (no Era 3 equivalent):
    F_C_S_, es, ea, Tu, To, DPV, Niv_Tanq, Rad_Epp
*/
{{ config(materialized='ephemeral') }}

with src as (
    {% for year in range(2003, 2016) %}
    select
        '{{ year }}'                                    as _year_str,
        _source_url,
        cast("Dia"          as numeric)                 as _julian_day,
        cast("Horas"        as numeric)                 as _hhmm,
        cast("Tar"          as double precision)        as "Tar_AVG",
        cast("UR"           as double precision)        as "UR_inst",
        cast("Vento"        as double precision)        as "Vvento_ms_AVG",
        cast("Dir_Ven"      as double precision)        as "Dvento_G",
        cast("Desv_Pad"     as double precision)        as "Dvento_SD1_WVT",
        cast("Rad_Solar"    as double precision)        as "Qg_AVG",
        cast("PAR"          as double precision)        as "PAR_AVG",
        cast("Rad_Liq"      as double precision)        as "Rn_Avg",
        cast("Precip"       as double precision)        as "Chuva_mm"
    from bronze."raw_{{ year }}"
    union all
    {% endfor %}
    -- raw_2016_s1: first section of 2016 file, still in Era 2 format
    select
        '2016'                                          as _year_str,
        _source_url,
        cast("Dia"          as numeric)                 as _julian_day,
        cast("Horas"        as numeric)                 as _hhmm,
        cast("Tar"          as double precision)        as "Tar_AVG",
        cast("UR"           as double precision)        as "UR_inst",
        cast("Vento"        as double precision)        as "Vvento_ms_AVG",
        cast("Dir_Ven"      as double precision)        as "Dvento_G",
        cast("Desv_Pad"     as double precision)        as "Dvento_SD1_WVT",
        cast("Rad_Solar"    as double precision)        as "Qg_AVG",
        cast("PAR"          as double precision)        as "PAR_AVG",
        cast("Rad_Liq"      as double precision)        as "Rn_Avg",
        cast("Precip"       as double precision)        as "Chuva_mm"
    from bronze."raw_2016_s1"
)

select
    make_timestamp(cast(_year_str as int), 1, 1, 0, 0, 0)
        + (_julian_day - 1)                             * interval '1 day'
        + (_hhmm - floor(_hhmm / 100) * 40)            * interval '1 minute'
                                                        as "TIMESTAMP",
    'Era 2'                                             as equipment_era,
    "Tar_AVG",
    "UR_inst",
    "Vvento_ms_AVG",
    "Dvento_G",
    "Qg_AVG",
    "PAR_AVG",
    "Rn_Avg",
    "Chuva_mm",
    "Dvento_SD1_WVT",
    -- Era 3-only columns: NULL for all Era 2 rows
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
from src
where _julian_day is not null
  and _hhmm is not null
