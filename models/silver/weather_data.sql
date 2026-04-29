-- Unified silver table (1997-present), partitioned by year on recorded_at.
-- Eras NULL-fill columns absent in their schema; all eras share the same
-- column list and order so the INSERT in the partitioned_table macro lines up.
{{ config(materialized='partitioned_table') }}

SELECT
  recorded_at,
  equipment_era,
  "Tar_AVG",
  "UR_inst",
  "Vvento_ms_AVG",
  "Dvento_G",
  "Qg_AVG",
  "PAR_AVG",
  "Rn_Avg",
  "Chuva_mm",
  "Dvento_SD1_WVT",
  "BattV_Avg",
  "Patm_kPa_AVG",
  "rQg_AVG",
  "Qatm_AVG",
  "Qsup_AVG",
  "Boc_AVG",
  "Bol_AVG",
  "Albedo_Avg",
  "QatmC_AVG",
  "QsupC_AVG",
  "Vvento_ms_S_WVT",
  "Dvento_D1_WVT",
  "PainelT",
  _source_url
FROM {{ ref('era1') }}

UNION ALL

SELECT
  recorded_at,
  equipment_era,
  "Tar_AVG",
  "UR_inst",
  "Vvento_ms_AVG",
  "Dvento_G",
  "Qg_AVG",
  "PAR_AVG",
  "Rn_Avg",
  "Chuva_mm",
  "Dvento_SD1_WVT",
  "BattV_Avg",
  "Patm_kPa_AVG",
  "rQg_AVG",
  "Qatm_AVG",
  "Qsup_AVG",
  "Boc_AVG",
  "Bol_AVG",
  "Albedo_Avg",
  "QatmC_AVG",
  "QsupC_AVG",
  "Vvento_ms_S_WVT",
  "Dvento_D1_WVT",
  "PainelT",
  _source_url
FROM {{ ref('era2') }}

UNION ALL

SELECT
  recorded_at,
  equipment_era,
  "Tar_AVG",
  "UR_inst",
  "Vvento_ms_AVG",
  "Dvento_G",
  "Qg_AVG",
  "PAR_AVG",
  "Rn_Avg",
  "Chuva_mm",
  "Dvento_SD1_WVT",
  "BattV_Avg",
  "Patm_kPa_AVG",
  "rQg_AVG",
  "Qatm_AVG",
  "Qsup_AVG",
  "Boc_AVG",
  "Bol_AVG",
  "Albedo_Avg",
  "QatmC_AVG",
  "QsupC_AVG",
  "Vvento_ms_S_WVT",
  "Dvento_D1_WVT",
  "PainelT",
  _source_url
FROM {{ ref('era3') }}
