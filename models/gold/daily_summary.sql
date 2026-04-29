-- Daily aggregation of silver.weather_data. Foundation for the rest of gold.
-- Radiation columns are summed and converted from W/m^2 (instantaneous average)
-- to MJ/m^2/day: avg_W_m2 * 86400 s / 1e6 J_per_MJ.
SELECT
  DATE(recorded_at AT TIME ZONE 'America/Sao_Paulo') AS obs_date,

  MIN("Tar_AVG")          AS "Tar_min",
  AVG("Tar_AVG")          AS "Tar_avg",
  MAX("Tar_AVG")          AS "Tar_max",

  MIN("UR_inst")          AS "UR_min",
  AVG("UR_inst")          AS "UR_avg",
  MAX("UR_inst")          AS "UR_max",

  AVG("Vvento_ms_AVG")    AS "Vvento_avg",
  MAX("Vvento_ms_AVG")    AS "Vvento_max",
  AVG("Dvento_G")         AS "Dvento_avg",

  AVG("Qg_AVG") * 86400.0 / 1e6 AS "Qg_sum_mj_m2",
  AVG("PAR_AVG")                AS "PAR_avg",
  AVG("Rn_Avg") * 86400.0 / 1e6 AS "Rn_sum_mj_m2",

  SUM("Chuva_mm")         AS "Chuva_mm_sum",
  AVG("Patm_kPa_AVG")     AS "Patm_kPa_avg",

  COUNT(*)                AS obs_count
FROM {{ ref('weather_data') }}
GROUP BY 1
