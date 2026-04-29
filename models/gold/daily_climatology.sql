-- 30-year normals per day-of-year. percentile_cont is Postgres-native; no
-- dbt_utils needed. Feb 29 has fewer samples (one per leap year) - use
-- n_years to detect under-sampled DOYs.
SELECT
  EXTRACT(DOY FROM obs_date)::SMALLINT AS doy,
  COUNT(DISTINCT EXTRACT(YEAR FROM obs_date))::SMALLINT AS n_years,

  AVG("Tar_avg")                                                AS "Tar_avg_mean",
  PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY "Tar_avg")        AS "Tar_avg_p10",
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "Tar_avg")        AS "Tar_avg_p50",
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY "Tar_avg")        AS "Tar_avg_p90",

  AVG("Tar_min")                                                AS "Tar_min_mean",
  PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY "Tar_min")        AS "Tar_min_p10",
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "Tar_min")        AS "Tar_min_p50",
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY "Tar_min")        AS "Tar_min_p90",

  AVG("Tar_max")                                                AS "Tar_max_mean",
  PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY "Tar_max")        AS "Tar_max_p10",
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "Tar_max")        AS "Tar_max_p50",
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY "Tar_max")        AS "Tar_max_p90",

  AVG("UR_avg")                                                 AS "UR_avg_mean",
  PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY "UR_avg")         AS "UR_avg_p10",
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "UR_avg")         AS "UR_avg_p50",
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY "UR_avg")         AS "UR_avg_p90",

  AVG("Chuva_mm_sum")                                           AS "Chuva_mm_sum_mean",
  PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY "Chuva_mm_sum")   AS "Chuva_mm_sum_p10",
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "Chuva_mm_sum")   AS "Chuva_mm_sum_p50",
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY "Chuva_mm_sum")   AS "Chuva_mm_sum_p90",

  AVG("Rn_sum_mj_m2")                                           AS "Rn_sum_mj_m2_mean",
  PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY "Rn_sum_mj_m2")   AS "Rn_sum_mj_m2_p10",
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "Rn_sum_mj_m2")   AS "Rn_sum_mj_m2_p50",
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY "Rn_sum_mj_m2")   AS "Rn_sum_mj_m2_p90"
FROM {{ ref('daily_summary') }}
GROUP BY 1
