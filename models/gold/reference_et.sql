-- FAO-56 Penman-Monteith reference evapotranspiration, daily timestep.
-- ET0 = (0.408 * delta * Rn + gamma * (900/(T+273)) * u2 * (es - ea))
--       / (delta + gamma * (1 + 0.34 * u2))
-- Soil heat flux G ~= 0 for daily. Wind assumed measured at 2 m. Pressure
-- defaults to 94.0 kPa (FAO-56 elevation formula for Piracicaba ~580 m)
-- when Patm_kPa_avg is missing (Era 1 / Era 2). Pre-Era-3 days lack Rn too,
-- so ET0 is NULL for those days.
WITH inputs AS (
  SELECT
    obs_date,
    "Chuva_mm_sum" AS precip_mm,
    ("Tar_max" + "Tar_min") / 2.0 AS t_mean_c,
    "Rn_sum_mj_m2" AS rn_mj_m2_day,
    "Vvento_avg" AS u2_m_s,
    "UR_avg" AS rh_pct,
    COALESCE("Patm_kPa_avg", 94.0) AS p_kpa,
    "Patm_kPa_avg" IS NOT NULL AS inputs_complete
  FROM {{ ref('daily_summary') }}
),

intermediates AS (
  SELECT
    *,
    0.6108 * EXP((17.27 * t_mean_c) / (t_mean_c + 237.3)) AS es_kpa,
    0.6108 * EXP((17.27 * t_mean_c) / (t_mean_c + 237.3)) * rh_pct / 100.0 AS ea_kpa,
    (4098.0 * (0.6108 * EXP((17.27 * t_mean_c) / (t_mean_c + 237.3))))
      / POWER(t_mean_c + 237.3, 2) AS delta_kpa_c,
    0.000665 * p_kpa AS gamma_kpa_c
  FROM inputs
),

et AS (
  SELECT
    obs_date,
    precip_mm,
    inputs_complete,
    (
      0.408 * delta_kpa_c * rn_mj_m2_day
      + gamma_kpa_c * (900.0 / (t_mean_c + 273.0)) * u2_m_s * (es_kpa - ea_kpa)
    ) / (
      delta_kpa_c + gamma_kpa_c * (1.0 + 0.34 * u2_m_s)
    ) AS et0_mm_day
  FROM intermediates
)

SELECT
  obs_date,
  et0_mm_day,
  precip_mm,
  precip_mm - et0_mm_day AS water_balance_mm,
  SUM(precip_mm - et0_mm_day) OVER (
    ORDER BY obs_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS water_balance_30d_mm,
  inputs_complete
FROM et
