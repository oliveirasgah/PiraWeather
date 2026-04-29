-- GDD per crop per day. Uses the simple average method:
--   gdd = max((Tmax + Tmin)/2 - Tbase, 0)
-- Cumulative sum resets each calendar year. Crops below their base temperature
-- contribute 0 GDD on a given day.
SELECT
  d.obs_date,
  c.crop,
  c.base_temp_c,
  GREATEST(((d."Tar_max" + d."Tar_min") / 2.0) - c.base_temp_c, 0) AS gdd_daily,
  SUM(GREATEST(((d."Tar_max" + d."Tar_min") / 2.0) - c.base_temp_c, 0)) OVER (
    PARTITION BY c.crop, EXTRACT(YEAR FROM d.obs_date)
    ORDER BY d.obs_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS gdd_cumulative_year
FROM {{ ref('daily_summary') }} d
CROSS JOIN {{ ref('crop_base_temperatures') }} c
WHERE d."Tar_max" IS NOT NULL AND d."Tar_min" IS NOT NULL
