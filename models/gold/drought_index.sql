-- Z-score variant of SPI over 1/3/6/12-month windows. Real SPI fits a gamma
-- distribution per (calendar month, window); we use z-score against historical
-- mean/std for the same calendar-month-end across all years. Operationally
-- close to NOAA's "Precipitation Z-Index". Categories follow standard SPI
-- thresholds. Partial windows at the start of the series are filtered out.
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', obs_date)::DATE AS obs_month,
    SUM("Chuva_mm_sum") AS precip_month_mm
  FROM {{ ref('daily_summary') }}
  GROUP BY 1
),

windows AS (
  {% for w in [1, 3, 6, 12] %}
  SELECT
    obs_month,
    {{ w }}::SMALLINT AS window_months,
    CASE
      WHEN COUNT(*) OVER (ORDER BY obs_month ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW) = {{ w }}
      THEN SUM(precip_month_mm) OVER (ORDER BY obs_month ROWS BETWEEN {{ w - 1 }} PRECEDING AND CURRENT ROW)
    END AS precip_window_mm
  FROM monthly
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
),

stats AS (
  SELECT
    obs_month,
    window_months,
    precip_window_mm,
    AVG(precip_window_mm) OVER (
      PARTITION BY window_months, EXTRACT(MONTH FROM obs_month)
    ) AS precip_window_mean_mm,
    STDDEV_POP(precip_window_mm) OVER (
      PARTITION BY window_months, EXTRACT(MONTH FROM obs_month)
    ) AS precip_window_std_mm
  FROM windows
  WHERE precip_window_mm IS NOT NULL
)

SELECT
  obs_month,
  window_months,
  precip_window_mm,
  precip_window_mean_mm,
  precip_window_std_mm,
  (precip_window_mm - precip_window_mean_mm) / NULLIF(precip_window_std_mm, 0) AS z_score,
  CASE
    WHEN (precip_window_mm - precip_window_mean_mm) / NULLIF(precip_window_std_mm, 0) <= -2 THEN 'extreme drought'
    WHEN (precip_window_mm - precip_window_mean_mm) / NULLIF(precip_window_std_mm, 0) <= -1.5 THEN 'severe drought'
    WHEN (precip_window_mm - precip_window_mean_mm) / NULLIF(precip_window_std_mm, 0) <= -1 THEN 'moderate drought'
    WHEN (precip_window_mm - precip_window_mean_mm) / NULLIF(precip_window_std_mm, 0) < 1 THEN 'near normal'
    WHEN (precip_window_mm - precip_window_mean_mm) / NULLIF(precip_window_std_mm, 0) < 1.5 THEN 'moderately wet'
    WHEN (precip_window_mm - precip_window_mean_mm) / NULLIF(precip_window_std_mm, 0) < 2 THEN 'severely wet'
    ELSE 'extremely wet'
  END AS category
FROM stats
