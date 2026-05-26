-- gdd_cumulative_year is a running sum partitioned by (crop, year), so it
-- must be non-decreasing within each partition. Any row where the previous
-- cumulative value is strictly greater than the current one is a violation.
-- Empty result set = pass.
WITH ordered AS (
  SELECT
    crop,
    EXTRACT(YEAR FROM obs_date)::int AS yr,
    obs_date,
    gdd_cumulative_year,
    LAG(gdd_cumulative_year) OVER (
      PARTITION BY crop, EXTRACT(YEAR FROM obs_date)
      ORDER BY obs_date
    ) AS prev_cum
  FROM {{ ref('growing_degree_days') }}
)
SELECT crop, yr, obs_date, prev_cum, gdd_cumulative_year
FROM ordered
WHERE prev_cum IS NOT NULL
  AND prev_cum > gdd_cumulative_year
