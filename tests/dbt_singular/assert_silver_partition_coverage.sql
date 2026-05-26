-- Every yearly partition between FIRST_YEAR (1997) and the previous calendar
-- year must contain at least one row. Detects a silently failed bronze
-- ingestion that left a partition empty after the daily reload. The current
-- calendar year is excluded because it may legitimately have zero rows
-- if the year just rolled over and no fresh data has arrived yet.
WITH expected AS (
  SELECT generate_series(
    {{ var('first_year') }},
    EXTRACT(YEAR FROM CURRENT_DATE)::int - 1
  ) AS yr
),
actual AS (
  SELECT DISTINCT EXTRACT(YEAR FROM recorded_at)::int AS yr
  FROM {{ ref('weather_data') }}
)
SELECT e.yr AS missing_year
FROM expected e
LEFT JOIN actual a USING (yr)
WHERE a.yr IS NULL
