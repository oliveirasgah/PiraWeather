{{ config(materialized='table') }}
-- Top-10 per category. Materialized as table because each TOP-N would re-scan
-- silver on every dashboard hit; pre-computing once per dbt run is cheaper.
WITH monthly_chuva AS (
  SELECT
    DATE_TRUNC('month', obs_date)::DATE AS month_start,
    SUM("Chuva_mm_sum") AS chuva_total
  FROM {{ ref('daily_summary') }}
  GROUP BY 1
),

hottest_day AS (
  SELECT 'hottest_day' AS category, rank, event_at, value, 'C' AS unit FROM (
    SELECT
      obs_date::TIMESTAMPTZ AS event_at,
      "Tar_max" AS value,
      ROW_NUMBER() OVER (ORDER BY "Tar_max" DESC NULLS LAST) AS rank
    FROM {{ ref('daily_summary') }}
  ) t WHERE rank <= 10
),

coldest_day AS (
  SELECT 'coldest_day' AS category, rank, event_at, value, 'C' AS unit FROM (
    SELECT
      obs_date::TIMESTAMPTZ AS event_at,
      "Tar_min" AS value,
      ROW_NUMBER() OVER (ORDER BY "Tar_min" ASC NULLS LAST) AS rank
    FROM {{ ref('daily_summary') }}
  ) t WHERE rank <= 10
),

wettest_day AS (
  SELECT 'wettest_day' AS category, rank, event_at, value, 'mm' AS unit FROM (
    SELECT
      obs_date::TIMESTAMPTZ AS event_at,
      "Chuva_mm_sum" AS value,
      ROW_NUMBER() OVER (ORDER BY "Chuva_mm_sum" DESC NULLS LAST) AS rank
    FROM {{ ref('daily_summary') }}
  ) t WHERE rank <= 10
),

wettest_hour AS (
  SELECT 'wettest_hour' AS category, rank, event_at, value, 'mm' AS unit FROM (
    SELECT
      recorded_at AS event_at,
      "Chuva_mm" AS value,
      ROW_NUMBER() OVER (ORDER BY "Chuva_mm" DESC NULLS LAST) AS rank
    FROM {{ ref('weather_data') }}
  ) t WHERE rank <= 10
),

windiest_hour AS (
  SELECT 'windiest_hour' AS category, rank, event_at, value, 'm/s' AS unit FROM (
    SELECT
      recorded_at AS event_at,
      "Vvento_ms_AVG" AS value,
      ROW_NUMBER() OVER (ORDER BY "Vvento_ms_AVG" DESC NULLS LAST) AS rank
    FROM {{ ref('weather_data') }}
  ) t WHERE rank <= 10
),

driest_month AS (
  SELECT 'driest_month' AS category, rank, event_at, value, 'mm' AS unit FROM (
    SELECT
      month_start::TIMESTAMPTZ AS event_at,
      chuva_total AS value,
      ROW_NUMBER() OVER (ORDER BY chuva_total ASC NULLS LAST) AS rank
    FROM monthly_chuva
  ) t WHERE rank <= 10
)

SELECT * FROM hottest_day
UNION ALL SELECT * FROM coldest_day
UNION ALL SELECT * FROM wettest_day
UNION ALL SELECT * FROM wettest_hour
UNION ALL SELECT * FROM windiest_hour
UNION ALL SELECT * FROM driest_month
