-- The category column in drought_index is computed from z_score thresholds
-- via a CASE expression. This test re-derives the expected category from
-- z_score and reports any rows where the two disagree — guards against
-- accidental threshold drift between the CASE and the documented bands.
--
-- Bands (matching drought_index.sql):
--   z <= -2         → extreme drought
--   -2  < z <= -1.5 → severe drought
--   -1.5 < z <= -1  → moderate drought
--   -1   < z <  1   → near normal
--   1   <= z <  1.5 → moderately wet
--   1.5 <= z <  2   → severely wet
--   z >= 2          → extremely wet
WITH expected AS (
  SELECT
    obs_month,
    window_months,
    z_score,
    category AS actual_category,
    CASE
      WHEN z_score <= -2   THEN 'extreme drought'
      WHEN z_score <= -1.5 THEN 'severe drought'
      WHEN z_score <= -1   THEN 'moderate drought'
      WHEN z_score <  1    THEN 'near normal'
      WHEN z_score <  1.5  THEN 'moderately wet'
      WHEN z_score <  2    THEN 'severely wet'
      ELSE                      'extremely wet'
    END AS expected_category
  FROM {{ ref('drought_index') }}
  WHERE z_score IS NOT NULL
)
SELECT *
FROM expected
WHERE actual_category IS DISTINCT FROM expected_category
