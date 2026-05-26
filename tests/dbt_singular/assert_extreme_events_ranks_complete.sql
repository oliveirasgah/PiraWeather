-- Every category in extreme_events must have ranks 1..10 contiguous.
-- unique_combination_of_columns(category, rank) catches duplicates;
-- this catches gaps and short categories. Empty = pass.
WITH per_category AS (
  SELECT
    category,
    COUNT(*) AS n,
    MIN(rank) AS min_rank,
    MAX(rank) AS max_rank
  FROM {{ ref('extreme_events') }}
  GROUP BY category
)
SELECT *
FROM per_category
WHERE n != 10
   OR min_rank != 1
   OR max_rank != 10
