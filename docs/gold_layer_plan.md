# Gold Layer Plan

## Context

The silver layer (`silver.weather_data`) provides ~30 years of sub-hourly weather
observations from ESALQ/USP in Piracicaba, SP. Piracicaba sits in Brazil's
sugarcane / coffee / citrus belt, so the gold layer leans into agronomy and
climatology to demonstrate domain understanding rather than generic aggregations.

Six marts. Each is a separate dbt model under `models/gold/`, materialized as
documented below. All marts read from `silver.weather_data` (or upstream gold
marts where noted) so the silver model stays the single source of truth.

## Marts

### 1. `gold.daily_summary` (view)

Operational baseline. Drives most dashboard panels and the API's daily endpoints.

- **Grain**: one row per `obs_date`.
- **Source**: `silver.weather_data`.
- **Columns**:
  - `obs_date DATE`
  - `Tar_min`, `Tar_avg`, `Tar_max DOUBLE PRECISION`
  - `UR_min`, `UR_avg`, `UR_max DOUBLE PRECISION`
  - `Vvento_avg`, `Vvento_max DOUBLE PRECISION`
  - `Dvento_avg DOUBLE PRECISION`
  - `Qg_sum_mj_m2`, `PAR_sum`, `Rn_sum_mj_m2 DOUBLE PRECISION` (W/m^2 averages times 86400 / 1e6 -> MJ/m^2/day)
  - `Chuva_mm_sum DOUBLE PRECISION`
  - `Patm_kPa_avg DOUBLE PRECISION` (NULL pre-Era 3)
  - `obs_count INT` (rows aggregated; useful for QA — flags days with sensor gaps)
- **Materialization**: `view`. Cheap to recompute, always fresh.

### 2. `gold.daily_climatology` (view)

The "what's normal for this day of year" view. Showcases window functions and
makes the 30-year history actually useful.

- **Grain**: one row per day-of-year (1..366) per variable.
- **Source**: `gold.daily_summary`.
- **Columns**:
  - `doy SMALLINT` (1..366)
  - For each tracked variable (`Tar_avg`, `Tar_max`, `Tar_min`, `UR_avg`, `Chuva_mm_sum`, `Rn_sum_mj_m2`):
    - `<var>_mean`, `<var>_p10`, `<var>_p50`, `<var>_p90 DOUBLE PRECISION`
  - `n_years SMALLINT` (sample size for that DOY — flags Feb 29 sparseness)
- **Key SQL**: `EXTRACT(doy FROM obs_date)` then `percentile_cont(...) WITHIN GROUP (ORDER BY ...)`.
- **Materialization**: `view`. ~366 rows; recompute is sub-second.

### 3. `gold.extreme_events` (table)

Headline content for the dashboard ("hottest day on record", "wettest hour", etc).

- **Grain**: one row per (category, rank) pair.
- **Source**: `silver.weather_data` and `gold.daily_summary`.
- **Columns**:
  - `category TEXT` ('hottest_day', 'coldest_day', 'wettest_day', 'wettest_hour', 'windiest_hour', 'driest_month', ...)
  - `rank SMALLINT` (1..10)
  - `event_at TIMESTAMPTZ`
  - `value DOUBLE PRECISION`
  - `unit TEXT`
  - `equipment_era TEXT` (so reviewers can see whether records cluster in one era)
- **Materialization**: `table`. Each TOP-N category does a full silver scan;
  pre-computing once per dbt run keeps dashboard hits cheap.

### 4. `gold.growing_degree_days` (view)

Agronomic differentiator. Tracks heat accumulation per crop.

- **Grain**: one row per `obs_date` per `crop`.
- **Source**: `gold.daily_summary` cross-joined with a crop reference (seed file).
- **Crops + base temperatures** (in `seeds/crop_base_temperatures.csv`),
  covering the major Brazilian field/forestry crops with a wide base-temp
  range so GDD curves visibly diverge:
  - sugarcane: 18.0 C — SP mainstay; C4 grass with high thermal threshold
  - corn: 10.0 C — USDA standard base
  - soybean: 10.0 C — top BR export
  - coffee: 10.0 C — Arabica reference; MG/SP/ES belt
  - citrus: 12.5 C — sweet orange; SP dominant globally
  - cotton: 15.0 C — Cerrado staple (MT, BA)
  - rice: 10.0 C — RS supplies ~70% of the national crop
  - wheat: 5.0 C — winter crop in PR/RS; lowest base in the set
  - sorghum: 10.0 C — safrinha alternative to corn
  - beans: 10.0 C — cultural staple, multiple seasons
  - eucalyptus: 13.0 C — forestry; large area in MG/SP/BA
- **Columns**:
  - `obs_date DATE`
  - `crop TEXT`
  - `base_temp_c DOUBLE PRECISION`
  - `gdd_daily DOUBLE PRECISION` — `GREATEST(((Tar_max + Tar_min) / 2.0) - base_temp_c, 0)`
  - `gdd_cumulative_year DOUBLE PRECISION` — running sum within the calendar year per crop
- **Materialization**: `view`.
- **Note**: traditional GDD uses planting-date-relative cumulative sums, but
  for a portfolio piece year-relative is simpler to query and still
  illustrates the concept.

### 5. `gold.reference_et` (view)

Reference evapotranspiration via FAO-56 Penman-Monteith, daily timestep.
Demonstrates use of the full Era 3 sensor suite (T, RH, wind, Rn, P) and ends
with a water-balance column for irrigation decisions.

- **Grain**: one row per `obs_date`.
- **Source**: `gold.daily_summary`.
- **Columns**:
  - `obs_date DATE`
  - `et0_mm_day DOUBLE PRECISION` — FAO-56 ET0
  - `precip_mm DOUBLE PRECISION` — copy of `Chuva_mm_sum`
  - `water_balance_mm DOUBLE PRECISION` — `precip_mm - et0_mm_day`
  - `water_balance_30d_mm DOUBLE PRECISION` — 30-day rolling sum of water_balance
  - `inputs_complete BOOLEAN` — false for Era 1 / Era 2 days where Patm is missing
    and the elevation fallback is used
- **Materialization**: `view`.

#### FAO-56 ET0 formula (kept simple)

The reference equation (FAO Paper 56, Eq. 6) for grass reference, daily timestep,
soil heat flux G ~= 0:

```
        0.408 * delta * Rn  +  gamma * (900 / (T + 273)) * u2 * (es - ea)
ET0 = -------------------------------------------------------------------
                       delta + gamma * (1 + 0.34 * u2)
```

Inputs (from `gold.daily_summary`, with unit conversions):
- `T`: daily mean temperature in C — `(Tar_max + Tar_min) / 2`
- `Rn`: net radiation in MJ/m^2/day — `Rn_sum_mj_m2`
- `u2`: wind speed at 2 m in m/s — `Vvento_avg` (assumed measured at 2 m)
- `RH`: mean relative humidity in % — `UR_avg`
- `P`: atmospheric pressure in kPa — `Patm_kPa_avg`, fallback `94.0` kPa
  (FAO-56 elevation formula for Piracicaba ~580 m elevation)

Intermediate variables (compute as CTEs, one expression each):
- `es = 0.6108 * exp((17.27 * T) / (T + 237.3))` — saturation vapor pressure (kPa)
- `ea = es * RH / 100.0` — actual vapor pressure (kPa)
- `delta = (4098.0 * es) / power(T + 237.3, 2)` — slope of vapor pressure curve (kPa/C)
- `gamma = 0.000665 * P` — psychrometric constant (kPa/C)

The model is a single SQL file: one CTE for inputs, one CTE for the four
intermediates, then the final SELECT applying the equation. Reviewer can
trace each variable back to FAO-56 by name.

### 6. `gold.drought_index` (view)

Multi-window precipitation anomaly index, simplified SPI variant. Real SPI fits
a gamma distribution to historical aggregates and transforms via the inverse
normal CDF; that requires gamma fitting per window and per month-of-year, which
is impractical in pure SQL. Use a z-score over month-of-year history instead —
this is the operational shortcut used in many monitoring dashboards (NOAA's
"Precipitation Z-Index" follows the same idea).

- **Grain**: one row per `obs_month` per `window_months`.
- **Source**: `gold.daily_summary` rolled up to month.
- **Windows**: `1, 3, 6, 12` months (matches standard SPI reporting).
- **Columns**:
  - `obs_month DATE` (first of month)
  - `window_months SMALLINT`
  - `precip_window_mm DOUBLE PRECISION` — sum of monthly rainfall over the trailing N months
  - `precip_window_mean_mm DOUBLE PRECISION` — historical mean of `precip_window_mm` for this same calendar-month-end across all years
  - `precip_window_std_mm DOUBLE PRECISION` — historical std for the same
  - `z_score DOUBLE PRECISION` — `(precip_window_mm - mean) / std`
  - `category TEXT` — derived: `extreme drought` (z <= -2), `severe` (-2..-1.5),
    `moderate` (-1.5..-1), `near normal` (-1..1), `moderately wet` (1..1.5),
    `severely wet` (1.5..2), `extremely wet` (z >= 2)
- **Materialization**: `view`.
- **Note**: column comment will flag this as "z-score variant of SPI" so a
  domain reviewer doesn't expect a true gamma-fit SPI.

## Materialization summary

| Mart | Type | Depends on |
|---|---|---|
| `daily_summary` | view | `silver.weather_data` |
| `daily_climatology` | view | `gold.daily_summary` |
| `extreme_events` | table | `silver.weather_data`, `gold.daily_summary` |
| `growing_degree_days` | view | `gold.daily_summary`, `seeds.crop_base_temperatures` |
| `reference_et` | view | `gold.daily_summary` |
| `drought_index` | view | `gold.daily_summary` |

`dbt_project.yml` already defaults gold models to `view` (directory-level
config). Only `extreme_events.sql` overrides with
`{{ config(materialized='table') }}`.

## Silver index (BRIN on recorded_at)

Add a BRIN index on `recorded_at` per yearly partition. Time-series data is
naturally clustered by ingestion order, which is BRIN's sweet spot. Storage is
a few tens of KB per partition. Speeds up any date-range query from gold views,
the API, and the dashboard.

Implementation: extend `macros/materializations/partitioned_table.sql` to issue
the index creation after the partition-creation loop:

```jinja
{% call statement('create_brin_' ~ year) %}
  CREATE INDEX IF NOT EXISTS
    {{ target_relation.identifier }}_{{ year }}_recorded_at_brin
    ON {{ target_relation.schema }}.{{ target_relation.identifier }}_{{ year }}
    USING brin (recorded_at);
{% endcall %}
```

`IF NOT EXISTS` keeps it idempotent and `TRUNCATE` preserves indexes, so the
index survives the daily reload.

## Files to add

```
models/gold/
  schema.yml
  daily_summary.sql
  daily_climatology.sql
  extreme_events.sql
  growing_degree_days.sql
  reference_et.sql
  drought_index.sql
seeds/
  crop_base_temperatures.csv
```

## Verification

For each mart after `dbt run --target prod`:

1. `dbt test` — schema tests defined in `models/gold/schema.yml` (not_null on
   grain columns, accepted_values on `category` columns, relationships back to
   silver).
2. Spot checks via psql:
   - `SELECT count(*), min(obs_date), max(obs_date) FROM gold.daily_summary;`
     — expect ~10700 rows for 30 years.
   - `SELECT * FROM gold.daily_climatology WHERE doy = 1 LIMIT 1;` — sanity-check
     percentiles.
   - `SELECT * FROM gold.extreme_events WHERE category = 'hottest_day' ORDER BY rank LIMIT 3;`
   - `SELECT crop, max(gdd_cumulative_year) FROM gold.growing_degree_days GROUP BY 1;`
     — expect sugarcane lowest (high base), corn/soy highest.
   - `SELECT obs_date, et0_mm_day FROM gold.reference_et ORDER BY obs_date DESC LIMIT 5;`
     — sanity: typical Piracicaba ET0 is 3-6 mm/day.
   - `SELECT obs_month, z_score, category FROM gold.drought_index WHERE window_months = 12 ORDER BY obs_month DESC LIMIT 6;`

3. BRIN index sanity check:
   `SELECT count(*) FROM pg_indexes WHERE schemaname='silver' AND indexdef ILIKE '%brin%recorded_at%';`
   — expect one row per yearly partition.

4. Dashboard / API integration: extend `dashboard/` with one panel per gold
   mart (climatology comparison, GDD per crop, ET0 + water balance line chart,
   drought-index heat strip) and add `/api/v1/gold/<mart>` endpoints.

## Out of scope (future work)

- True gamma-fit SPI (would require offline Python step writing a seed).
- Hourly Penman-Monteith ET0 (requires per-hour Rn integration).
- Forecasting / ML features.
- Crop water requirement (Kc * ET0) — needs crop coefficient curves per
  growth stage; one level deeper than this scope.
