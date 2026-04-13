/*
  Custom dbt materialization: partitioned_table
  ─────────────────────────────────────────────
  Creates a PostgreSQL PARTITION BY RANGE (recorded_at) table with one child
  partition per calendar year (2016 → current_year + 1). On every dbt run it:
    1. Creates the parent table and all year partitions (IF NOT EXISTS — idempotent)
    2. TRUNCATEs the parent (cascades to all children) for a full reload
    3. INSERTs the transformed rows from the model SQL

  Usage in a model:
    {{ config(materialized='partitioned_table') }}
*/
{% materialization partitioned_table, adapter='postgres' %}

  {% set target_relation = this %}

  {{ run_hooks(pre_hooks) }}

  -- 1. Create the partitioned parent table (schema defined here once)
  {% call statement('create_parent') %}
    CREATE TABLE IF NOT EXISTS {{ target_relation }} (
      recorded_at        TIMESTAMPTZ      NOT NULL,
      equipment_era      TEXT,
      "Tar_AVG"          DOUBLE PRECISION,
      "UR_inst"          DOUBLE PRECISION,
      "Vvento_ms_AVG"    DOUBLE PRECISION,
      "Dvento_G"         DOUBLE PRECISION,
      "Qg_AVG"           DOUBLE PRECISION,
      "PAR_AVG"          DOUBLE PRECISION,
      "Rn_Avg"           DOUBLE PRECISION,
      "Chuva_mm"         DOUBLE PRECISION,
      "Dvento_SD1_WVT"   DOUBLE PRECISION,
      "BattV_Avg"        DOUBLE PRECISION,
      "Patm_kPa_AVG"     DOUBLE PRECISION,
      "rQg_AVG"          DOUBLE PRECISION,
      "Qatm_AVG"         DOUBLE PRECISION,
      "Qsup_AVG"         DOUBLE PRECISION,
      "Boc_AVG"          DOUBLE PRECISION,
      "Bol_AVG"          DOUBLE PRECISION,
      "Albedo_Avg"       DOUBLE PRECISION,
      "QatmC_AVG"        DOUBLE PRECISION,
      "QsupC_AVG"        DOUBLE PRECISION,
      "Vvento_ms_S_WVT"  DOUBLE PRECISION,
      "Dvento_D1_WVT"    DOUBLE PRECISION,
      "PainelT"          DOUBLE PRECISION,
      _source_url        TEXT
    ) PARTITION BY RANGE (recorded_at);
  {% endcall %}

  -- 2. Create year partitions from 2016 to current_year + 1 (idempotent)
  {% set current_year = modules.datetime.date.today().year %}
  {% for year in range(2016, current_year + 2) %}
    {% call statement('create_partition_' ~ year) %}
      CREATE TABLE IF NOT EXISTS
        {{ target_relation.schema }}.{{ target_relation.identifier }}_{{ year }}
        PARTITION OF {{ target_relation }}
        FOR VALUES FROM ('{{ year }}-01-01') TO ('{{ year + 1 }}-01-01');
    {% endcall %}
  {% endfor %}

  -- 3. Full reload: truncate all partitions then insert fresh data
  {% call statement('truncate') %}
    TRUNCATE {{ target_relation }};
  {% endcall %}

  {% call statement('main') %}
    INSERT INTO {{ target_relation }}
    {{ sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
