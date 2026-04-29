-- dbt materialization: PARTITION BY RANGE (recorded_at) parent + yearly children
-- (1997 to current_year + 1). Full reload: TRUNCATE cascades, then INSERT.
{% materialization partitioned_table, adapter='postgres' %}

  {% set target_relation = this %}

  {% set columns = [
    'recorded_at',
    'equipment_era',
    '"Tar_AVG"',
    '"UR_inst"',
    '"Vvento_ms_AVG"',
    '"Dvento_G"',
    '"Qg_AVG"',
    '"PAR_AVG"',
    '"Rn_Avg"',
    '"Chuva_mm"',
    '"Dvento_SD1_WVT"',
    '"BattV_Avg"',
    '"Patm_kPa_AVG"',
    '"rQg_AVG"',
    '"Qatm_AVG"',
    '"Qsup_AVG"',
    '"Boc_AVG"',
    '"Bol_AVG"',
    '"Albedo_Avg"',
    '"QatmC_AVG"',
    '"QsupC_AVG"',
    '"Vvento_ms_S_WVT"',
    '"Dvento_D1_WVT"',
    '"PainelT"',
    '_source_url'
  ] %}

  {{ run_hooks(pre_hooks) }}

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

  {% set current_year = modules.datetime.date.today().year %}
  {% for year in range(1997, current_year + 2) %}
    {% call statement('create_partition_' ~ year) %}
      CREATE TABLE IF NOT EXISTS
        {{ target_relation.schema }}.{{ target_relation.identifier }}_{{ year }}
        PARTITION OF {{ target_relation }}
        FOR VALUES FROM ('{{ year }}-01-01') TO ('{{ year + 1 }}-01-01');
    {% endcall %}
    -- BRIN: time-series data is naturally clustered by recorded_at (ingest order),
    -- which is BRIN's sweet spot. Tens of KB per partition; survives TRUNCATE.
    {% call statement('create_brin_' ~ year) %}
      CREATE INDEX IF NOT EXISTS
        {{ target_relation.identifier }}_{{ year }}_recorded_at_brin
        ON {{ target_relation.schema }}.{{ target_relation.identifier }}_{{ year }}
        USING brin (recorded_at);
    {% endcall %}
  {% endfor %}

  {% call statement('truncate') %}
    TRUNCATE {{ target_relation }};
  {% endcall %}

  {% call statement('main') %}
    INSERT INTO {{ target_relation }} ({{ columns | join(', ') }})
    {{ sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  -- Required: dbt-postgres opens an explicit transaction for the materialization
  -- (autocommit=False). Without this commit, the TRUNCATE+INSERT rolls back
  -- when the connection is reused, leaving stale data visible to other sessions.
  {{ adapter.commit() }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
