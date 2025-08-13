-- macros/mirror_all_to_bronze.sql
{% macro mirror_all_to_bronze(
    dest_db='spider_bronze',
    include_dbs=[],
    exclude_dbs=[],
    include_views=true,
    materialize_as='table',
    dry_run=false
) %}
  {# Skip system databases #}
  {% set system_dbs = ['information_schema','mysql','performance_schema','sys'] %}

  {# Ensure destination DB exists #}
  {% set create_db_sql %}CREATE DATABASE IF NOT EXISTS `{{ dest_db }}`{% endset %}
  {% if not dry_run %}
    {{ run_query(create_db_sql) }}
  {% endif %}
  {{ log('Ensured database ' ~ dest_db ~ ' exists', info=True) }}

  {# Discover all databases #}
  {% set dbs_sql %}SELECT schema_name FROM information_schema.schemata{% endset %}
  {% set dbs_tbl = run_query(dbs_sql) %}
  {% set dbs = dbs_tbl.columns[0].values() if dbs_tbl is not none else [] %}

  {% for db in dbs %}
    {% if db in system_dbs or db == dest_db %}{% continue %}{% endif %}
    {% if include_dbs and db not in include_dbs %}{% continue %}{% endif %}
    {% if db in exclude_dbs %}{% continue %}{% endif %}

    {{ log('Scanning database: ' ~ db, info=True) }}

    {# List tables/views #}
    {% if include_views %}
      {% set tbls_sql %}
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{{ db }}'
          AND table_type IN ('BASE TABLE','VIEW')
      {% endset %}
    {% else %}
      {% set tbls_sql %}
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{{ db }}'
          AND table_type = 'BASE TABLE'
      {% endset %}
    {% endif %}

    {% set tbls_tbl = run_query(tbls_sql) %}
    {% set tbls = tbls_tbl.columns[0].values() if tbls_tbl is not none else [] %}

    {% for t in tbls %}
      {% set bronze_name = db ~ '__' ~ t %}

      {% if materialize_as == 'view' %}
        {% set ddl %}
          CREATE OR REPLACE VIEW `{{ dest_db }}`.`{{ bronze_name }}`
          AS SELECT * FROM `{{ db }}`.`{{ t }}`;
        {% endset %}

        {% if dry_run %}
          {{ log('DRY RUN: ' ~ ddl|replace('\n',' '), info=True) }}
        {% else %}
          {{ run_query(ddl) }}
          {{ log('Created view ' ~ dest_db ~ '.' ~ bronze_name, info=True) }}
        {% endif %}

      {% elif materialize_as == 'table' %}
        {% set drop_sql %}DROP TABLE IF EXISTS `{{ dest_db }}`.`{{ bronze_name }}`{% endset %}
        {% set create_sql %}
          CREATE TABLE `{{ dest_db }}`.`{{ bronze_name }}`
          AS SELECT * FROM `{{ db }}`.`{{ t }}`;
        {% endset %}

        {% if dry_run %}
          {{ log('DRY RUN: ' ~ drop_sql, info=True) }}
          {{ log('DRY RUN: ' ~ create_sql|replace('\n',' '), info=True) }}
        {% else %}
          {{ run_query(drop_sql) }}
          {{ run_query(create_sql) }}
          {{ log('Created table ' ~ dest_db ~ '.' ~ bronze_name, info=True) }}
        {% endif %}

      {% else %}
        {% do exceptions.raise_compiler_error("materialize_as must be 'view' or 'table'") %}
      {% endif %}

    {% endfor %}
  {% endfor %}

  {{ log('Mirror complete.', info=True) }}
{% endmacro %}
