{{ config(materialized='table', schema='spider_meta') }}
select
    kcu.constraint_name,
    kcu.constraint_schema                as src_schema,
    kcu.table_name                       as child_table,
    kcu.column_name                      as child_column,
    kcu.referenced_table_schema          as parent_schema,
    kcu.referenced_table_name            as parent_table,
    kcu.referenced_column_name           as parent_column,
    rc.update_rule,
    rc.delete_rule
from information_schema.key_column_usage kcu
join information_schema.referential_constraints rc
    on rc.constraint_schema = kcu.constraint_schema
    and rc.constraint_name   = kcu.constraint_name
where kcu.referenced_table_name is not null
