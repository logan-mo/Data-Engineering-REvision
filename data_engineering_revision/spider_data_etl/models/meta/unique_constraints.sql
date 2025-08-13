{{ config(materialized='table', schema='spider_meta') }}
select
    tc.constraint_name,
    tc.table_schema as src_schema,
    tc.table_name,
    tc.constraint_type,
    group_concat(kcu.column_name order by kcu.ordinal_position) as columns
from information_schema.table_constraints tc
join information_schema.key_column_usage kcu
    on kcu.constraint_schema = tc.constraint_schema
    and kcu.constraint_name   = tc.constraint_name
    and kcu.table_schema      = tc.table_schema
    and kcu.table_name        = tc.table_name
where tc.constraint_type in ('PRIMARY KEY','UNIQUE')
    and tc.table_schema not in ('spider_bronze','spiderman')
group by 1,2,3,4
