-- dim_users.sql
select * from {{ ref('stg_users') }}