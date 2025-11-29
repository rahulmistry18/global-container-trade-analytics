{{ config(materialized='table') }}

with monthly_data as (
    select
        date_trunc('month', date) as month,
        container_type,
        sum(volume) as total_volume
    from {{ ref('stg_container_moves') }}
    group by month, container_type
)

select *
from monthly_data
order by month, container_type
