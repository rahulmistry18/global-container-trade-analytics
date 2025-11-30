{{ config(
    materialized='table'
) }}

select
    date_trunc('month', move_date) as month,
    count(*) as total_moves
from {{ ref('stg_container_moves') }}
group by 1
order by 1
