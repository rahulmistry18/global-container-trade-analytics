{{ config(materialized='table') }}

select
    container_type,
    sum(volume) as total_volume
from {{ ref('stg_container_moves') }}
group by container_type
order by total_volume desc
