{{ config(materialized='table') }}

select
    origin_port,
    container_type,
    sum(volume) as total_volume
from {{ ref('stg_container_moves') }}
group by origin_port, container_type
order by origin_port
