{{ config(materialized='table') }}

select
    destination_port,
    container_type,
    sum(volume) as total_volume
from {{ ref('stg_container_moves') }}
group by destination_port, container_type
order by destination_port
