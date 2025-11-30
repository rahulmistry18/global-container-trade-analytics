{{ config(materialized='table') }}

select
    port_code as destination_port,
    count(*) as total_moves
from {{ ref('stg_container_moves') }}
group by 1
order by 2 desc
