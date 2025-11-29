{{ config(materialized='table') }}

with port_data as (
    select
        origin_port,
        container_type,
        sum(volume) as total_volume
    from {{ ref('stg_container_moves') }}
    group by origin_port, container_type
),

ranked as (
    select
        *,
        rank() over (partition by container_type order by total_volume desc) as rank
    from port_data
)

select *
from ranked
where rank <= 3
order by container_type, rank
