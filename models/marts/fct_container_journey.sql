{{ config(materialized='table') }}

with journeys as (
    select
        cm.container_id,
        min(ce.event_time) as first_event_time,
        max(ce.event_time) as last_event_time,
        count(*) as total_events,
        datediff('day', min(ce.event_time), max(ce.event_time)) as total_transit_days
    from {{ ref('stg_container_events') }} ce
    join {{ ref('stg_container_moves') }} cm
        on ce.container_id = cm.container_id
    group by cm.container_id
)

select *
from journeys
