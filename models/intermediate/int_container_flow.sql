{{ config(materialized='incremental') }}

with events as (
    select
        container_id,
        event_time,
        event_type,
        port_code,
        row_number() over (
            partition by container_id
            order by event_time
        ) as event_sequence
    from {{ ref('stg_container_events') }}
    where container_id is not null
      and event_time is not null
)

select *
from events
