{{ config(materialized='table') }}

with base as (

    select
        container_id,
        event_type,
        event_time,
        event_sequence,
        port_code,
        next_event_time,
        hours_to_next_event
    from {{ ref('int_container_flow') }}
),

journey as (

    select
        container_id,

        -- First and last event
        min(event_time) as first_event_time,
        max(event_time) as last_event_time,

        -- First and last port
        first_value(port_code) over (
            partition by container_id
            order by event_time
        ) as first_port,

        last_value(port_code) over (
            partition by container_id
            order by event_time
            rows between unbounded preceding and unbounded following
        ) as last_port,

        -- Total transit duration
        datediff('hour',
            min(event_time),
            max(event_time)
        ) as total_transit_hours,

        datediff('day',
            min(event_time),
            max(event_time)
        ) as total_transit_days,

        -- Number of events
        count(*) as total_events,

        -- Total dwell time between events
        sum(hours_to_next_event) as total_dwell_hours,

        -- Current status (last event)
        last_value(event_type) over (
            partition by container_id
            order by event_sequence
            rows between unbounded preceding and unbounded following
        ) as current_status

    from base
    group by container_id
)

select * from journey;
