{{ config(materialized='incremental', unique_key='event_id') }}

with base as (

    select
        container_id,
        event_type,
        event_time,
        location_code,
        port_code,
        vessel_voyage,
        load_status,
        remarks,
        created_at,
        updated_at

    from {{ ref('stg_container_events') }}

),

cleaned as (

    select
        container_id,
        event_type,
        cast(event_time as timestamp) as event_time,
        upper(port_code) as port_code,
        upper(location_code) as location_code,
        vessel_voyage,
        load_status,
        remarks,
        created_at,
        updated_at
    from base
),

ordered as (

    select
        *,
        row_number() over (partition by container_id order by event_time) as event_sequence
    from cleaned
),

with_next as (

    select
        *,
        lead(event_type) over (partition by container_id order by event_time) as next_event_type,
        lead(event_time) over (partition by container_id order by event_time) as next_event_time,
        datediff(
            'hour',
            event_time,
            lead(event_time) over (partition by container_id order by event_time)
        ) as hours_to_next_event
    from ordered
)

select *
from with_next
