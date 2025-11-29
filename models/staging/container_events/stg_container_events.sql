{{ config(materialized='view') }}

select 
    container_id,
    event_type,
    event_time,
    location_code,
    vessel_voyage,
    port_code,
    remarks,
    load_status,
    created_at,
    updated_at
from {{ source('raw', 'container_events') }}
