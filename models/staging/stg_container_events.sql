{{ config(materialized='view') }}

select
    container_id,
    cast(event_time as timestamp) as event_time,
    event_type,
    port_code,
    country_code
from {{ ref('container_events') }}
