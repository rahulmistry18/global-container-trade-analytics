{{ config(materialized='table') }}

select distinct
    event_type as current_status
from {{ ref('stg_container_events') }}
