{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('int_container_flow') }}
),

-- Dimension tables
dim_ports as (
    select port_code
    from {{ ref('dim_ports') }}
),

dim_event as (
    select event_type, event_category
    from {{ ref('dim_event_types') }}
),

dim_status as (
    select current_status, status_group
    from {{ ref('dim_container_status') }}
)

select
    b.container_id,
    b.event_time,
    b.event_type,
    e.event_category,
    b.port_code,
    p.port_code as port_key,
    b.country_code,
    b.move_sequence,
    b.current_status,
    s.status_group
from base b
left join dim_event e
    on b.event_type = e.event_type
left join dim_ports p
    on b.port_code = p.port_code
left join dim_status s
    on b.current_status = s.current_status;
