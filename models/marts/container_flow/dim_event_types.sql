{{ config(materialized='table') }}

with events as (
    select distinct event_type
    from {{ ref('stg_container_moves') }}
)

select
    event_type,
    case
        when event_type ilike '%load%' then 'Loading'
        when event_type ilike '%discharge%' then 'Discharge'
        when event_type ilike '%trans%' then 'Transshipment'
        when event_type ilike '%gate%' then 'Gate Move'
        when event_type ilike '%arrival%' then 'Arrival'
        when event_type ilike '%depart%' then 'Departure'
        else 'Other'
    end as event_category
from events
order by 1;
