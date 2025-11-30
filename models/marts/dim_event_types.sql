{{ config(materialized='table') }}

with base as (
    select distinct event_type
    from {{ ref('stg_container_events') }}
)

select
    event_type,
    case
        when event_type ilike '%gate%' then 'Gate Operation'
        when event_type ilike '%load%' then 'Loading'
        when event_type ilike '%discharg%' then 'Discharge'
        when event_type ilike '%arriv%' then 'Arrival'
        when event_type ilike '%depart%' then 'Departure'
        when event_type ilike '%custom%' then 'Customs Hold'
        else 'Other'
    end as event_category
from base
order by 1
