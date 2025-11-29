{{ config(materialized='table') }}

with status_values as (
    select distinct current_status
    from {{ ref('fct_container_journey') }}
)

select
    current_status,
    case
        when current_status ilike '%completed%' then 'Completed'
        when current_status ilike '%in transit%' then 'In Transit'
        when current_status ilike '%hold%' then 'On Hold'
        when current_status ilike '%pending%' then 'Pending'
        else 'Other'
    end as status_group
from status_values
order by 1;
