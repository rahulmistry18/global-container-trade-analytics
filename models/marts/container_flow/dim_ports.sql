{{ config(materialized='table') }}

select
    port_code,
    country_code
from {{ ref('int_container_journey') }}
group by 1,2
order by 1;
