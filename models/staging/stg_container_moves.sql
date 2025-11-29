-- models/staging/stg_container_moves.sql

{{ config(
    materialized='table'
) }}

with raw as (

    select
        '2025-01-01' as date,
        'Shanghai' as origin_port,
        'Rotterdam' as destination_port,
        'dry' as container_type,
        1000 as volume
    union all
    select
        '2025-01-02', 'Hamburg', 'New York', 'reefer', 500
    union all
    select
        '2025-01-03', 'Singapore', 'Los Angeles', 'open', 300
    union all
    select
        '2025-01-04', 'Shanghai', 'Los Angeles', 'dry', 700
    union all
    select
        '2025-01-05', 'Hamburg', 'Rotterdam', 'reefer', 200

)

select *
from raw
