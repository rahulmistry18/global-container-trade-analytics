{{ config(
    materialized='table'
) }}

select
    container_id,
    cast(move_date as date)      as move_date,
    port_code,
    move_type,
    container_type
from {{ ref('container_moves') }}
