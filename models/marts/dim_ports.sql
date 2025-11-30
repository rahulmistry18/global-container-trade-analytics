{{ config(materialized='table') }}

select distinct
    port_code
from {{ ref('stg_container_moves') }}
