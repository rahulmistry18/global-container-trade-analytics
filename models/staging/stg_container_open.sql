{{ config(materialized='table') }}

select *
from {{ ref('stg_container_moves') }}
where container_type = 'open'
