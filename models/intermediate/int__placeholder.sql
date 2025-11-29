-- Placeholder intermediate model
-- Delete or replace as soon as you add a real intermediate model

select *
from {{ ref('stg_container_moves') }}
