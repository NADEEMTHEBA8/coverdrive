{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['venue', 'city']) }} as venue_id,
    venue as venue_name,
    city
from {{ ref('stg_cricsheet_matches') }}
where venue is not null
