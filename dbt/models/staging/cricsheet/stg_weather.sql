{{ config(materialized='view') }}

select
    match_id,
    date as weather_date,
    temp_max_c,
    precip_mm,
    rain_mm
from {{ source('cricsheet', 'weather') }}
