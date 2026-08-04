{{ config(materialized='view') }}

select
    match_id,
    coalesce(
        try_cast(temperature_2m as double),
        0.0
    ) as temp_max_c,
    coalesce(
        try_cast(precipitation as double),
        0.0
    ) as precip_mm,
    0.0 as rain_mm
from {{ source('cricsheet', 'weather') }}
