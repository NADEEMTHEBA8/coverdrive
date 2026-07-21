{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['temp_max_c', 'precip_mm']) }} as weather_id,
    temp_max_c,
    precip_mm,
    rain_mm,
    case
        when precip_mm > 5 then 'Rainy'
        when temp_max_c > 30 then 'Hot'
        when temp_max_c < 15 then 'Cold'
        else 'Moderate'
    end as weather_condition
from {{ ref('stg_weather') }}
where temp_max_c is not null
