{{ config(materialized='table') }}

with balls as (
    select * from {{ ref('stg_cricsheet_balls') }}
),
matches as (
    select * from {{ ref('stg_cricsheet_matches') }}
),
weather as (
    select * from {{ ref('stg_weather') }}
)

select
    balls.match_id,
    matches.match_date,
    {{ dbt_utils.generate_surrogate_key(['matches.venue', 'matches.city']) }} as venue_id,
    {{ dbt_utils.generate_surrogate_key(['weather.temp_max_c', 'weather.precip_mm']) }} as weather_id,
    {{ dbt_utils.generate_surrogate_key(['balls.batter']) }} as batter_id,
    {{ dbt_utils.generate_surrogate_key(['balls.bowler']) }} as bowler_id,
    balls.batting_team,
    balls.over_num,
    balls.runs_batter,
    balls.runs_extras,
    balls.runs_total,
    balls.wicket_kind,
    case when balls.player_out is not null then 1 else 0 end as is_wicket
from balls
left join matches on balls.match_id = matches.match_id
left join weather on balls.match_id = weather.match_id
