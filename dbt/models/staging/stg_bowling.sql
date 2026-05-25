{{ config(materialized='view') }}

-- Staging: thin view over Silver bowling. See stg_batting.sql for rationale.

select
    player,
    country_tag,
    career_start_year,
    career_end_year,
    cast(matches as integer)              as matches,
    cast(innings as integer)              as innings,
    cast(balls_bowled as integer)         as balls_bowled,
    cast(runs_conceded as integer)        as runs_conceded,
    cast(wickets as integer)              as wickets,
    cast(bowling_average as double)       as bowling_average,
    cast(economy_rate as double)          as economy_rate,
    cast(bowling_strike_rate as double)   as bowling_strike_rate,
    cast(four_wicket_hauls as integer)    as four_wicket_hauls,
    cast(five_wicket_hauls as integer)    as five_wicket_hauls,
    best_bowling_innings
from {{ source('silver', 'bowling') }}
where wickets > 0
qualify row_number() over (partition by player order by career_start_year asc nulls last) = 1
