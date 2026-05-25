{{ config(materialized='view') }}

-- Staging: thin view over Silver batting. Renames and explicit types only —
-- business logic lives in marts. The view layer means changes here propagate
-- without rematerializing terabytes.
--
-- transform_batting always emits the full Silver schema, so this view can
-- reference every column directly without coalesce or null-injection hacks.

select
    player,
    country_tag,
    career_start_year,
    career_end_year,
    cast(matches as integer)              as matches,
    cast(innings as integer)              as innings,
    cast(not_outs as integer)             as not_outs,
    cast(runs as integer)                 as runs,
    cast(average as double)               as average,
    cast(balls_faced as integer)          as balls_faced,
    cast(strike_rate as double)           as strike_rate,
    cast(hundreds as integer)             as hundreds,
    cast(fifties as integer)              as fifties,
    cast(ducks as integer)                as ducks,
    cast(fours as integer)                as fours,
    cast(sixes as integer)                as sixes,
    cast(high_score as integer)           as high_score,
    cast(high_score_not_out as boolean)   as high_score_not_out
from {{ source('silver', 'batting') }}
where runs is not null  -- enforced by Pandera too; defense-in-depth
qualify row_number() over (partition by player order by career_start_year asc nulls last) = 1
