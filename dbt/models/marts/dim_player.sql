{{ config(materialized='table') }}

-- Conformed player dimension: one row per player, joining their batting and
-- bowling records. Most players appear in one source; all-rounders in both.
-- A FULL OUTER JOIN preserves every player from either source.
--
-- The `player_role` column is derived heuristically:
--   - "batsman" if batting record present and bowling absent (or <50 wickets)
--   - "bowler" if bowling record present and batting absent (or <500 runs)
--   - "all_rounder" if both meaningful
--   - "specialist_keeper" not derivable from these sources; left as 'unknown'

with batting as (
    select * from {{ ref('stg_batting') }}
),
bowling as (
    select * from {{ ref('stg_bowling') }}
),
joined as (
    select
        coalesce(b.player, bw.player)                                       as player,
        coalesce(b.country_tag, bw.country_tag)                             as country_tag,
        coalesce(b.career_start_year, bw.career_start_year)                 as career_start_year,
        coalesce(b.career_end_year, bw.career_end_year)                     as career_end_year,
        coalesce(b.matches, bw.matches)                                     as matches,
        coalesce(b.innings, bw.innings)                                     as innings,
        b.runs                                                              as runs,
        b.average                                                           as batting_average,
        b.strike_rate                                                       as batting_strike_rate,
        b.hundreds,
        b.fifties,
        b.fours,
        b.sixes,
        bw.wickets,
        bw.bowling_average,
        bw.economy_rate,
        bw.bowling_strike_rate
    from batting b
    full outer join bowling bw
        on b.player = bw.player
       and b.career_start_year is not distinct from bw.career_start_year
),
classified as (
    select
        *,
        case
            when wickets is null or wickets < 50 then 'batsman'
            when runs is null    or runs    < 500 then 'bowler'
            else 'all_rounder'
        end as player_role,
        {{ compute_pca_batsman(
              'runs', 'batting_average', 'batting_strike_rate',
              'fours', 'sixes', 'hundreds', 'fifties') }} as pca_batting_score,
        {{ compute_pca_bowler(
              'wickets', 'bowling_average', 'economy_rate', 'bowling_strike_rate'
        ) }} as pca_bowling_score
    from joined
)
select
    player,
    country_tag,
    career_start_year,
    career_end_year,
    matches,
    innings,
    runs,
    wickets,
    player_role,
    -- Use the role-appropriate PCA as the headline score; fall back to whichever exists.
    case
        when player_role in ('batsman', 'all_rounder') then pca_batting_score
        when player_role = 'bowler'                    then pca_bowling_score
        else coalesce(pca_batting_score, pca_bowling_score)
    end as pca_score,
    pca_batting_score,
    pca_bowling_score
from classified
