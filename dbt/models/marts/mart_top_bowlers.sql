{{ config(materialized='table') }}

-- Business mart: top bowlers ranked by PCA composite score.
-- Eligibility floor: ≥10 wickets — filters out players with one lucky spell.

with eligible as (
    select *
    from {{ ref('dim_player') }}
    where player_role in ('bowler', 'all_rounder')
      and wickets is not null
      and wickets >= 10
)
select
    row_number() over (order by pca_bowling_score desc) as rank,
    player,
    country_tag,
    career_start_year,
    career_end_year,
    matches,
    wickets,
    pca_bowling_score as pca_score
from eligible
order by pca_bowling_score desc
