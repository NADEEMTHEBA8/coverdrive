{{ config(materialized='table') }}

-- Business mart: top batsmen ranked by PCA composite score.
-- Powers the /api/v1/rankings/batsmen endpoint and the scout report.
--
-- Eligibility: at least 20 matches AND non-null runs. Without this floor the
-- top of the list is junk — players with one outlier innings ranked above
-- consistent multi-year careers.

with eligible as (
    select *
    from {{ ref('dim_player') }}
    where player_role in ('batsman', 'all_rounder')
      and matches >= 20
      and runs is not null
)
select
    row_number() over (order by pca_batting_score desc) as rank,
    player,
    country_tag,
    career_start_year,
    career_end_year,
    matches,
    innings,
    runs,
    pca_batting_score as pca_score
from eligible
order by pca_batting_score desc
