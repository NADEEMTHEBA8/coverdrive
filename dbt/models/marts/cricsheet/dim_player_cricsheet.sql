{{ config(materialized='table') }}

with all_players as (
    select batter as player_name from {{ ref('stg_cricsheet_balls') }}
    union
    select bowler as player_name from {{ ref('stg_cricsheet_balls') }}
    union
    select player_out as player_name from {{ ref('stg_cricsheet_balls') }} where player_out is not null
)
select distinct
    {{ dbt_utils.generate_surrogate_key(['player_name']) }} as player_id,
    player_name
from all_players
where player_name is not null
