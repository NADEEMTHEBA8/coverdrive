{{ config(materialized='view') }}

select
    match_id,
    batting_team,
    over_num,
    batter,
    bowler,
    runs_batter,
    runs_extras,
    runs_total,
    wicket_kind,
    player_out
from {{ source('cricsheet', 'cricsheet_balls') }}
