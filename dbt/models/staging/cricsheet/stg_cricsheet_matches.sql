{{ config(materialized='view') }}

select
    match_id,
    match_date,
    venue,
    city,
    team1,
    team2,
    match_type
from {{ source('cricsheet', 'cricsheet_matches') }}
