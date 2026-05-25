{{ config(materialized='table') }}

-- Career-level fact table. Long format — one row per (player, metric_name) —
-- lets BI tools pivot freely without re-materializing N wide tables.

with batting_long as (
    select player, 'matches'         as metric_name, cast(matches as double)         as metric_value from {{ ref('stg_batting') }}
    union all
    select player, 'runs',                            cast(runs as double)            from {{ ref('stg_batting') }}
    union all
    select player, 'batting_average',                 average                          from {{ ref('stg_batting') }}
    union all
    select player, 'strike_rate',                     strike_rate                      from {{ ref('stg_batting') }}
    union all
    select player, 'hundreds',                        cast(hundreds as double)         from {{ ref('stg_batting') }}
    union all
    select player, 'fifties',                         cast(fifties as double)          from {{ ref('stg_batting') }}
    union all
    select player, 'fours',                           cast(fours as double)            from {{ ref('stg_batting') }}
    union all
    select player, 'sixes',                           cast(sixes as double)            from {{ ref('stg_batting') }}
),
bowling_long as (
    select player, 'wickets'             as metric_name, cast(wickets as double)             as metric_value from {{ ref('stg_bowling') }}
    union all
    select player, 'bowling_average',                     bowling_average                     from {{ ref('stg_bowling') }}
    union all
    select player, 'economy_rate',                        economy_rate                        from {{ ref('stg_bowling') }}
    union all
    select player, 'bowling_strike_rate',                 bowling_strike_rate                 from {{ ref('stg_bowling') }}
)
select * from batting_long where metric_value is not null
union all
select * from bowling_long where metric_value is not null
