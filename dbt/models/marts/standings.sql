-- standings: incremental mart model.
--
-- Materialisation: incremental (table on first run, INSERT new seasons on reruns).
-- The unique key is (season, team) — if new seasons arrive, dbt merges them in
-- without reprocessing historical data.
--
-- The UNION ALL logic is the same SQL that ran inside spark.sql() in v4 —
-- dbt compiles this to a CREATE TABLE AS SELECT on the first run, then to
-- INSERT … WHERE NOT EXISTS on subsequent runs.

{{
  config(
    materialized = 'incremental',
    unique_key   = ['season', 'team'],
    on_schema_change = 'sync_all_columns'
  )
}}

with home_perspective as (
    select
        season,
        home_team                                   as team,
        count(*)                                    as played,
        sum(case when result_type = 'home_win' then 1 else 0 end) as won,
        sum(case when result_type = 'draw'     then 1 else 0 end) as drawn,
        sum(case when result_type = 'away_win' then 1 else 0 end) as lost,
        sum(home_goals)                             as gf,
        sum(away_goals)                             as ga
    from {{ ref('stg_matches') }}
    {% if is_incremental() %}
        -- only process seasons not yet in the standings table
        where season not in (select distinct season from {{ this }})
    {% endif %}
    group by season, home_team
),

away_perspective as (
    select
        season,
        away_team                                   as team,
        count(*)                                    as played,
        sum(case when result_type = 'away_win' then 1 else 0 end) as won,
        sum(case when result_type = 'draw'     then 1 else 0 end) as drawn,
        sum(case when result_type = 'home_win' then 1 else 0 end) as lost,
        sum(away_goals)                             as gf,
        sum(home_goals)                             as ga
    from {{ ref('stg_matches') }}
    {% if is_incremental() %}
        where season not in (select distinct season from {{ this }})
    {% endif %}
    group by season, away_team
),

combined as (
    select * from home_perspective
    union all
    select * from away_perspective
),

aggregated as (
    select
        season,
        team,
        sum(played)                                 as played,
        sum(won)                                    as won,
        sum(drawn)                                  as drawn,
        sum(lost)                                   as lost,
        sum(gf)                                     as gf,
        sum(ga)                                     as ga,
        sum(gf) - sum(ga)                           as gd,
        sum(won) * 3 + sum(drawn)                   as points
    from combined
    group by season, team
)

select
    season,
    team,
    played,
    won,
    drawn,
    lost,
    gf,
    ga,
    gd,
    points,
    row_number() over (
        partition by season
        order by points desc, gd desc, gf desc
    )                                               as position
from aggregated
