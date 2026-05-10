-- stg_matches: light cleaning layer on top of the raw matches source.
-- Casts types, enforces non-null contracts, and renames nothing —
-- downstream mart models reference this view, not the raw table.

with source as (
    select * from {{ source('raw', 'matches') }}
),

cleaned as (
    select
        match_id::text                     as match_id,
        season::text                       as season,
        matchday::integer                  as matchday,
        home_team::text                    as home_team,
        away_team::text                    as away_team,
        home_goals::integer                as home_goals,
        away_goals::integer                as away_goals,

        -- derived convenience columns
        case
            when home_goals > away_goals then home_team
            when away_goals > home_goals then away_team
            else null
        end                                as winning_team,

        case
            when home_goals = away_goals   then 'draw'
            when home_goals > away_goals   then 'home_win'
            else 'away_win'
        end                                as result_type
    from source
    where match_id is not null            -- guard against partially-loaded rows
)

select * from cleaned
