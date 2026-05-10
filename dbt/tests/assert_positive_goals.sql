-- assert_positive_goals.sql
-- Singular (custom) dbt test: fails if any match has negative goal values.
-- dbt treats any rows returned by a singular test as test failures.

select
    match_id,
    home_goals,
    away_goals
from {{ ref('stg_matches') }}
where home_goals < 0
   or away_goals < 0
