-- schema.sql
-- Creates all tables, indexes, and analytical views for the football pipeline.
-- Designed for PostgreSQL 15+.

-- ── Raw match facts ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS matches (
    match_id         TEXT        PRIMARY KEY,
    date             DATE        NOT NULL,
    season           SMALLINT    NOT NULL,
    home_team        TEXT        NOT NULL,
    away_team        TEXT        NOT NULL,
    home_goals       SMALLINT    NOT NULL CHECK (home_goals >= 0),
    away_goals       SMALLINT    NOT NULL CHECK (away_goals >= 0),
    home_shots       SMALLINT,
    away_shots       SMALLINT,
    home_possession  NUMERIC(5,2),
    away_possession  NUMERIC(5,2),
    stadium          TEXT,
    referee          TEXT,
    -- Derived columns (computed in Python transform, stored for query speed)
    result           TEXT        NOT NULL CHECK (result IN ('home_win','away_win','draw')),
    total_goals      SMALLINT    NOT NULL,
    goal_diff        SMALLINT    NOT NULL,
    high_scoring     BOOLEAN     NOT NULL,
    home_shot_acc    NUMERIC(6,3),
    away_shot_acc    NUMERIC(6,3),
    dominant_team    TEXT,
    match_label      TEXT,
    loaded_at        TIMESTAMPTZ DEFAULT now()
);

-- ── Indexes ──────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_matches_season    ON matches (season);
CREATE INDEX IF NOT EXISTS idx_matches_home_team ON matches (home_team);
CREATE INDEX IF NOT EXISTS idx_matches_away_team ON matches (away_team);
CREATE INDEX IF NOT EXISTS idx_matches_result    ON matches (result);
CREATE INDEX IF NOT EXISTS idx_matches_date      ON matches (date);

-- ── Standings view ───────────────────────────────────────────────────────────
-- Computes the league table entirely in SQL using a UNION + window functions.
CREATE OR REPLACE VIEW standings AS
WITH all_results AS (
    -- Home perspective
    SELECT
        home_team                              AS team,
        season,
        CASE result WHEN 'home_win' THEN 1 ELSE 0 END AS win,
        CASE result WHEN 'draw'     THEN 1 ELSE 0 END AS draw,
        CASE result WHEN 'away_win' THEN 1 ELSE 0 END AS loss,
        home_goals                             AS gf,
        away_goals                             AS ga
    FROM matches
    UNION ALL
    -- Away perspective
    SELECT
        away_team,
        season,
        CASE result WHEN 'away_win' THEN 1 ELSE 0 END,
        CASE result WHEN 'draw'     THEN 1 ELSE 0 END,
        CASE result WHEN 'home_win' THEN 1 ELSE 0 END,
        away_goals,
        home_goals
    FROM matches
)
SELECT
    team,
    season,
    COUNT(*)            AS played,
    SUM(win)            AS wins,
    SUM(draw)           AS draws,
    SUM(loss)           AS losses,
    SUM(gf)             AS goals_for,
    SUM(ga)             AS goals_against,
    SUM(gf) - SUM(ga)  AS goal_diff,
    SUM(win) * 3 + SUM(draw) AS points,
    RANK() OVER (
        PARTITION BY season
        ORDER BY SUM(win)*3 + SUM(draw) DESC,
                 SUM(gf) - SUM(ga) DESC
    )                   AS position
FROM all_results
GROUP BY team, season
ORDER BY season, position;

-- ── Referee summary view ─────────────────────────────────────────────────────
CREATE OR REPLACE VIEW referee_stats AS
SELECT
    referee,
    COUNT(*)                        AS matches,
    ROUND(AVG(total_goals), 2)      AS avg_goals,
    SUM(CASE WHEN high_scoring THEN 1 ELSE 0 END) AS high_scoring_games,
    MAX(total_goals)                AS max_goals_in_game
FROM matches
WHERE referee IS NOT NULL
GROUP BY referee
ORDER BY matches DESC;

-- ── Team head-to-head view ───────────────────────────────────────────────────
CREATE OR REPLACE VIEW head_to_head AS
SELECT
    LEAST(home_team, away_team)    AS team_a,
    GREATEST(home_team, away_team) AS team_b,
    COUNT(*)                       AS meetings,
    SUM(home_goals + away_goals)   AS total_goals,
    ROUND(AVG(home_goals + away_goals), 2) AS avg_goals
FROM matches
GROUP BY team_a, team_b
HAVING COUNT(*) > 0
ORDER BY meetings DESC, total_goals DESC;
