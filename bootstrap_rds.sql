-- bootstrap_rds.sql
-- Run against RDS after `terraform apply`:
--   psql -h <RDS_HOST> -U football_user -d football -f scripts/bootstrap_rds.sql

CREATE TABLE IF NOT EXISTS matches (
    match_id   INTEGER PRIMARY KEY,
    season     TEXT        NOT NULL,
    date       DATE,
    home_team  TEXT,
    away_team  TEXT,
    home_goals INTEGER,
    away_goals INTEGER,
    result     TEXT,
    loaded_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS matches_staging (
    match_id   INTEGER PRIMARY KEY,
    season     TEXT,
    date       TEXT,
    home_team  TEXT,
    away_team  TEXT,
    home_goals INTEGER,
    away_goals INTEGER,
    result     TEXT
);

CREATE INDEX IF NOT EXISTS idx_matches_season ON matches (season);
CREATE INDEX IF NOT EXISTS idx_matches_home   ON matches (home_team);
CREATE INDEX IF NOT EXISTS idx_matches_away   ON matches (away_team);
