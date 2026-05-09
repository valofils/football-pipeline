"""
db.py — PostgreSQL connection, schema management, and data loading.

Demonstrates: psycopg2 connection handling, context managers,
              executemany bulk insert, DDL from SQL files.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.extras
import pandas as pd


# ── Connection config ─────────────────────────────────────────────────────────

def get_connection_params(override: dict | None = None) -> dict:
    """
    Build connection params from environment variables with sensible defaults.
    Override dict is used by tests to inject a test database.
    """
    params = {
        "dbname":   os.getenv("FOOTBALL_DB",       "football_db"),
        "user":     os.getenv("FOOTBALL_DB_USER",   "football"),
        "password": os.getenv("FOOTBALL_DB_PASS",   "football123"),
        "host":     os.getenv("FOOTBALL_DB_HOST",   "localhost"),
        "port":     int(os.getenv("FOOTBALL_DB_PORT", "5432")),
    }
    if override:
        params.update(override)
    return params


@contextmanager
def get_conn(params: dict | None = None):
    """
    Context manager that yields a psycopg2 connection and auto-commits
    or rolls back on error.

    Usage:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    conn = psycopg2.connect(**(params or get_connection_params()))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Schema management ─────────────────────────────────────────────────────────

def apply_schema(params: dict | None = None) -> None:
    """
    Execute schema.sql against the target database.
    Idempotent — uses CREATE IF NOT EXISTS / CREATE OR REPLACE.
    """
    sql_path = Path(__file__).parent / "sql" / "schema.sql"
    ddl = sql_path.read_text(encoding="utf-8")

    with get_conn(params) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)

    print("[db] Schema applied (tables + views)")


def drop_all(params: dict | None = None) -> None:
    """
    Drop all project tables and views. Used by tests for teardown.
    """
    sql = """
        DROP VIEW  IF EXISTS head_to_head   CASCADE;
        DROP VIEW  IF EXISTS referee_stats  CASCADE;
        DROP VIEW  IF EXISTS standings      CASCADE;
        DROP TABLE IF EXISTS matches        CASCADE;
    """
    with get_conn(params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    print("[db] All tables and views dropped")


# ── Data loading ──────────────────────────────────────────────────────────────

_INSERT_SQL = """
    INSERT INTO matches (
        match_id, date, season, home_team, away_team,
        home_goals, away_goals, home_shots, away_shots,
        home_possession, away_possession, stadium, referee,
        result, total_goals, goal_diff, high_scoring,
        home_shot_acc, away_shot_acc, dominant_team, match_label
    ) VALUES (
        %(match_id)s, %(date)s, %(season)s, %(home_team)s, %(away_team)s,
        %(home_goals)s, %(away_goals)s, %(home_shots)s, %(away_shots)s,
        %(home_possession)s, %(away_possession)s, %(stadium)s, %(referee)s,
        %(result)s, %(total_goals)s, %(goal_diff)s, %(high_scoring)s,
        %(home_shot_acc)s, %(away_shot_acc)s, %(dominant_team)s, %(match_label)s
    )
    ON CONFLICT (match_id) DO UPDATE SET
        home_goals      = EXCLUDED.home_goals,
        away_goals      = EXCLUDED.away_goals,
        result          = EXCLUDED.result,
        total_goals     = EXCLUDED.total_goals,
        loaded_at       = now();
"""


def load_matches(df: pd.DataFrame, params: dict | None = None) -> int:
    """
    Bulk-insert an enriched matches DataFrame into PostgreSQL.
    Uses executemany with named-parameter dicts for clarity and safety.
    Upserts on match_id conflict.

    Args:
        df:     Enriched DataFrame from transform.enrich().
        params: Optional connection override (used by tests).

    Returns:
        Number of rows inserted/updated.
    """
    # Convert pandas types that psycopg2 can't handle directly
    records = []
    for row in df.itertuples(index=False):
        records.append({
            "match_id":        row.match_id,
            "date":            row.date.date() if hasattr(row.date, "date") else row.date,
            "season":          int(row.season),
            "home_team":       row.home_team,
            "away_team":       row.away_team,
            "home_goals":      int(row.home_goals),
            "away_goals":      int(row.away_goals),
            "home_shots":      int(row.home_shots)      if row.home_shots      is not None else None,
            "away_shots":      int(row.away_shots)      if row.away_shots      is not None else None,
            "home_possession": float(row.home_possession) if row.home_possession is not None else None,
            "away_possession": float(row.away_possession) if row.away_possession is not None else None,
            "stadium":         row.stadium,
            "referee":         row.referee,
            "result":          row.result,
            "total_goals":     int(row.total_goals),
            "goal_diff":       int(row.goal_diff),
            "high_scoring":    bool(row.high_scoring),
            "home_shot_acc":   float(row.home_shot_acc),
            "away_shot_acc":   float(row.away_shot_acc),
            "dominant_team":   row.dominant_team,
            "match_label":     row.match_label,
        })

    with get_conn(params) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, _INSERT_SQL, records, page_size=500)
            count = cur.rowcount

    print(f"[db] Loaded {len(records)} rows into matches table")
    return len(records)


# ── Query helpers ─────────────────────────────────────────────────────────────

def fetch_standings(season: int, params: dict | None = None) -> list[dict]:
    """Return league standings for a given season from the SQL view."""
    sql = "SELECT * FROM standings WHERE season = %s ORDER BY position"
    with get_conn(params) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (season,))
            return [dict(r) for r in cur.fetchall()]


def fetch_team_stats(team: str, params: dict | None = None) -> dict | None:
    """Return aggregated stats for one team across all seasons."""
    sql = "SELECT * FROM standings WHERE team = %s ORDER BY season"
    with get_conn(params) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (team,))
            rows = cur.fetchall()
            return [dict(r) for r in rows] if rows else None


def fetch_high_scoring(min_goals: int = 4, params: dict | None = None) -> list[dict]:
    """Return matches with total_goals >= min_goals, ordered by goals desc."""
    sql = """
        SELECT match_label, total_goals, date, stadium
        FROM   matches
        WHERE  total_goals >= %s
        ORDER  BY total_goals DESC, date
    """
    with get_conn(params) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (min_goals,))
            return [dict(r) for r in cur.fetchall()]
