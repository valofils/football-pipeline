"""
test_db.py — Integration tests for pipeline/db.py.

These tests hit a real PostgreSQL database (football_db).
They use the empty_db and loaded_db fixtures from conftest.py
which guarantee a clean table state before each test.

Covers: schema creation, bulk insert, upsert behaviour,
        SQL views (standings, referee_stats), query helpers.
"""

import pytest
import psycopg2

from pipeline.db import (
    apply_schema, load_matches, fetch_standings,
    fetch_team_stats, fetch_high_scoring, get_conn,
)


# ── Schema ────────────────────────────────────────────────────────────────────

class TestSchema:
    def test_matches_table_exists(self, db_schema, db_params):
        with get_conn(db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'matches'
                    )
                """)
                assert cur.fetchone()[0] is True

    @pytest.mark.parametrize("view_name", ["standings", "referee_stats", "head_to_head"])
    def test_views_exist(self, db_schema, db_params, view_name):
        with get_conn(db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.views
                        WHERE table_name = %s
                    )
                """, (view_name,))
                assert cur.fetchone()[0] is True, f"View '{view_name}' not found"

    @pytest.mark.parametrize("index_name", [
        "idx_matches_season", "idx_matches_home_team",
        "idx_matches_away_team", "idx_matches_result",
    ])
    def test_indexes_exist(self, db_schema, db_params, index_name):
        with get_conn(db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_indexes
                        WHERE indexname = %s
                    )
                """, (index_name,))
                assert cur.fetchone()[0] is True, f"Index '{index_name}' not found"


# ── Load / upsert ─────────────────────────────────────────────────────────────

class TestLoadMatches:
    def test_inserts_correct_row_count(self, empty_db, enriched_df, db_params):
        load_matches(enriched_df, db_params)
        with get_conn(db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM matches")
                assert cur.fetchone()[0] == len(enriched_df)

    def test_upsert_does_not_duplicate(self, empty_db, enriched_df, db_params):
        """Loading the same data twice must not create duplicate rows."""
        load_matches(enriched_df, db_params)
        load_matches(enriched_df, db_params)
        with get_conn(db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM matches")
                assert cur.fetchone()[0] == len(enriched_df)

    def test_data_integrity(self, loaded_db, db_params):
        """Spot-check values stored in the DB match the source DataFrame."""
        with get_conn(db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT home_goals, away_goals, result FROM matches WHERE match_id = %s",
                    ("t001",)
                )
                row = cur.fetchone()
        assert row is not None
        assert row[0] == 2          # home_goals
        assert row[1] == 0          # away_goals
        assert row[2] == "home_win"

    def test_check_constraint_prevents_negative_goals(self, empty_db, enriched_df, db_params):
        """The CHECK constraint on home_goals >= 0 must reject invalid data."""
        with get_conn(db_params) as conn:
            with conn.cursor() as cur:
                with pytest.raises(psycopg2.errors.CheckViolation):
                    cur.execute(
                        "INSERT INTO matches (match_id, date, season, home_team, away_team, "
                        "home_goals, away_goals, result, total_goals, goal_diff, high_scoring) "
                        "VALUES ('bad', '2024-01-01', 2024, 'A', 'B', -1, 0, 'home_win', -1, -1, false)"
                    )


# ── Standings view ────────────────────────────────────────────────────────────

class TestStandingsView:
    def test_returns_all_teams(self, loaded_db, db_params):
        rows = fetch_standings(2024, db_params)
        teams = {r["team"] for r in rows}
        # Sample data has: Arsenal, Chelsea, Liverpool, Man City,
        # Wolves, Brentford, Tottenham, Leicester
        assert "Arsenal" in teams
        assert "Man City" in teams

    def test_arsenal_leads_in_sample(self, loaded_db, db_params):
        """Arsenal wins all 3 in sample → 9 pts, should be position 1."""
        rows = fetch_standings(2024, db_params)
        arsenal = next(r for r in rows if r["team"] == "Arsenal")
        assert arsenal["wins"]   == 3
        assert arsenal["points"] == 9
        assert arsenal["position"] == 1

    def test_points_formula_in_view(self, loaded_db, db_params):
        """SQL view must compute points as wins*3 + draws for every team."""
        rows = fetch_standings(2024, db_params)
        for r in rows:
            expected = r["wins"] * 3 + r["draws"]
            assert int(r["points"]) == expected, f"Points wrong for {r['team']}"

    def test_position_is_dense_rank(self, loaded_db, db_params):
        """Position 1 must exist exactly once (no ties at top in sample data)."""
        rows = fetch_standings(2024, db_params)
        pos1 = [r for r in rows if r["position"] == 1]
        assert len(pos1) == 1


# ── Query helpers ─────────────────────────────────────────────────────────────

class TestQueryHelpers:
    def test_fetch_team_stats_returns_data(self, loaded_db, db_params):
        result = fetch_team_stats("Arsenal", db_params)
        assert result is not None
        assert len(result) > 0

    def test_fetch_team_stats_unknown_team_returns_none(self, loaded_db, db_params):
        result = fetch_team_stats("Fake FC", db_params)
        assert result is None

    @pytest.mark.parametrize("min_goals,expected_min_count", [
        (4, 1),   # Arsenal 4-2 Leicester = 6 goals definitely qualifies
        (7, 0),   # no match in sample has 7+ goals
    ])
    def test_fetch_high_scoring(self, loaded_db, db_params, min_goals, expected_min_count):
        results = fetch_high_scoring(min_goals, db_params)
        assert len(results) >= expected_min_count
        for r in results:
            assert r["total_goals"] >= min_goals

    def test_fetch_high_scoring_sorted_desc(self, loaded_db, db_params):
        results = fetch_high_scoring(1, db_params)
        goals = [r["total_goals"] for r in results]
        assert goals == sorted(goals, reverse=True)
