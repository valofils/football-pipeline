"""
test_transform.py — Unit tests for pipeline/transform.py.

Covers: derived column correctness, edge cases, parametrize for
        result classification, standings aggregation logic.
"""

import pandas as pd
import pytest

from pipeline.transform import enrich, summarise_by_team


# ── Enrichment: column presence ───────────────────────────────────────────────

class TestEnrichColumns:
    EXPECTED_DERIVED = [
        "result", "total_goals", "goal_diff", "high_scoring",
        "home_shot_acc", "away_shot_acc", "dominant_team", "match_label",
    ]

    def test_all_derived_columns_exist(self, enriched_df):
        for col in self.EXPECTED_DERIVED:
            assert col in enriched_df.columns, f"Missing column: {col}"

    def test_row_count_unchanged(self, enriched_df, raw_table):
        assert len(enriched_df) == raw_table.num_rows

    def test_date_column_is_datetime(self, enriched_df):
        assert pd.api.types.is_datetime64_any_dtype(enriched_df["date"])


# ── Result classification ─────────────────────────────────────────────────────

@pytest.mark.parametrize("home_goals,away_goals,expected_result", [
    (2, 0, "home_win"),
    (0, 1, "away_win"),
    (1, 1, "draw"),
    (0, 0, "draw"),
    (5, 0, "home_win"),
    (0, 3, "away_win"),
])
def test_result_classification(home_goals, away_goals, expected_result, tmp_path):
    """Parametrized: result must be correct for all score combinations."""
    import pyarrow as pa
    from pipeline.ingest import SCHEMA

    row = {f.name: [None] for f in SCHEMA}
    row.update({
        "match_id":        ["px"],
        "date":            ["2024-01-01"],
        "season":          [2024],
        "home_team":       ["A"],
        "away_team":       ["B"],
        "home_goals":      [home_goals],
        "away_goals":      [away_goals],
        "home_shots":      [10],
        "away_shots":      [8],
        "home_possession": [50.0],
        "away_possession": [50.0],
    })
    table = pa.table(row, schema=SCHEMA)
    df    = enrich(table)
    assert df["result"].iloc[0] == expected_result


# ── Derived column correctness ────────────────────────────────────────────────

class TestDerivedValues:
    def test_total_goals(self, enriched_df):
        row = enriched_df[enriched_df["match_id"] == "t001"].iloc[0]
        assert row["total_goals"] == row["home_goals"] + row["away_goals"]

    def test_goal_diff(self, enriched_df):
        row = enriched_df[enriched_df["match_id"] == "t001"].iloc[0]
        assert row["goal_diff"] == row["home_goals"] - row["away_goals"]

    def test_high_scoring_true_when_4_plus_goals(self, enriched_df):
        # t006: Arsenal 4-2 Leicester = 6 goals
        row = enriched_df[enriched_df["match_id"] == "t006"].iloc[0]
        assert row["total_goals"] == 6
        assert row["high_scoring"] == True

    def test_high_scoring_false_when_under_4(self, enriched_df):
        # t001: Arsenal 2-0 Wolves = 2 goals
        row = enriched_df[enriched_df["match_id"] == "t001"].iloc[0]
        assert row["high_scoring"] == False

    def test_match_label_format(self, enriched_df):
        row = enriched_df[enriched_df["match_id"] == "t001"].iloc[0]
        assert row["match_label"] == "Arsenal 2-0 Wolves"

    def test_dominant_team_is_higher_possession(self, enriched_df):
        # t001: Arsenal home possession 62.3 > away 37.7
        row = enriched_df[enriched_df["match_id"] == "t001"].iloc[0]
        assert row["dominant_team"] == "Arsenal"

    def test_shot_accuracy_zero_when_no_shots(self, raw_table):
        """shot_acc must be 0.0 when shots is None, not a ZeroDivisionError."""
        import pyarrow as pa
        from pipeline.ingest import SCHEMA

        row = {f.name: [None] for f in SCHEMA}
        row.update({
            "match_id": ["z001"], "date": ["2024-01-01"], "season": [2024],
            "home_team": ["A"], "away_team": ["B"],
            "home_goals": [2], "away_goals": [0],
            "home_shots": [None], "away_shots": [None],
            "home_possession": [50.0], "away_possession": [50.0],
        })
        table = pa.table(row, schema=SCHEMA)
        df = enrich(table)
        assert df["home_shot_acc"].iloc[0] == 0.0

    @pytest.mark.parametrize("match_id,expected_acc", [
        ("t001", round(2 / 14, 3)),   # Arsenal 2 goals, 14 shots
        ("t003", round(3 / 18, 3)),   # Liverpool 3 goals, 18 shots
    ])
    def test_shot_accuracy_value(self, enriched_df, match_id, expected_acc):
        row = enriched_df[enriched_df["match_id"] == match_id].iloc[0]
        assert row["home_shot_acc"] == pytest.approx(expected_acc, abs=1e-3)


# ── Team standings summary ────────────────────────────────────────────────────

class TestSummariseByTeam:
    def test_returns_dataframe(self, enriched_df):
        summary = summarise_by_team(enriched_df)
        assert isinstance(summary, pd.DataFrame)

    def test_correct_columns(self, enriched_df):
        summary = summarise_by_team(enriched_df)
        for col in ["team", "played", "wins", "draws", "losses",
                    "goals_for", "goals_against", "goal_diff", "points"]:
            assert col in summary.columns

    def test_arsenal_record(self, enriched_df):
        """Arsenal: t001 W, t004 W, t006 W → 3 wins, 9 points."""
        summary = summarise_by_team(enriched_df)
        arsenal = summary[summary["team"] == "Arsenal"].iloc[0]
        assert arsenal["wins"]   == 3
        assert arsenal["draws"]  == 0
        assert arsenal["losses"] == 0
        assert arsenal["points"] == 9

    def test_points_formula(self, enriched_df):
        """Points must always equal wins*3 + draws."""
        summary = summarise_by_team(enriched_df)
        for _, row in summary.iterrows():
            assert row["points"] == row["wins"] * 3 + row["draws"]

    def test_sorted_by_points_descending(self, enriched_df):
        summary = summarise_by_team(enriched_df)
        pts = summary["points"].tolist()
        assert pts == sorted(pts, reverse=True)

    def test_played_equals_wins_plus_draws_plus_losses(self, enriched_df):
        summary = summarise_by_team(enriched_df)
        for _, row in summary.iterrows():
            assert row["played"] == row["wins"] + row["draws"] + row["losses"]
