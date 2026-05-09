"""
query.py — Query the Parquet data lake for match and team statistics.

Demonstrates: pyarrow.parquet projection/predicate pushdown,
              pandas aggregations, list comprehensions for formatting.
"""

from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd


def _read_lake(lake_dir: Path, columns: list[str] | None = None,
               filters: list | None = None) -> pd.DataFrame:
    """Read the partitioned lake with optional column projection and row filters."""
    dataset = pq.read_table(
        str(lake_dir),
        columns=columns,
        filters=filters,
    )
    return dataset.to_pandas()


def top_scorers(lake_dir: str | Path, n: int = 5) -> pd.DataFrame:
    """
    Return the top N highest-scoring matches.

    Args:
        lake_dir: Lake root directory.
        n:        Number of results to return.

    Returns:
        DataFrame with match_label and total_goals.
    """
    df = _read_lake(
        Path(lake_dir),
        columns=["match_label", "total_goals", "date"],
    )
    return (
        df.sort_values("total_goals", ascending=False)
        .head(n)
        .reset_index(drop=True)
    )


def team_stats(lake_dir: str | Path, team: str) -> dict:
    """
    Return a stat summary dict for a specific team.

    Args:
        lake_dir: Lake root directory.
        team:     Exact team name (e.g. "Arsenal").

    Returns:
        Dict with played, wins, draws, losses, goals_for, goals_against, points.
    """
    df = _read_lake(Path(lake_dir))

    home = df[df["home_team"] == team]
    away = df[df["away_team"] == team]

    if home.empty and away.empty:
        raise ValueError(f"Team '{team}' not found in dataset")

    wins   = int((home["result"] == "home_win").sum() + (away["result"] == "away_win").sum())
    draws  = int((home["result"] == "draw").sum()     + (away["result"] == "draw").sum())
    losses = int((home["result"] == "away_win").sum() + (away["result"] == "home_win").sum())

    gf = int(home["home_goals"].sum() + away["away_goals"].sum())
    ga = int(home["away_goals"].sum() + away["home_goals"].sum())

    return {
        "team":          team,
        "played":        wins + draws + losses,
        "wins":          wins,
        "draws":         draws,
        "losses":        losses,
        "goals_for":     gf,
        "goals_against": ga,
        "goal_diff":     gf - ga,
        "points":        wins * 3 + draws,
    }


def standings(lake_dir: str | Path) -> pd.DataFrame:
    """
    Compute and return a full league standings table from the lake.

    Args:
        lake_dir: Lake root directory.

    Returns:
        DataFrame sorted by points, then goal difference.
    """
    df = _read_lake(Path(lake_dir))

    teams = sorted({t for t in df["home_team"].tolist() + df["away_team"].tolist() if isinstance(t, str)})
    rows  = [team_stats(lake_dir, t) for t in teams]

    return (
        pd.DataFrame(rows)
        .sort_values(["points", "goal_diff"], ascending=False)
        .reset_index(drop=True)
    )


def high_scoring_matches(lake_dir: str | Path, min_goals: int = 4) -> pd.DataFrame:
    """
    Return all matches with total goals >= min_goals.

    Uses PyArrow predicate pushdown — only qualifying row groups are read.

    Args:
        lake_dir:  Lake root directory.
        min_goals: Minimum total goals threshold.

    Returns:
        DataFrame of high-scoring matches sorted by total_goals descending.
    """
    df = _read_lake(
        Path(lake_dir),
        columns=["match_label", "total_goals", "date", "stadium"],
        filters=[("total_goals", ">=", min_goals)],
    )
    return df.sort_values("total_goals", ascending=False).reset_index(drop=True)


def referee_summary(lake_dir: str | Path) -> pd.DataFrame:
    """
    Aggregate matches and average goals per game grouped by referee.

    Args:
        lake_dir: Lake root directory.

    Returns:
        DataFrame sorted by matches officiated descending.
    """
    df = _read_lake(Path(lake_dir), columns=["referee", "total_goals"])
    return (
        df.groupby("referee")["total_goals"]
        .agg(matches="count", avg_goals="mean")
        .round(2)
        .sort_values("matches", ascending=False)
        .reset_index()
    )
