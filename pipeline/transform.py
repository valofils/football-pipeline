"""
transform.py — Enrich raw match data with derived statistics.

Demonstrates: pandas transforms, list comprehensions, method chaining,
              Arrow ↔ pandas zero-copy conversion.
"""

import pyarrow as pa
import pandas as pd


def enrich(table: pa.Table) -> pd.DataFrame:
    """
    Convert an Arrow table to pandas and apply all enrichment transforms.

    Derived columns added:
      - result          : 'home_win' | 'away_win' | 'draw'
      - total_goals     : home_goals + away_goals
      - goal_diff       : home_goals - away_goals (home perspective)
      - high_scoring    : True if total_goals >= 4
      - home_shot_acc   : home_goals / home_shots (0 if no shots)
      - away_shot_acc   : away_goals / away_shots (0 if no shots)
      - dominant_team   : team with higher possession
      - match_label     : "Arsenal 2-0 Wolves" style string

    Args:
        table: Raw pa.Table from ingest.

    Returns:
        Enriched pandas DataFrame.
    """
    df = table.to_pandas()

    # --- Result classification (list comprehension over rows) ---
    df["result"] = [
        "home_win" if h > a else "away_win" if a > h else "draw"
        for h, a in zip(df["home_goals"], df["away_goals"])
    ]

    # --- Simple arithmetic columns ---
    df["total_goals"]  = df["home_goals"] + df["away_goals"]
    df["goal_diff"]    = df["home_goals"] - df["away_goals"]
    df["high_scoring"] = df["total_goals"] >= 4

    # --- Shot accuracy — guard against zero shots ---
    df["home_shot_acc"] = [
        round(g / s, 3) if s and s > 0 else 0.0
        for g, s in zip(df["home_goals"], df["home_shots"])
    ]
    df["away_shot_acc"] = [
        round(g / s, 3) if s and s > 0 else 0.0
        for g, s in zip(df["away_goals"], df["away_shots"])
    ]

    # --- Dominant possession team ---
    df["dominant_team"] = [
        home if (hp or 0) >= (ap or 0) else away
        for home, away, hp, ap in zip(
            df["home_team"], df["away_team"],
            df["home_possession"], df["away_possession"]
        )
    ]

    # --- Human-readable match label ---
    df["match_label"] = [
        f"{home} {hg}-{ag} {away}"
        for home, away, hg, ag in zip(
            df["home_team"], df["away_team"],
            df["home_goals"], df["away_goals"]
        )
    ]

    # --- Parse date as proper datetime ---
    df["date"] = pd.to_datetime(df["date"])

    print(f"[transform] Enriched {len(df)} matches — added 8 derived columns")
    return df


def summarise_by_team(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per-team season statistics from match-level data.

    Produces one row per team with: played, wins, draws, losses,
    goals_for, goals_against, goal_diff, points, avg_possession.

    Args:
        df: Enriched DataFrame from enrich().

    Returns:
        Team-level summary DataFrame sorted by points descending.
    """
    records = []

    # Collect every team that appeared as home or away
    teams = sorted(set(df["home_team"].tolist() + df["away_team"].tolist()))

    for team in teams:
        home = df[df["home_team"] == team]
        away = df[df["away_team"] == team]

        # Win/draw/loss from each perspective
        hw = (home["result"] == "home_win").sum()
        hd = (home["result"] == "draw").sum()
        hl = (home["result"] == "away_win").sum()

        aw = (away["result"] == "away_win").sum()
        ad = (away["result"] == "draw").sum()
        al = (away["result"] == "home_win").sum()

        wins   = int(hw + aw)
        draws  = int(hd + ad)
        losses = int(hl + al)
        played = wins + draws + losses

        gf = int(home["home_goals"].sum() + away["away_goals"].sum())
        ga = int(home["away_goals"].sum() + away["home_goals"].sum())

        # Avg possession across all matches
        home_poss = home["home_possession"].dropna().tolist()
        away_poss = away["away_possession"].dropna().tolist()
        all_poss  = home_poss + away_poss
        avg_poss  = round(sum(all_poss) / len(all_poss), 1) if all_poss else 0.0

        records.append({
            "team":           team,
            "played":         played,
            "wins":           wins,
            "draws":          draws,
            "losses":         losses,
            "goals_for":      gf,
            "goals_against":  ga,
            "goal_diff":      gf - ga,
            "points":         wins * 3 + draws,
            "avg_possession": avg_poss,
        })

    summary = (
        pd.DataFrame(records)
        .sort_values(["points", "goal_diff"], ascending=False)
        .reset_index(drop=True)
    )

    print(f"[transform] Built standings table for {len(summary)} teams")
    return summary
