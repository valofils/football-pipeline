"""
cli.py — Command-line interface for the football analytics pipeline.

Usage:
    python cli.py ingest  --input data/raw/matches.csv
    python cli.py query   --stat standings
    python cli.py query   --stat team    --team Arsenal
    python cli.py query   --stat top     --n 5
    python cli.py query   --stat high    --min-goals 4
    python cli.py query   --stat referee
"""

import argparse
import sys
from pathlib import Path

from pipeline.ingest    import load_csv
from pipeline.transform import enrich, summarise_by_team
from pipeline.store     import write_matches, write_standings
from pipeline import query as Q

LAKE_DIR       = Path("data/lake")
STANDINGS_PATH = Path("data/lake/standings.parquet")


def cmd_ingest(args: argparse.Namespace) -> None:
    """Run the full ingest → transform → store pipeline."""
    table = load_csv(args.input)
    df    = enrich(table)
    write_matches(df, LAKE_DIR)

    standings_df = summarise_by_team(df)
    write_standings(standings_df, STANDINGS_PATH)

    print("\nPipeline complete. Run 'python cli.py query --stat standings' to explore.")


def cmd_query(args: argparse.Namespace) -> None:
    """Query the lake and print results to stdout."""
    if not LAKE_DIR.exists():
        sys.exit("Lake not found. Run: python cli.py ingest --input data/raw/matches.csv")

    stat = args.stat

    if stat == "standings":
        df = Q.standings(LAKE_DIR)
        print(f"\n{'Pos':<4} {'Team':<25} {'P':>3} {'W':>3} {'D':>3} {'L':>3} "
              f"{'GF':>4} {'GA':>4} {'GD':>4} {'Pts':>4}")
        print("-" * 65)
        for i, row in df.iterrows():
            print(f"{i+1:<4} {row['team']:<25} {row['played']:>3} {row['wins']:>3} "
                  f"{row['draws']:>3} {row['losses']:>3} {row['goals_for']:>4} "
                  f"{row['goals_against']:>4} {row['goal_diff']:>4} {row['points']:>4}")

    elif stat == "team":
        if not args.team:
            sys.exit("--team is required for --stat team")
        stats = Q.team_stats(LAKE_DIR, args.team)
        print(f"\n--- {stats['team']} ---")
        for k, v in stats.items():
            if k != "team":
                print(f"  {k:<16} {v}")

    elif stat == "top":
        n  = args.n or 5
        df = Q.top_scorers(LAKE_DIR, n=n)
        print(f"\nTop {n} highest-scoring matches:")
        for _, row in df.iterrows():
            print(f"  {row['match_label']:<40} {row['total_goals']} goals")

    elif stat == "high":
        min_g = args.min_goals or 4
        df    = Q.high_scoring_matches(LAKE_DIR, min_goals=min_g)
        print(f"\nMatches with {min_g}+ goals ({len(df)} found):")
        for _, row in df.iterrows():
            print(f"  {row['match_label']:<40} {row['total_goals']} goals")

    elif stat == "referee":
        df = Q.referee_summary(LAKE_DIR)
        print(f"\n{'Referee':<20} {'Matches':>8} {'Avg goals':>10}")
        print("-" * 42)
        for _, row in df.iterrows():
            print(f"  {row['referee']:<20} {row['matches']:>6} {row['avg_goals']:>10.2f}")

    else:
        sys.exit(f"Unknown stat '{stat}'. Choose: standings, team, top, high, referee")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="football-pipeline",
        description="Sports analytics pipeline — ingest CSV, store as Parquet, query stats.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- ingest subcommand ---
    p_ingest = sub.add_parser("ingest", help="Run the full ETL pipeline")
    p_ingest.add_argument(
        "--input", type=Path, default=Path("data/raw/matches.csv"),
        help="Path to raw CSV file (default: data/raw/matches.csv)",
    )

    # --- query subcommand ---
    p_query = sub.add_parser("query", help="Query the processed data lake")
    p_query.add_argument(
        "--stat", required=True,
        choices=["standings", "team", "top", "high", "referee"],
        help="Which statistic to display",
    )
    p_query.add_argument("--team",      type=str, help="Team name (for --stat team)")
    p_query.add_argument("--n",         type=int, help="Number of results (for --stat top)")
    p_query.add_argument("--min-goals", type=int, dest="min_goals",
                         help="Goal threshold (for --stat high)")

    return parser


if __name__ == "__main__":
    parser = build_parser()
    args   = parser.parse_args()

    if args.command == "ingest":
        cmd_ingest(args)
    elif args.command == "query":
        cmd_query(args)
