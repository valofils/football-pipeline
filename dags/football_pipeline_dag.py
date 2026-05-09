from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")
PARQUET_DIR = DATA_DIR / "parquet"
POSTGRES_CONN_ID = "football_postgres"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="football_pipeline",
    description="Ingest Premier League CSVs → Parquet lake → PostgreSQL → standings",
    schedule="@weekly",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["football", "etl"],
)
def football_pipeline():

    @task()
    def ingest() -> dict:
        """Read CSVs from data/raw/, enforce schema, write partitioned Parquet."""
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        raw_dir = DATA_DIR / "raw"
        csv_files = list(raw_dir.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {raw_dir}")

        schema = pa.schema([
            pa.field("match_id",   pa.int64()),
            pa.field("season",     pa.string()),
            pa.field("home_team",  pa.string()),
            pa.field("away_team",  pa.string()),
            pa.field("home_goals", pa.int32()),
            pa.field("away_goals", pa.int32()),
            pa.field("match_date", pa.date32()),
        ])

        frames = []
        for f in csv_files:
            df = pd.read_csv(f, parse_dates=["match_date"])
            frames.append(df)

        combined = pd.concat(frames, ignore_index=True)

        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        for season, group in combined.groupby("season"):
            season_table = pa.Table.from_pandas(group, schema=schema)
            out = PARQUET_DIR / f"season={season}" / "matches.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(season_table, out, compression="snappy")
            log.info("Written %s rows → %s", len(group), out)

        row_count = len(combined)
        log.info("Ingest complete: %d total rows across %d files", row_count, len(csv_files))
        return {"row_count": row_count, "seasons": combined["season"].nunique()}

    @task()
    def validate(ingest_result: dict) -> dict:
        """Assert no nulls in key columns and row count is non-zero."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        row_count = ingest_result["row_count"]
        if row_count == 0:
            raise ValueError("Validation failed: 0 rows ingested")

        tables = [pq.read_table(p) for p in PARQUET_DIR.rglob("*.parquet")]
        df = pa.concat_tables(tables).to_pandas()

        for col in ["match_id", "home_team", "away_team", "home_goals", "away_goals"]:
            nulls = df[col].isna().sum()
            if nulls > 0:
                raise ValueError(f"Validation failed: {nulls} nulls in column '{col}'")

        log.info("Validation passed: %d rows, %d seasons", row_count, ingest_result["seasons"])
        return ingest_result

    @task()
    def load_postgres(validated: dict) -> dict:
        """Upsert match rows into PostgreSQL using PostgresHook."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        hook.run("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id   BIGINT PRIMARY KEY,
                season     TEXT NOT NULL,
                home_team  TEXT NOT NULL,
                away_team  TEXT NOT NULL,
                home_goals INT  NOT NULL,
                away_goals INT  NOT NULL,
                match_date DATE NOT NULL
            );
        """)

        tables = [pq.read_table(p) for p in PARQUET_DIR.rglob("*.parquet")]
        df = pa.concat_tables(tables).to_pandas()
        rows = list(df.itertuples(index=False, name=None))

        hook.insert_rows(
            table="matches",
            rows=rows,
            target_fields=["match_id", "season", "home_team", "away_team",
                           "home_goals", "away_goals", "match_date"],
            replace=True,
            replace_index=["match_id"],
        )

        log.info("Upserted %d rows into matches", len(rows))
        return {**validated, "loaded": len(rows)}

    @task()
    def build_standings(load_result: dict) -> None:
        """Rebuild the standings view from the matches table."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        hook.run("""
            CREATE OR REPLACE VIEW standings AS
            WITH all_matches AS (
                SELECT season, home_team AS team,
                       home_goals AS gf, away_goals AS ga,
                       CASE WHEN home_goals > away_goals THEN 3
                            WHEN home_goals = away_goals THEN 1
                            ELSE 0 END AS pts
                FROM matches
                UNION ALL
                SELECT season, away_team,
                       away_goals, home_goals,
                       CASE WHEN away_goals > home_goals THEN 3
                            WHEN away_goals = home_goals THEN 1
                            ELSE 0 END
                FROM matches
            )
            SELECT season, team,
                   COUNT(*)           AS played,
                   SUM(pts)           AS points,
                   SUM(gf) - SUM(ga)  AS goal_diff
            FROM all_matches
            GROUP BY season, team
            ORDER BY season, points DESC, goal_diff DESC;
        """)

        log.info("Standings view rebuilt. Rows loaded this run: %d", load_result["loaded"])

    ingest_result = ingest()
    validated     = validate(ingest_result)
    load_result   = load_postgres(validated)
    build_standings(load_result)


football_pipeline()
