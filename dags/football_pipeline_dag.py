"""
football_pipeline_dag.py
~~~~~~~~~~~~~~~~~~~~~~~~
football-pipeline-v4 — Apache Spark edition.

What changed from v3
--------------------
- ingest task   : pandas → PySpark DataFrame; Parquet written via df.write
- validate task : pandas checks → Spark DataFrame API checks (count, isNull)
- load_postgres : psycopg2/PostgresHook insert → df.write.jdbc (JDBC)
- build_standings: SQL window function unchanged, but now run inside Spark
                   via spark.sql() after registering the DataFrame as a temp
                   view, then written back to Postgres over JDBC.

The four-task chain and all Airflow wiring (schedule, retries, XCom) are
identical to v3 — only the compute layer changes.

DAG: football_pipeline
Tasks: ingest >> validate >> load_postgres >> build_standings
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# spark_utils lives next to this file in dags/ — no package install needed
from spark_utils import MATCH_SCHEMA, get_spark, jdbc_params_from_env

# ── Paths (same as v1/v2/v3) ──────────────────────────────────────────────────
DATA_DIR    = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data"
RAW_DIR     = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "parquet"

# ── DDL (unchanged from v2/v3) ────────────────────────────────────────────────
CREATE_MATCHES_DDL = """
CREATE TABLE IF NOT EXISTS matches (
    match_id        TEXT PRIMARY KEY,
    date            DATE        NOT NULL,
    season          TEXT        NOT NULL,
    home_team       TEXT        NOT NULL,
    away_team       TEXT        NOT NULL,
    home_goals      INTEGER     NOT NULL,
    away_goals      INTEGER     NOT NULL,
    home_shots      INTEGER,
    away_shots      INTEGER,
    home_possession FLOAT,
    away_possession FLOAT,
    stadium         TEXT,
    referee         TEXT
);
"""

STANDINGS_SQL = """
WITH results AS (
    SELECT season,
           home_team AS team,
           home_goals AS gf,
           away_goals AS ga,
           CASE WHEN home_goals > away_goals THEN 3
                WHEN home_goals = away_goals THEN 1
                ELSE 0 END AS pts
    FROM   matches
    UNION ALL
    SELECT season,
           away_team,
           away_goals,
           home_goals,
           CASE WHEN away_goals > home_goals THEN 3
                WHEN away_goals = home_goals THEN 1
                ELSE 0 END
    FROM   matches
)
SELECT season,
       team,
       COUNT(*)        AS played,
       SUM(pts)        AS points,
       SUM(gf) - SUM(ga) AS goal_diff,
       SUM(gf)         AS goals_for,
       SUM(ga)         AS goals_against,
       SUM(CASE WHEN pts = 3 THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN pts = 1 THEN 1 ELSE 0 END) AS draws,
       SUM(CASE WHEN pts = 0 THEN 1 ELSE 0 END) AS losses
FROM   results
GROUP  BY season, team
"""

# ── Default args (unchanged from v3) ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="football_pipeline",
    description="Premier League pipeline — PySpark edition (v4)",
    schedule="@weekly",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["football", "spark", "v4"],
)
def football_pipeline():

    # ── Task 1: ingest ────────────────────────────────────────────────────────
    @task()
    def ingest() -> dict:
        """
        Read all CSVs from data/raw/ with PySpark, enforce MATCH_SCHEMA,
        cast date strings, and write a partitioned Parquet lake.

        Key Spark concepts
        ------------------
        spark.read.csv()    — lazy read; schema enforcement happens on the
                              executor, not the driver.
        df.write.parquet()  — replaces pyarrow.parquet.write_to_dataset();
                              partitioning is declared via partitionBy().
        mode="overwrite"    — idempotent; safe to re-run.
        """
        csv_files = list(RAW_DIR.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

        spark = get_spark("football-ingest")

        # Read all CSVs in one call; Spark infers partitions automatically.
        # We pass our explicit schema so bad rows surface as job failures,
        # not silent nulls — same guarantee as pyarrow schema enforcement in v1.
        df = (
            spark.read
            .option("header", "true")
            .option("mode", "FAILFAST")       # blow up on malformed rows
            .schema(MATCH_SCHEMA)
            .csv([str(f) for f in csv_files])
        )

        row_count = df.count()   # triggers the first Spark action

        # Write partitioned Parquet — identical partition layout to v1
        (
            df.write
            .mode("overwrite")
            .partitionBy("season")
            .parquet(str(PARQUET_DIR))
        )

        spark.stop()

        return {
            "files_ingested": len(csv_files),
            "rows_written": row_count,
            "parquet_dir": str(PARQUET_DIR),
        }

    # ── Task 2: validate ──────────────────────────────────────────────────────
    @task()
    def validate(ingest_result: dict) -> dict:
        """
        Read the Parquet lake with Spark and run data quality assertions.

        Key Spark concepts
        ------------------
        spark.read.parquet() — reads partitioned directories natively; Spark
                               discovers all partition files automatically.
        df.filter().count()  — lazy filter + eager count; Spark optimises the
                               predicate pushdown into the Parquet scan.
        Column.isNull()      — Spark's null check, equivalent to pandas isna().
        """
        from pyspark.sql.functions import col

        spark = get_spark("football-validate")

        df = spark.read.parquet(ingest_result["parquet_dir"])
        total_rows = df.count()

        if total_rows == 0:
            raise ValueError("Parquet lake is empty — ingest may have failed")

        # Null checks on non-nullable columns
        required_cols = ["match_id", "date", "season", "home_team", "away_team",
                         "home_goals", "away_goals"]
        for c in required_cols:
            null_count = df.filter(col(c).isNull()).count()
            if null_count > 0:
                raise ValueError(
                    f"Column '{c}' has {null_count} null value(s) — schema violated"
                )

        # Goals must be non-negative
        bad_goals = df.filter((col("home_goals") < 0) | (col("away_goals") < 0)).count()
        if bad_goals > 0:
            raise ValueError(f"{bad_goals} row(s) have negative goal values")

        spark.stop()

        return {
            **ingest_result,
            "rows_validated": total_rows,
            "validation_passed": True,
        }

    # ── Task 3: load_postgres ─────────────────────────────────────────────────
    @task()
    def load_postgres(validate_result: dict) -> dict:
        """
        Write the Parquet lake to PostgreSQL via Spark JDBC.

        Key Spark concepts
        ------------------
        df.write.jdbc()     — Spark distributes the write across executors;
                              each partition writes its slice in parallel.
        mode="append"       — preserves existing rows; combine with the
                              ON CONFLICT upsert below for idempotency.
        numPartitions       — controls write parallelism; 4 is safe for a
                              single-worker local cluster.

        Upsert strategy
        ---------------
        JDBC's append mode does not handle conflicts natively. We first write
        to a staging table, then run an INSERT ... ON CONFLICT in Postgres
        (identical to the v2/v3 upsert) via PostgresHook. This keeps the
        idempotency guarantee from v2.
        """
        spark = get_spark("football-load")
        url, props = jdbc_params_from_env()

        df = spark.read.parquet(validate_result["parquet_dir"])

        # Ensure the target table exists (PostgresHook for DDL, same as v3)
        hook = PostgresHook(postgres_conn_id="football_db")
        hook.run(CREATE_MATCHES_DDL)

        # Write to a staging table first
        (
            df.write
            .mode("overwrite")
            .option("numPartitions", "4")
            .option("truncate", "true")
            .jdbc(url=url, table="matches_staging", properties=props)
        )

        # Upsert from staging → target (unchanged from v2)
        upsert_sql = """
            INSERT INTO matches
            SELECT * FROM matches_staging
            ON CONFLICT (match_id) DO UPDATE SET
                date            = EXCLUDED.date,
                home_goals      = EXCLUDED.home_goals,
                away_goals      = EXCLUDED.away_goals,
                home_shots      = EXCLUDED.home_shots,
                away_shots      = EXCLUDED.away_shots,
                home_possession = EXCLUDED.home_possession,
                away_possession = EXCLUDED.away_possession;
        """
        hook.run(upsert_sql)

        rows_loaded = df.count()
        spark.stop()

        return {
            **validate_result,
            "rows_loaded": rows_loaded,
        }

    # ── Task 4: build_standings ───────────────────────────────────────────────
    @task()
    def build_standings(load_result: dict) -> dict:
        """
        Compute standings with Spark SQL and write results back to Postgres.

        Key Spark concepts
        ------------------
        spark.read.jdbc()   — reads a Postgres table into a DataFrame; Spark
                              pushes the query down to the database engine.
        createOrReplaceTempView() — registers a DataFrame as a virtual SQL
                              table for use in spark.sql().
        spark.sql()         — runs ANSI SQL against registered views; the same
                              UNION ALL + window logic from v2 works unchanged.
        """
        spark = get_spark("football-standings")
        url, props = jdbc_params_from_env()

        # Read from Postgres into Spark (JDBC read)
        matches_df = (
            spark.read
            .option("numPartitions", "4")
            .option("partitionColumn", "home_goals")
            .option("lowerBound", "0")
            .option("upperBound", "20")
            .jdbc(url=url, table="matches", properties=props)
        )

        # Register as a temp view so spark.sql() can reference it by name
        matches_df.createOrReplaceTempView("matches")

        # Identical SQL to v2 — window functions work in Spark SQL
        standings_df = spark.sql(STANDINGS_SQL)

        # Write standings back to Postgres
        (
            standings_df.write
            .mode("overwrite")
            .option("numPartitions", "2")
            .jdbc(url=url, table="standings", properties=props)
        )

        seasons = [r.season for r in standings_df.select("season").distinct().collect()]
        row_count = standings_df.count()

        spark.stop()

        return {
            **load_result,
            "standings_rows": row_count,
            "seasons_processed": seasons,
        }

    # ── Wire tasks ────────────────────────────────────────────────────────────
    ingest_result   = ingest()
    validate_result = validate(ingest_result)
    load_result     = load_postgres(validate_result)
    build_standings(load_result)


football_pipeline()
