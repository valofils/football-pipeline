"""
football_pipeline — v5 (dbt edition)
=====================================
Task chain:
    ingest >> validate >> load_postgres >> dbt_run

New in v5
---------
* build_standings Spark task removed.
* dbt_run (BashOperator) calls `dbt run && dbt test` inside the dbt/ directory.
  dbt builds stg_matches (view) and standings (incremental table) directly in
  PostgreSQL — no Spark SQL needed for the modelling layer.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import pendulum

from spark_utils import (
    get_spark,
    MATCH_SCHEMA,
    jdbc_url,
    jdbc_properties,
    jdbc_params_from_env,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data"
RAW_DIR = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "parquet"
DBT_DIR = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "dbt"

STAGING_TABLE = "matches_staging"
TARGET_TABLE = "matches"

UPSERT_SQL = f"""
INSERT INTO {TARGET_TABLE}
SELECT * FROM {STAGING_TABLE}
ON CONFLICT (match_id) DO UPDATE SET
    season     = EXCLUDED.season,
    matchday   = EXCLUDED.matchday,
    home_team  = EXCLUDED.home_team,
    away_team  = EXCLUDED.away_team,
    home_goals = EXCLUDED.home_goals,
    away_goals = EXCLUDED.away_goals;
"""

CREATE_TABLES_SQL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    match_id   TEXT PRIMARY KEY,
    season     TEXT NOT NULL,
    matchday   INTEGER NOT NULL,
    home_team  TEXT NOT NULL,
    away_team  TEXT NOT NULL,
    home_goals INTEGER NOT NULL,
    away_goals INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (LIKE {TARGET_TABLE});

CREATE TABLE IF NOT EXISTS standings (
    season   TEXT NOT NULL,
    team     TEXT NOT NULL,
    played   INTEGER,
    won      INTEGER,
    drawn    INTEGER,
    lost     INTEGER,
    gf       INTEGER,
    ga       INTEGER,
    gd       INTEGER,
    points   INTEGER,
    position INTEGER,
    PRIMARY KEY (season, team)
);
"""


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="football_pipeline",
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2},
    tags=["football", "spark", "dbt"],
)
def football_pipeline():

    # -----------------------------------------------------------------------
    # 1. ingest
    # -----------------------------------------------------------------------
    @task()
    def ingest() -> str:
        """Read CSVs with Spark, enforce schema, write partitioned Parquet lake."""
        spark: SparkSession = get_spark()

        csv_files = list(RAW_DIR.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

        df = spark.read.csv(
            [str(p) for p in csv_files],
            schema=MATCH_SCHEMA,
            header=True,
            mode="FAILFAST",
        )

        (
            df.write
            .mode("overwrite")
            .partitionBy("season")
            .parquet(str(PARQUET_DIR))
        )

        return str(PARQUET_DIR)

    # -----------------------------------------------------------------------
    # 2. validate
    # -----------------------------------------------------------------------
    @task()
    def validate(parquet_path: str) -> str:
        """Null checks and non-negative goals validation via Spark."""
        spark: SparkSession = get_spark()
        df = spark.read.parquet(parquet_path)

        if df.count() == 0:
            raise ValueError("Parquet dataset is empty — nothing to validate.")

        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                raise ValueError(f"Column '{column}' has {null_count} null value(s).")

        neg_goals = df.filter(
            (col("home_goals") < 0) | (col("away_goals") < 0)
        ).count()
        if neg_goals > 0:
            raise ValueError(f"{neg_goals} row(s) have negative goal values.")

        return parquet_path

    # -----------------------------------------------------------------------
    # 3. load_postgres
    # -----------------------------------------------------------------------
    @task()
    def load_postgres(parquet_path: str) -> None:
        """Write Parquet to staging via JDBC, then upsert into target table."""
        spark: SparkSession = get_spark()
        hook = PostgresHook(postgres_conn_id="football_db")

        params = jdbc_params_from_env()
        url = jdbc_url(**params)
        props = jdbc_properties(**params)

        # Ensure tables exist
        hook.run(CREATE_TABLES_SQL)

        # Truncate staging and refill
        hook.run(f"TRUNCATE TABLE {STAGING_TABLE};")
        df = spark.read.parquet(parquet_path)
        df.write.jdbc(url=url, table=STAGING_TABLE, mode="overwrite", properties=props)

        # Upsert staging → target
        hook.run(UPSERT_SQL)

    # -----------------------------------------------------------------------
    # 4. dbt_run  (replaces build_standings from v4)
    # -----------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --profiles-dir . && "
            "dbt test --profiles-dir ."
        ),
        params={"dbt_dir": str(DBT_DIR)},
        env={
            "FOOTBALL_DB_HOST": os.getenv("FOOTBALL_DB_HOST", "football-db"),
            "FOOTBALL_DB_PORT": os.getenv("FOOTBALL_DB_PORT", "5432"),
            "FOOTBALL_DB_NAME": os.getenv("FOOTBALL_DB_NAME", "football"),
            "FOOTBALL_DB_USER": os.getenv("FOOTBALL_DB_USER", "football"),
            "FOOTBALL_DB_PASS": os.getenv("FOOTBALL_DB_PASS", "football"),
        },
    )

    # -----------------------------------------------------------------------
    # Wire up
    # -----------------------------------------------------------------------
    parquet_path = ingest()
    validated = validate(parquet_path)
    load_postgres(validated) >> dbt_run


football_pipeline()
