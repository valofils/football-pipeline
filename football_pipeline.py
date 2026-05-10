"""
football_pipeline.py — Airflow DAG (v7)

Changes vs v6:
- kafka_ingest writes Parquet to S3 (s3a://) instead of local filesystem
- validate reads from S3
- load_postgres reads Parquet from S3 via Spark then upserts into RDS
- dbt_run unchanged (still BashOperator; dbt profiles.yml points to RDS host)

DAG chain: kafka_ingest >> validate >> load_postgres >> dbt_run
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from kafka.consumer.consume_matches import consume
from spark_utils import MATCH_SCHEMA, get_spark, jdbc_params_from_env, s3a_path

# ── DAG defaults ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

S3_RAW_PREFIX   = "raw/matches"
S3_PARQUET_PREFIX = "parquet/matches"


@dag(
    dag_id="football_pipeline",
    schedule="@weekly",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["football", "v7", "aws"],
)
def football_pipeline():

    # ── Task 1: Consume Kafka → write Parquet to S3 ───────────────────────────

    @task
    def kafka_ingest() -> dict:
        """
        Consume match-events topic, write partitioned Parquet to S3.

        Returns a dict with the s3a path and row count for downstream tasks.
        """
        from pyspark.sql import Row

        records = consume()  # list[dict] from Kafka consumer
        if not records:
            raise ValueError("kafka_ingest: no messages consumed — topic empty?")

        spark = get_spark("FootballPipeline-Ingest")

        rows = [Row(**r) for r in records]
        df = spark.createDataFrame(rows, schema=MATCH_SCHEMA)

        out_path = s3a_path(S3_PARQUET_PREFIX)
        df.write.mode("overwrite").partitionBy("season").parquet(out_path)

        row_count = df.count()
        spark.stop()

        return {"s3_path": out_path, "row_count": row_count}

    # ── Task 2: Validate ──────────────────────────────────────────────────────

    @task
    def validate(ingest_result: dict) -> dict:
        """
        Read Parquet from S3, assert no nulls on critical columns.
        Passes s3_path downstream unchanged.
        """
        from pyspark.sql import functions as F

        spark = get_spark("FootballPipeline-Validate")
        df = spark.read.parquet(ingest_result["s3_path"])

        critical_cols = ["match_id", "season", "home_team", "away_team"]
        for col_name in critical_cols:
            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count > 0:
                raise ValueError(f"validate: {null_count} null(s) in column '{col_name}'")

        spark.stop()
        return ingest_result

    # ── Task 3: Load Postgres (RDS) ───────────────────────────────────────────

    @task
    def load_postgres(ingest_result: dict) -> None:
        """
        Read Parquet from S3, upsert into matches_staging on RDS via JDBC.
        Then promote from staging to matches via ON CONFLICT.
        """
        spark = get_spark("FootballPipeline-Load")
        url, props = jdbc_params_from_env()

        df = spark.read.parquet(ingest_result["s3_path"])
        df.write.jdbc(url=url, table="matches_staging", mode="overwrite", properties=props)
        spark.stop()

        hook = PostgresHook(postgres_conn_id="football_db")
        hook.run(
            """
            INSERT INTO matches (match_id, season, date, home_team, away_team, home_goals, away_goals, result)
            SELECT match_id, season, date::date, home_team, away_team, home_goals, away_goals, result
            FROM   matches_staging
            ON CONFLICT (match_id) DO UPDATE SET
                home_goals = EXCLUDED.home_goals,
                away_goals = EXCLUDED.away_goals,
                result     = EXCLUDED.result;
            """
        )

    # ── Task 4: dbt ───────────────────────────────────────────────────────────

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt && "
            "dbt test --profiles-dir /opt/airflow/dbt"
        ),
    )

    # ── Wire up ───────────────────────────────────────────────────────────────

    ingest_result = kafka_ingest()
    validated     = validate(ingest_result)
    loaded        = load_postgres(validated)
    loaded >> dbt_run


football_pipeline()
