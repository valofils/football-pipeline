"""
football_pipeline.py
--------------------
Airflow DAG for football-pipeline-v6.

Pipeline chain (same four tasks as v5, but `ingest` is now Kafka-based):

    kafka_ingest >> validate >> load_postgres >> dbt_run

kafka_ingest:
    Calls the Kafka consumer as a Python callable.
    Reads from topic `match-events`, upserts rows into `matches_staging`.

validate:
    Spark-based null checks on `matches_staging` via JDBC.

load_postgres:
    Spark reads `matches_staging`, writes to the canonical `matches` table
    via upsert (INSERT … ON CONFLICT).

dbt_run:
    BashOperator — runs `dbt run && dbt test` to build staging view
    + incremental standings table and assert data quality.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Allow imports from project root inside the container
sys.path.insert(0, "/opt/airflow")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt"
KAFKA_TIMEOUT = int(os.getenv("KAFKA_CONSUMER_TIMEOUT", "120"))
KAFKA_BATCH_SIZE = int(os.getenv("KAFKA_CONSUMER_BATCH_SIZE", "1000"))


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="football_pipeline",
    description="PL match pipeline v6: Kafka → Spark → dbt",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["football", "kafka", "spark", "dbt"],
)
def football_pipeline():

    # ------------------------------------------------------------------
    # Task 1 — Kafka ingest
    # ------------------------------------------------------------------
    @task(task_id="kafka_ingest")
    def kafka_ingest() -> dict:
        """
        Consume all pending messages from `match-events` topic and upsert
        into `matches_staging`.  Returns a summary dict passed via XCom.
        """
        # Import here so Airflow's module scanner doesn't need confluent_kafka
        # at parse time on workers that haven't installed it yet.
        from kafka.consumer.consume_matches import consume

        rows_written = consume(
            timeout_seconds=KAFKA_TIMEOUT,
            batch_size=KAFKA_BATCH_SIZE,
        )

        if rows_written == 0:
            raise ValueError(
                "Kafka consumer wrote 0 rows — topic may be empty or consumer "
                "group has already processed all messages."
            )

        return {"rows_ingested": rows_written}

    # ------------------------------------------------------------------
    # Task 2 — Validate (Spark null checks on matches_staging)
    # ------------------------------------------------------------------
    @task(task_id="validate")
    def validate(ingest_result: dict) -> dict:
        """
        Use Spark to run null-checks on the staging table.
        Raises ValueError if any required column contains nulls.
        """
        from dags.spark_utils import get_spark, jdbc_params_from_env, jdbc_url

        spark = get_spark("validate")
        params = jdbc_params_from_env()
        url = jdbc_url(**params)
        props = {"user": params["user"], "password": params["password"], "driver": "org.postgresql.Driver"}

        df = spark.read.jdbc(url=url, table="matches_staging", properties=props)

        required_cols = ["match_id", "season", "home_team", "away_team"]
        errors = []
        for col_name in required_cols:
            from pyspark.sql.functions import col
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count:
                errors.append(f"{col_name}: {null_count} null(s)")

        spark.stop()

        if errors:
            raise ValueError(f"Validation failed — null values found: {errors}")

        row_count = df.count()
        return {"rows_validated": row_count, **ingest_result}

    # ------------------------------------------------------------------
    # Task 3 — Load canonical matches table (Spark JDBC upsert)
    # ------------------------------------------------------------------
    @task(task_id="load_postgres")
    def load_postgres(validate_result: dict) -> dict:
        """
        Spark reads matches_staging and upserts into the canonical `matches`
        table via a temp staging write + INSERT … ON CONFLICT.
        """
        from dags.spark_utils import get_spark, jdbc_params_from_env, jdbc_url

        spark = get_spark("load_postgres")
        params = jdbc_params_from_env()
        url = jdbc_url(**params)
        props = {"user": params["user"], "password": params["password"], "driver": "org.postgresql.Driver"}

        df = spark.read.jdbc(url=url, table="matches_staging", properties=props)

        # Write to a temp table, then upsert into canonical table via SQL
        df.write.jdbc(url=url, table="matches_load_tmp", mode="overwrite", properties=props)

        hook = PostgresHook(postgres_conn_id="football_db")
        hook.run("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id    INTEGER PRIMARY KEY,
                season      TEXT    NOT NULL,
                home_team   TEXT    NOT NULL,
                away_team   TEXT    NOT NULL,
                home_goals  INTEGER,
                away_goals  INTEGER,
                match_date  DATE,
                referee     TEXT
            );

            INSERT INTO matches
                SELECT match_id, season, home_team, away_team,
                       home_goals, away_goals, match_date, referee
                FROM matches_load_tmp
            ON CONFLICT (match_id) DO UPDATE SET
                season     = EXCLUDED.season,
                home_team  = EXCLUDED.home_team,
                away_team  = EXCLUDED.away_team,
                home_goals = EXCLUDED.home_goals,
                away_goals = EXCLUDED.away_goals,
                match_date = EXCLUDED.match_date,
                referee    = EXCLUDED.referee;

            DROP TABLE IF EXISTS matches_load_tmp;
        """)

        spark.stop()
        return {"rows_loaded": validate_result["rows_validated"], **validate_result}

    # ------------------------------------------------------------------
    # Task 4 — dbt run (unchanged from v5)
    # ------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run && dbt test",
        env={
            "FOOTBALL_DB_HOST": os.getenv("FOOTBALL_DB_HOST", "postgres"),
            "FOOTBALL_DB_PORT": os.getenv("FOOTBALL_DB_PORT", "5432"),
            "FOOTBALL_DB_NAME": os.getenv("FOOTBALL_DB_NAME", "football"),
            "FOOTBALL_DB_USER": os.getenv("FOOTBALL_DB_USER", "airflow"),
            "FOOTBALL_DB_PASSWORD": os.getenv("FOOTBALL_DB_PASSWORD", "airflow"),
            "PATH": os.environ.get("PATH", ""),
        },
        append_env=False,
    )

    # ------------------------------------------------------------------
    # Wire up the chain
    # ------------------------------------------------------------------
    ingest_result = kafka_ingest()
    validate_result = validate(ingest_result)
    load_result = load_postgres(validate_result)
    load_result >> dbt_run


football_pipeline()
