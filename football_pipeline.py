"""
football_pipeline.py — v9: Great Expectations data quality layer.

DAG chain (unchanged structure):
    kafka_ingest >> validate >> load_postgres >> dbt_run

Change from v8:
    validate task:  hand-rolled null checks → GE checkpoint via gx_utils.validate_dataframe()
    On any expectation failure the task raises DataQualityError,
    marking the DAG run as failed and preventing downstream tasks from executing.
    Checkpoint results + Data Docs are written to S3 by the GE action list.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.gx_utils import validate_dataframe
from dags.spark_utils import (
    delta_path,
    get_spark,
    jdbc_params_from_env,
    merge_delta,
    write_delta,
)
from kafka.consumer.consume_matches import consume_matches

MATCHES_TABLE = "matches"
DBT_DIR = "/opt/airflow/dbt"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
}


@dag(
    dag_id="football_pipeline",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["football", "v9"],
)
def football_pipeline():

    @task()
    def kafka_ingest() -> str:
        """Consume Kafka topic → write to Delta Lake on S3."""
        spark = get_spark()
        rows = consume_matches()
        df = spark.createDataFrame(rows)
        write_delta(df, MATCHES_TABLE, mode="append")
        spark.stop()
        return delta_path(MATCHES_TABLE)

    @task()
    def validate(delta_table_path: str, **context) -> str:
        """
        Read Delta table → run Great Expectations checkpoint.

        Raises DataQualityError (→ task failure) if any expectation fails.
        Checkpoint results and Data Docs are stored on S3 by the GE action list.
        """
        spark = get_spark()
        df = spark.read.format("delta").load(delta_table_path)

        run_id = context["run_id"]  # Airflow logical run_id
        validate_dataframe(spark_df=df, run_id=run_id, raise_on_failure=True)

        spark.stop()
        return delta_table_path

    @task()
    def load_postgres(delta_table_path: str) -> None:
        """Delta Lake → RDS PostgreSQL via Spark JDBC + ON CONFLICT upsert."""
        spark = get_spark()
        df = spark.read.format("delta").load(delta_table_path)

        jdbc_url, props = jdbc_params_from_env()
        df.write.jdbc(
            url=jdbc_url,
            table="matches_staging",
            mode="overwrite",
            properties=props,
        )

        hook = PostgresHook(postgres_conn_id="football_db")
        hook.run("""
            INSERT INTO matches
                SELECT * FROM matches_staging
            ON CONFLICT (match_id) DO UPDATE SET
                home_goals  = EXCLUDED.home_goals,
                away_goals  = EXCLUDED.away_goals,
                match_date  = EXCLUDED.match_date;
        """)
        spark.stop()

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run && dbt test",
    )

    path = kafka_ingest()
    validated = validate(path)
    load_postgres(validated) >> dbt_run


football_pipeline()
