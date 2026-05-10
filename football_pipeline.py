"""
dags/football_pipeline.py
--------------------------
v11 — Spark Structured Streaming replaces the batch Kafka consumer.

Task chain:
    streaming_ingest >> validate >> load_postgres >> [dbt_run, ml_train] >> predict

streaming_ingest:
    Submits streaming/stream_ingest.py with --once so Airflow can treat it
    as a finite task: the job drains all available Kafka offsets then exits.
    Uses BashOperator + spark-submit (compatible with local Compose cluster).
    In production, swap for SparkSubmitOperator or KubernetesPodOperator.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Shared helpers (carried forward from v10)
# ---------------------------------------------------------------------------
from gx_utils import DataQualityError, validate_dataframe
from ml.predict import predict as ml_predict_fn
from ml.train import train as ml_train_fn

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths / config
# ---------------------------------------------------------------------------
DELTA_PATH = os.getenv("DELTA_PATH", "s3a://football-data/delta/matches")
PREDICTIONS_PATH = os.getenv(
    "PREDICTIONS_DELTA_PATH", "s3a://football-data/delta/predictions"
)
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT_BIN", "spark-submit")
STREAM_SCRIPT = os.getenv(
    "STREAM_SCRIPT_PATH", "/opt/airflow/streaming/stream_ingest.py"
)

# Spark submit options injected from docker-compose via environment variable
# e.g. --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
SPARK_SUBMIT_OPTS = os.getenv("SPARK_SUBMIT_OPTIONS", "")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Task callables (Python-based tasks unchanged from v10)
# ---------------------------------------------------------------------------


def _get_spark(app_name: str = "football-airflow"):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def validate(**_) -> None:
    spark = _get_spark("football-validate")
    df = spark.read.format("delta").load(DELTA_PATH)
    validate_dataframe(df)
    logger.info("Validation passed.")


def load_postgres(**_) -> None:
    import os

    spark = _get_spark("football-load-pg")
    df = spark.read.format("delta").load(DELTA_PATH)
    pandas_df = df.toPandas()

    import sqlalchemy

    pg_url = os.getenv(
        "POSTGRES_CONN",
        "postgresql+psycopg2://airflow:airflow@airflow-db:5432/airflow",
    )
    engine = sqlalchemy.create_engine(pg_url)
    pandas_df.to_sql("matches", engine, if_exists="replace", index=False)
    logger.info("Loaded %d rows to postgres.matches", len(pandas_df))


def dbt_run(**_) -> None:
    import subprocess

    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")
    logger.info(result.stdout)


def ml_train(**context) -> None:
    run_id = ml_train_fn(run_name=f"airflow_{context['ds_nodash']}")
    context["ti"].xcom_push(key="mlflow_run_id", value=run_id)
    logger.info("MLflow run_id: %s", run_id)


def predict(**_) -> None:
    spark = _get_spark("football-predict")
    written = ml_predict_fn(spark=spark)
    logger.info("predict task wrote %d new predictions", written)


# ---------------------------------------------------------------------------
# Spark-submit command for the streaming task
# ---------------------------------------------------------------------------
STREAM_CMD = (
    f"{SPARK_SUBMIT} "
    f"${{SPARK_SUBMIT_OPTIONS}} "   # noqa: E501 — Airflow expands env vars in BashOperator
    f"{STREAM_SCRIPT} --once"
)

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="football_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["football", "streaming", "mlops"],
) as dag:

    # v11: streaming_ingest replaces the old kafka_ingest PythonOperator.
    # --once makes the job finite so Airflow can mark it success/failed.
    t_stream = BashOperator(
        task_id="streaming_ingest",
        bash_command=STREAM_CMD,
        env={**os.environ, "SPARK_SUBMIT_OPTIONS": SPARK_SUBMIT_OPTS},
    )

    t_validate = PythonOperator(task_id="validate", python_callable=validate)
    t_load = PythonOperator(task_id="load_postgres", python_callable=load_postgres)
    t_dbt = PythonOperator(task_id="dbt_run", python_callable=dbt_run)
    t_train = PythonOperator(task_id="ml_train", python_callable=ml_train)
    t_predict = PythonOperator(task_id="predict", python_callable=predict)

    t_stream >> t_validate >> t_load >> [t_dbt, t_train]
    t_train >> t_predict
