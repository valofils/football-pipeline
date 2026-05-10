"""
dags/football_pipeline.py  —  v13
Adds OpenLineage lineage emission around every pipeline task.
All six stages now emit START / COMPLETE / FAIL RunEvents to Marquez.

Pipeline:
    streaming_ingest >> validate >> load_postgres >> [dbt_run, ml_train] >> predict
"""

from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# lineage emitters (module mounted into the Airflow container)
from lineage.emitters import (
    emit_streaming_ingest,
    emit_validation,
    emit_load_postgres,
    emit_dbt_run,
    emit_ml_train,
    emit_predict,
)

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "football-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_SUBMIT_OPTIONS = os.getenv(
    "SPARK_SUBMIT_OPTIONS",
    "--packages io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
)
STREAM_SCRIPT_PATH = os.getenv(
    "STREAM_SCRIPT_PATH", "/opt/airflow/streaming/stream_ingest.py"
)


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def _run_streaming_ingest(**ctx):
    run_id = str(uuid.uuid4())
    with emit_streaming_ingest(run_id=run_id):
        cmd = (
            f"spark-submit {SPARK_SUBMIT_OPTIONS} {STREAM_SCRIPT_PATH} --once"
        )
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)


def _run_validate(**ctx):
    run_id = str(uuid.uuid4())
    with emit_validation(run_id=run_id):
        from dags.gx_utils import run_checkpoint  # noqa: PLC0415
        run_checkpoint()


def _run_load_postgres(**ctx):
    run_id = str(uuid.uuid4())
    with emit_load_postgres(run_id=run_id):
        from dags.postgres_loader import load  # noqa: PLC0415
        load()


def _run_dbt(**ctx):
    run_id = str(uuid.uuid4())
    with emit_dbt_run(run_id=run_id):
        result = subprocess.run(
            "dbt run --profiles-dir /opt/airflow/dbt_project --project-dir /opt/airflow/dbt_project",
            shell=True,
            check=True,
            capture_output=True,
            text=True,
        )
        print(result.stdout)


def _run_ml_train(**ctx):
    run_id = str(uuid.uuid4())
    with emit_ml_train(run_id=run_id):
        from ml.train import train  # noqa: PLC0415
        train()


def _run_predict(**ctx):
    run_id = str(uuid.uuid4())
    with emit_predict(run_id=run_id):
        from ml.predict import predict  # noqa: PLC0415
        predict()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="football_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["football", "streaming", "mlops", "lineage"],
) as dag:

    streaming_ingest = PythonOperator(
        task_id="streaming_ingest",
        python_callable=_run_streaming_ingest,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=_run_validate,
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=_run_load_postgres,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=_run_dbt,
    )

    ml_train = PythonOperator(
        task_id="ml_train",
        python_callable=_run_ml_train,
    )

    predict = PythonOperator(
        task_id="predict",
        python_callable=_run_predict,
    )

    streaming_ingest >> validate >> load_postgres >> [dbt_run, ml_train] >> predict
