"""Football Pipeline DAG — v15 (SLA + Data Quality)."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SLA callbacks
# ---------------------------------------------------------------------------

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Airflow SLA miss callback — delegates to BreachHandler."""
    try:
        from sla.sla_monitor import SLABreachEvent
        from sla.breach_handler import BreachHandler

        handler = BreachHandler()
        for sla_obj in slas:
            # Map Airflow SLA miss to our internal event
            stage = sla_obj.task_id
            # Retrieve configured threshold from DEFAULT_SLA_THRESHOLDS
            from sla.sla_monitor import DEFAULT_SLA_THRESHOLDS
            expected = DEFAULT_SLA_THRESHOLDS.get(stage, 600)
            event = SLABreachEvent(
                stage=stage,
                expected_seconds=expected,
                actual_seconds=expected + 60,  # conservative estimate on miss
            )
            handler.handle(event)
    except Exception:
        logger.exception("SLA miss callback failed")


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def streaming_ingest(**kwargs: Any) -> None:
    logger.info("streaming_ingest: consuming Kafka → Delta Lake")


def validate(**kwargs: Any) -> None:
    logger.info("validate: running Great Expectations suite")


def load_postgres(**kwargs: Any) -> None:
    logger.info("load_postgres: upserting Delta Lake → PostgreSQL via JDBC")


def dbt_transform(**kwargs: Any) -> None:
    logger.info("dbt_transform: running dbt models")


def ml_train(**kwargs: Any) -> None:
    logger.info("ml_train: training XGBoost model, logging to MLflow")


def batch_predict(**kwargs: Any) -> None:
    logger.info("batch_predict: running batch inference → Delta Lake predictions")


def quality_check(**kwargs: Any) -> None:
    """Run all four data-quality checks after dbt_transform."""
    from sla.quality_checks import QualityChecks

    # In production these counts come from upstream XComs / metadata stores
    ti = kwargs.get("ti")
    kafka_count = int(ti.xcom_pull(task_ids="streaming_ingest", key="kafka_offset_count") or 0)
    delta_count = int(ti.xcom_pull(task_ids="streaming_ingest", key="delta_row_count") or 0)

    checker = QualityChecks()
    results = checker.run_all(
        kafka_offset_count=kafka_count,
        delta_row_count=delta_count,
    )

    failures = [r for r in results if not r.passed]
    if failures:
        failed_checks = ", ".join(r.check_name for r in failures)
        raise ValueError(f"Data quality checks failed: {failed_checks}")

    logger.info("All quality checks passed")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

_ONE_MINUTE = timedelta(minutes=1)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="football_pipeline",
    default_args=default_args,
    description="Production football data pipeline v15 — SLA + Quality",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["football", "production"],
) as dag:

    t_ingest = PythonOperator(
        task_id="streaming_ingest",
        python_callable=streaming_ingest,
        sla=timedelta(minutes=5),
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
        sla=timedelta(minutes=7),  # cumulative from dag start
    )

    t_load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_postgres,
        sla=timedelta(minutes=17),
    )

    t_dbt = PythonOperator(
        task_id="dbt_transform",
        python_callable=dbt_transform,
        sla=timedelta(minutes=32),
    )

    t_quality = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        sla=timedelta(minutes=34),
    )

    t_train = PythonOperator(
        task_id="ml_train",
        python_callable=ml_train,
        sla=timedelta(minutes=64),
    )

    t_predict = PythonOperator(
        task_id="batch_predict",
        python_callable=batch_predict,
        sla=timedelta(minutes=74),
    )

    # Dependencies
    t_ingest >> t_validate >> t_load >> t_dbt >> t_quality >> t_train >> t_predict
