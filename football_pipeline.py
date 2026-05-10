"""
dags/football_pipeline.py — v13 DAG.

Every task callable wraps its business logic in the matching emit_*
context manager from lineage/emitters.py. A shared pipeline_run_id
threads through all stages so Marquez can correlate the full run.
"""

from __future__ import annotations

import uuid
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "football-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def _streaming_ingest(**context) -> None:
    """Consume Kafka topic → Delta Lake."""
    from lineage.emitters import emit_streaming_ingest

    run_id = context["dag_run"].run_id
    logger.info("streaming_ingest starting, pipeline_run_id=%s", run_id)

    with emit_streaming_ingest(run_id=run_id):
        # --- real work (stub) ---
        logger.info("Consuming football_matches topic → delta/matches")
        # e.g.: spark.readStream.format("kafka")...writeStream.format("delta")...


def _validate_matches(**context) -> None:
    """Great Expectations validation on Delta table."""
    from lineage.emitters import emit_validation

    run_id = context["dag_run"].run_id

    with emit_validation(run_id=run_id):
        logger.info("Running GE suite on delta/matches")
        # e.g.: ge_context.run_checkpoint("matches_checkpoint")


def _load_postgres(**context) -> None:
    """Delta Lake → PostgreSQL upsert."""
    from lineage.emitters import emit_load_postgres

    run_id = context["dag_run"].run_id

    with emit_load_postgres(run_id=run_id):
        logger.info("Upserting delta/matches → public.matches")
        # e.g.: spark.read.format("delta").load(...).write.jdbc(...)


def _dbt_run(**context) -> None:
    """dbt transformation: matches → mart_team_season_stats."""
    from lineage.emitters import emit_dbt_run
    import subprocess

    run_id = context["dag_run"].run_id

    with emit_dbt_run(run_id=run_id):
        logger.info("Running dbt models")
        # subprocess.run(["dbt", "run", "--select", "mart_team_season_stats"], check=True)


def _ml_train(**context) -> None:
    """Train outcome predictor and register in MLflow."""
    from lineage.emitters import emit_ml_train

    run_id = context["dag_run"].run_id

    with emit_ml_train(run_id=run_id):
        logger.info("Training football outcome predictor")
        # mlflow.sklearn.log_model(model, "football_outcome_predictor")


def _batch_predict(**context) -> None:
    """Batch inference → Delta Lake predictions."""
    from lineage.emitters import emit_predict

    run_id = context["dag_run"].run_id

    with emit_predict(run_id=run_id):
        logger.info("Running batch predictions → delta/predictions")
        # model = mlflow.pyfunc.load_model("models:/football_outcome_predictor/Production")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="football_pipeline_v13",
    default_args=DEFAULT_ARGS,
    description="Football data pipeline v13 — full lineage via OpenLineage + Marquez",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["football", "v13", "lineage"],
) as dag:

    t_ingest = PythonOperator(
        task_id="streaming_ingest",
        python_callable=_streaming_ingest,
    )

    t_validate = PythonOperator(
        task_id="validate_matches",
        python_callable=_validate_matches,
    )

    t_load_pg = PythonOperator(
        task_id="load_postgres",
        python_callable=_load_postgres,
    )

    t_dbt = PythonOperator(
        task_id="dbt_run",
        python_callable=_dbt_run,
    )

    t_train = PythonOperator(
        task_id="ml_train",
        python_callable=_ml_train,
    )

    t_predict = PythonOperator(
        task_id="batch_predict",
        python_callable=_batch_predict,
    )

    # Dependency chain
    t_ingest >> t_validate >> t_load_pg >> [t_dbt, t_train]
    t_train >> t_predict
