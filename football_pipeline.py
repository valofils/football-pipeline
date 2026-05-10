"""
dags/football_pipeline.py
--------------------------
v10 — MLOps layer added on top of v9 (Great Expectations).

DAG chain:
  kafka_ingest >> validate >> load_postgres >> dbt_run
                                            >> ml_train >> predict
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dags.gx_utils import validate_dataframe, DataQualityError  # noqa: F401
from ml.train import train as ml_train_fn
from ml.predict import predict as ml_predict_fn

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "football_matches")
DELTA_MATCHES_PATH = os.getenv(
    "DELTA_MATCHES_PATH", "s3a://football-data-lake/delta/matches"
)
DELTA_PREDICTIONS_PATH = os.getenv(
    "DELTA_PREDICTIONS_PATH", "s3a://football-data-lake/delta/predictions"
)
POSTGRES_CONN_ID = "football_postgres"
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Spark factory
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "football-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def kafka_ingest(delta_path: str = DELTA_MATCHES_PATH, **_) -> None:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "airflow-ingest",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    records = []
    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            import json
            records.append(json.loads(msg.value().decode()))
    finally:
        consumer.close()

    if not records:
        logger.info("No messages consumed — skipping write.")
        return

    spark = get_spark("football-ingest")
    df = spark.createDataFrame(records)

    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, delta_path):
        dt = DeltaTable.forPath(spark, delta_path)
        dt.alias("t").merge(
            df.alias("s"), "t.match_id = s.match_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").save(delta_path)

    logger.info("Ingested %d records into %s", len(records), delta_path)


def validate(delta_path: str = DELTA_MATCHES_PATH, **_) -> None:
    spark = get_spark("football-validate")
    df = spark.read.format("delta").load(delta_path)
    validate_dataframe(spark_df=df, run_id=datetime.utcnow().isoformat())


def load_postgres(delta_path: str = DELTA_MATCHES_PATH, **_) -> None:
    spark = get_spark("football-load")
    df = spark.read.format("delta").load(delta_path)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df.toPandas().to_sql("matches", engine, if_exists="replace", index=False)
    logger.info("Loaded %d rows to postgres.matches", df.count())


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
    spark = get_spark("football-predict")
    written = ml_predict_fn(spark=spark)
    logger.info("predict task wrote %d new predictions", written)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="football_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["football", "mlops"],
) as dag:

    t_ingest = PythonOperator(task_id="kafka_ingest", python_callable=kafka_ingest)
    t_validate = PythonOperator(task_id="validate", python_callable=validate)
    t_load = PythonOperator(task_id="load_postgres", python_callable=load_postgres)
    t_dbt = PythonOperator(task_id="dbt_run", python_callable=dbt_run)
    t_train = PythonOperator(task_id="ml_train", python_callable=ml_train)
    t_predict = PythonOperator(task_id="predict", python_callable=predict)

    t_ingest >> t_validate >> t_load >> [t_dbt, t_train]
    t_train >> t_predict
