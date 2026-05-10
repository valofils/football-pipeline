"""
football_pipeline.py — Airflow DAG for football-pipeline-v8.

Changes from v7
---------------
* ``kafka_ingest``: writes Parquet → writes Delta (``write_delta``).
* ``validate``:     reads Parquet → reads Delta format.
* ``load_postgres``: reads Parquet → reads Delta; upsert path unchanged.
* ``dbt_run``:      unchanged.

DAG chain: kafka_ingest >> validate >> load_postgres >> dbt_run
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "football-matches")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MATCHES_TABLE = "matches"
STAGING_TABLE = "matches_staging"
DBT_DIR = "/opt/airflow/dbt"


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="football_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["football", "v8", "delta"],
)
def football_pipeline():

    # ------------------------------------------------------------------
    # Task 1 — consume Kafka → write Delta on S3
    # ------------------------------------------------------------------
    @task()
    def kafka_ingest() -> str:
        """
        Consume a finite batch from the Kafka topic and write rows to the
        Delta table ``delta/matches`` on S3.

        Returns the Delta table path for downstream tasks.
        """
        from confluent_kafka import Consumer, TopicPartition

        from dags.spark_utils import MATCH_SCHEMA, delta_path, get_spark, write_delta

        consumer_cfg = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "airflow-ingest",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = Consumer(consumer_cfg)

        # Snapshot high-water marks before subscribing
        metadata = consumer.list_topics(KAFKA_TOPIC, timeout=10)
        partitions = [
            TopicPartition(KAFKA_TOPIC, p)
            for p in metadata.topics[KAFKA_TOPIC].partitions
        ]
        hwms = {
            tp.partition: consumer.get_watermark_offsets(tp, timeout=10)[1]
            for tp in partitions
        }
        consumer.assign(partitions)

        rows: list[dict] = []
        reached: dict[int, bool] = {p: (hwm == 0) for p, hwm in hwms.items()}

        while not all(reached.values()):
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("Kafka error: %s", msg.error())
                continue
            rows.append(json.loads(msg.value().decode()))
            part = msg.partition()
            if msg.offset() + 1 >= hwms[part]:
                reached[part] = True
            consumer.commit(message=msg)

        consumer.close()
        log.info("Consumed %d rows from Kafka.", len(rows))

        if not rows:
            log.warning("No rows consumed — skipping Delta write.")
            return delta_path(MATCHES_TABLE)

        spark = get_spark()
        df = spark.createDataFrame(rows, schema=MATCH_SCHEMA)

        # v8: write Delta instead of raw Parquet
        write_delta(df, MATCHES_TABLE, mode="append")
        log.info("Written %d rows to Delta table: %s", df.count(), delta_path(MATCHES_TABLE))

        spark.stop()
        return delta_path(MATCHES_TABLE)

    # ------------------------------------------------------------------
    # Task 2 — validate Delta table
    # ------------------------------------------------------------------
    @task()
    def validate(delta_table_path: str) -> str:
        """Read the Delta table and assert no nulls on critical columns."""
        from pyspark.sql.functions import col

        from dags.spark_utils import get_spark

        spark = get_spark()
        # v8: read as Delta format
        df = spark.read.format("delta").load(delta_table_path)

        for column in ("match_id", "season", "home_team", "away_team"):
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                raise ValueError(
                    f"Validation failed: {null_count} null(s) in column '{column}'."
                )

        total = df.count()
        log.info("Validation passed — %d rows, zero nulls on key columns.", total)
        spark.stop()
        return delta_table_path

    # ------------------------------------------------------------------
    # Task 3 — load Postgres via JDBC (MERGE at storage + upsert at DB)
    # ------------------------------------------------------------------
    @task()
    def load_postgres(delta_table_path: str) -> None:
        """
        Read the Delta table, write to a staging table via JDBC, then upsert
        into the canonical ``matches`` table via PostgresHook.
        """
        from dags.spark_utils import get_spark, jdbc_params_from_env

        spark = get_spark()
        url, props = jdbc_params_from_env()

        # v8: read Delta instead of Parquet
        df = spark.read.format("delta").load(delta_table_path)

        # Write staging (overwrite on every run — idempotent)
        (
            df.write.jdbc(
                url=url,
                table=STAGING_TABLE,
                mode="overwrite",
                properties=props,
            )
        )

        upsert_sql = f"""
            INSERT INTO {MATCHES_TABLE}
            SELECT * FROM {STAGING_TABLE}
            ON CONFLICT (match_id) DO UPDATE SET
                home_goals = EXCLUDED.home_goals,
                away_goals = EXCLUDED.away_goals,
                result     = EXCLUDED.result,
                xg_home    = EXCLUDED.xg_home,
                xg_away    = EXCLUDED.xg_away;
        """
        hook = PostgresHook(postgres_conn_id="football_db")
        hook.run(upsert_sql)
        log.info("Upserted rows from staging into %s.", MATCHES_TABLE)
        spark.stop()

    # ------------------------------------------------------------------
    # Task 4 — dbt (unchanged from v5–v7)
    # ------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run && dbt test",
    )

    # ------------------------------------------------------------------
    # Wire tasks
    # ------------------------------------------------------------------
    path = kafka_ingest()
    validated_path = validate(path)
    load_postgres(validated_path) >> dbt_run


football_pipeline()
