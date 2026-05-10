"""
streaming/stream_ingest.py
--------------------------
v11 — Spark Structured Streaming job.

Replaces the finite-batch Kafka consumer (kafka_ingest) with a
long-running readStream job that continuously merges new messages
into the Delta table using foreachBatch + DeltaTable.merge().

Schema:
  Kafka value (JSON) → matches schema → delta/matches

Usage (standalone):
    spark-submit streaming/stream_ingest.py [--once]

    --once   triggers Trigger.Once() for CI / Airflow one-shot runs
             (default: continuous with processingTime='10 seconds')
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Iterator

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "football_matches")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
DELTA_PATH = os.getenv("DELTA_PATH", "s3a://football-data/delta/matches")
CHECKPOINT_PATH = os.getenv(
    "STREAMING_CHECKPOINT_PATH", "s3a://football-data/checkpoints/matches"
)
TRIGGER_INTERVAL = os.getenv("STREAMING_TRIGGER_INTERVAL", "10 seconds")

# ---------------------------------------------------------------------------
# Schema for the JSON payload inside each Kafka message value
# ---------------------------------------------------------------------------
MATCH_SCHEMA = StructType(
    [
        StructField("match_id", StringType(), nullable=False),
        StructField("season", StringType(), nullable=False),
        StructField("home_team", StringType(), nullable=False),
        StructField("away_team", StringType(), nullable=False),
        StructField("home_goals", IntegerType(), nullable=False),
        StructField("away_goals", IntegerType(), nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def get_spark(app_name: str = "football-stream-ingest") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3 / MinIO / Moto settings are injected by docker-compose via
        # SPARK_DEFAULTS_CONF or spark-defaults.conf — not hard-coded here.
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------------------------------------------
# foreachBatch handler
# ---------------------------------------------------------------------------
def _upsert_to_delta(batch_df: DataFrame, batch_id: int) -> None:
    """Called by Structured Streaming for every micro-batch.

    Merges new rows into delta/matches on match_id (idempotent).
    Creates the table on first call if it does not yet exist.
    """
    if batch_df.isEmpty():
        logger.info("Batch %d is empty — skipping.", batch_id)
        return

    spark = batch_df.sparkSession

    if DeltaTable.isDeltaTable(spark, DELTA_PATH):
        dt = DeltaTable.forPath(spark, DELTA_PATH)
        (
            dt.alias("existing")
            .merge(
                batch_df.alias("incoming"),
                "existing.match_id = incoming.match_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First batch — write as new Delta table
        batch_df.write.format("delta").mode("overwrite").save(DELTA_PATH)

    logger.info(
        "Batch %d: upserted %d rows into %s",
        batch_id,
        batch_df.count(),
        DELTA_PATH,
    )


# ---------------------------------------------------------------------------
# Stream definition
# ---------------------------------------------------------------------------
def build_stream(spark: SparkSession) -> "DataStreamWriter":
    """Constructs the streaming query but does not start it."""
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka value is bytes → cast to string → parse JSON
    parsed = raw_stream.select(
        F.from_json(F.col("value").cast("string"), MATCH_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("data.*", "kafka_timestamp")

    # Drop rows that failed JSON parsing (null match_id)
    clean = parsed.filter(F.col("match_id").isNotNull())

    return clean


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
def run(once: bool = False, spark: SparkSession | None = None) -> None:
    spark = spark or get_spark()
    stream_df = build_stream(spark)

    writer = stream_df.writeStream.foreachBatch(_upsert_to_delta).option(
        "checkpointLocation", CHECKPOINT_PATH
    )

    if once:
        query = writer.trigger(once=True).start()
    else:
        query = writer.trigger(processingTime=TRIGGER_INTERVAL).start()

    logger.info(
        "Streaming query started (id=%s, once=%s)", query.id, once
    )
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="Football Structured Streaming ingest")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Use Trigger.Once() — process available data then exit",
    )
    args = parser.parse_args()
    run(once=args.once)
