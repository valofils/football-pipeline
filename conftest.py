"""
tests/conftest.py
-----------------
v11 — adds streaming fixtures on top of v10 (MLflow / XGBoost) fixtures.
All previous fixtures are carried forward unchanged.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# SparkSession (shared across all tests)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("football-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Shared sample data
# ---------------------------------------------------------------------------
SAMPLE_MATCHES = [
    ("m001", "2023-01", "Arsenal", "Chelsea", 2, 1),
    ("m002", "2023-01", "Liverpool", "ManCity", 1, 1),
    ("m003", "2023-02", "Tottenham", "Everton", 0, 2),
]

MATCH_SCHEMA = StructType(
    [
        StructField("match_id", StringType(), False),
        StructField("season", StringType(), False),
        StructField("home_team", StringType(), False),
        StructField("away_team", StringType(), False),
        StructField("home_goals", IntegerType(), False),
        StructField("away_goals", IntegerType(), False),
    ]
)


@pytest.fixture()
def sample_matches_df(spark):
    return spark.createDataFrame(SAMPLE_MATCHES, schema=MATCH_SCHEMA)


# ---------------------------------------------------------------------------
# Great Expectations fixtures (v9)
# ---------------------------------------------------------------------------
@pytest.fixture()
def ge_context():
    ctx = MagicMock()
    ctx.get_expectation_suite.return_value = MagicMock()
    ctx.get_validator.return_value = MagicMock()
    return ctx


@pytest.fixture()
def ge_suite():
    suite = MagicMock()
    suite.expectation_suite_name = "matches_suite"
    return suite


@pytest.fixture()
def ge_validator():
    validator = MagicMock()
    result = MagicMock()
    result.success = True
    validator.validate.return_value = result
    return validator


@pytest.fixture()
def mock_validate():
    with patch("gx_utils.validate_dataframe") as m:
        m.return_value = None
        yield m


# ---------------------------------------------------------------------------
# MLflow / XGBoost fixtures (v10)
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_mlflow_client():
    client = MagicMock()
    version = MagicMock()
    version.run_id = "test-run-id"
    version.source = "s3://football-data/mlflow/artifacts/1/test-run-id/artifacts/model"
    client.get_latest_versions.return_value = [version]
    return client


@pytest.fixture()
def mock_xgb_model():
    model = MagicMock()
    import numpy as np

    model.predict.return_value = np.array([2, 1, 0])
    model.predict_proba.return_value = np.array(
        [[0.1, 0.2, 0.7], [0.3, 0.4, 0.3], [0.6, 0.2, 0.2]]
    )
    model.get_booster.return_value = MagicMock()
    return model


@pytest.fixture()
def mock_ml_train():
    with patch("ml.train.train") as m:
        m.return_value = "mock-run-id-abc123"
        yield m


@pytest.fixture()
def mock_ml_predict():
    with patch("ml.predict.predict") as m:
        m.return_value = 3
        yield m


# ---------------------------------------------------------------------------
# Streaming fixtures (v11)
# ---------------------------------------------------------------------------

KAFKA_MESSAGE_SCHEMA = StructType(
    [
        StructField("value", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)


def _make_kafka_row(match: tuple, ts=None) -> dict:
    """Helper: build a mock Kafka row (value=JSON bytes, timestamp)."""
    payload = {
        "match_id": match[0],
        "season": match[1],
        "home_team": match[2],
        "away_team": match[3],
        "home_goals": match[4],
        "away_goals": match[5],
    }
    return {
        "value": json.dumps(payload),
        "timestamp": ts or datetime(2024, 1, 1, tzinfo=timezone.utc),
    }


@pytest.fixture()
def kafka_batch_df(spark):
    """Simulates a parsed Kafka micro-batch DataFrame (post JSON parsing)."""
    rows = [_make_kafka_row(m) for m in SAMPLE_MATCHES]
    raw = spark.createDataFrame(rows, schema=KAFKA_MESSAGE_SCHEMA)

    from pyspark.sql import functions as F
    from streaming.stream_ingest import MATCH_SCHEMA as STREAM_MATCH_SCHEMA

    parsed = raw.select(
        F.from_json(F.col("value"), STREAM_MATCH_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("data.*", "kafka_timestamp")

    return parsed.filter(F.col("match_id").isNotNull())


@pytest.fixture()
def kafka_batch_df_with_nulls(spark):
    """Micro-batch containing one malformed row (unparseable JSON → null match_id)."""
    good = _make_kafka_row(SAMPLE_MATCHES[0])
    bad = {"value": "not-valid-json", "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc)}
    rows = [good, bad]
    raw = spark.createDataFrame(rows, schema=KAFKA_MESSAGE_SCHEMA)

    from pyspark.sql import functions as F
    from streaming.stream_ingest import MATCH_SCHEMA as STREAM_MATCH_SCHEMA

    parsed = raw.select(
        F.from_json(F.col("value"), STREAM_MATCH_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("data.*", "kafka_timestamp")

    return parsed  # intentionally NOT filtered — tests check filter behaviour


@pytest.fixture()
def mock_delta_table():
    dt = MagicMock()
    merge_builder = MagicMock()
    merge_builder.whenNotMatchedInsertAll.return_value = merge_builder
    merge_builder.execute.return_value = None
    dt.alias.return_value = merge_builder
    return dt


@pytest.fixture()
def mock_stream_run():
    """Mocks a Structured Streaming query object."""
    query = MagicMock()
    query.id = "mock-stream-query-id"
    query.isActive = True
    query.awaitTermination.return_value = None
    return query


@pytest.fixture()
def mock_spark_stream(mock_stream_run):
    """Patches build_stream + writeStream to avoid real Kafka connections."""
    with patch("streaming.stream_ingest.build_stream") as mock_build:
        mock_df = MagicMock()
        writer = MagicMock()
        writer.foreachBatch.return_value = writer
        writer.option.return_value = writer
        writer.trigger.return_value = writer
        writer.start.return_value = mock_stream_run
        mock_df.writeStream = writer
        mock_build.return_value = mock_df
        yield mock_build, mock_stream_run
