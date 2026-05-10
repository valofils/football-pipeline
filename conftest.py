"""
conftest.py — v12
Cumulative fixtures: v1-v11 baseline + v12 observability fixtures.
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

RAW_MATCH = {
    "match_id": "m001",
    "home_team": "Arsenal",
    "away_team": "Chelsea",
    "home_score": 2,
    "away_score": 1,
    "date": "2024-01-15",
    "season": "2023-24",
    "competition": "Premier League",
}


# ─────────────────────────────────────────────────────────────────────────────
# v1-v2: core data fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_match():
    return RAW_MATCH.copy()


@pytest.fixture
def sample_matches():
    return [
        {**RAW_MATCH, "match_id": f"m{i:03d}", "home_score": i % 5, "away_score": i % 3}
        for i in range(1, 21)
    ]


@pytest.fixture
def parquet_file(tmp_path, sample_matches):
    import pandas as pd
    path = tmp_path / "matches.parquet"
    pd.DataFrame(sample_matches).to_parquet(path, index=False)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# v3: Airflow DAG fixture
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def dag():
    from airflow.models import DagBag
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    return dag_bag.get_dag("football_pipeline")


# ─────────────────────────────────────────────────────────────────────────────
# v4: Spark fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-football-pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def matches_df(spark, sample_matches):
    import pandas as pd
    return spark.createDataFrame(pd.DataFrame(sample_matches))


# ─────────────────────────────────────────────────────────────────────────────
# v5: dbt fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def dbt_project_dir(tmp_path):
    project = tmp_path / "dbt_project"
    project.mkdir()
    (project / "dbt_project.yml").write_text(
        "name: football_pipeline\nversion: '1.0'\nprofile: football_pipeline\n"
    )
    return project


# ─────────────────────────────────────────────────────────────────────────────
# v6: Kafka fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def kafka_message():
    return json.dumps(RAW_MATCH).encode()


@pytest.fixture
def mock_kafka_producer():
    with patch("kafka.KafkaProducer") as mock:
        producer = MagicMock()
        mock.return_value = producer
        yield producer


@pytest.fixture
def mock_kafka_consumer():
    with patch("kafka.KafkaConsumer") as mock:
        consumer = MagicMock()
        consumer.__iter__ = MagicMock(return_value=iter([]))
        mock.return_value = consumer
        yield consumer


# ─────────────────────────────────────────────────────────────────────────────
# v7: AWS fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_s3():
    with patch("boto3.client") as mock:
        s3 = MagicMock()
        mock.return_value = s3
        yield s3


@pytest.fixture
def mock_rds():
    with patch("boto3.client") as mock:
        rds = MagicMock()
        mock.return_value = rds
        yield rds


# ─────────────────────────────────────────────────────────────────────────────
# v8: Delta Lake fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def delta_table_path(tmp_path):
    return str(tmp_path / "delta" / "matches")


@pytest.fixture
def mock_delta_table():
    with patch("delta.tables.DeltaTable") as mock:
        table = MagicMock()
        mock.forPath.return_value = table
        mock.isDeltaTable.return_value = True
        # merge chain
        merge = MagicMock()
        merge.whenMatchedUpdateAll.return_value = merge
        merge.whenNotMatchedInsertAll.return_value = merge
        table.merge.return_value = merge
        yield table


# ─────────────────────────────────────────────────────────────────────────────
# v9: Great Expectations fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_ge_context():
    with patch("great_expectations.get_context") as mock:
        ctx = MagicMock()
        result = MagicMock()
        result.success = True
        result.statistics = {"successful_expectations": 5, "unsuccessful_expectations": 0}
        ctx.run_checkpoint.return_value = result
        mock.return_value = ctx
        yield ctx


# ─────────────────────────────────────────────────────────────────────────────
# v10: MLflow / XGBoost fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_mlflow():
    with patch("mlflow.start_run") as mock_run, \
         patch("mlflow.log_param") as mock_param, \
         patch("mlflow.log_metric") as mock_metric, \
         patch("mlflow.xgboost.log_model") as mock_model:
        run = MagicMock()
        run.__enter__ = MagicMock(return_value=run)
        run.__exit__ = MagicMock(return_value=False)
        run.info.run_id = "test-run-id-001"
        mock_run.return_value = run
        yield {
            "start_run": mock_run,
            "log_param": mock_param,
            "log_metric": mock_metric,
            "log_model": mock_model,
            "run": run,
        }


@pytest.fixture
def sample_features_df():
    import pandas as pd
    import numpy as np
    rng = np.random.default_rng(42)
    n = 100
    return pd.DataFrame(
        {
            "home_goals_avg": rng.uniform(0.5, 3.0, n),
            "away_goals_avg": rng.uniform(0.5, 3.0, n),
            "home_win_rate": rng.uniform(0.2, 0.8, n),
            "away_win_rate": rng.uniform(0.2, 0.8, n),
            "outcome": rng.integers(0, 3, n),
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# v11: Spark Structured Streaming fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def kafka_batch_df(spark):
    """Simulates a micro-batch DataFrame arriving from Kafka."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField("value", StringType(), True)])
    rows = [
        (json.dumps({**RAW_MATCH, "match_id": f"m{i:03d}"}),)
        for i in range(1, 6)
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture
def kafka_batch_df_with_nulls(spark, kafka_batch_df):
    """Kafka batch that includes a null-value row (should be filtered out)."""
    from pyspark.sql import Row
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField("value", StringType(), True)])
    null_rows = spark.createDataFrame([(None,)], schema)
    return kafka_batch_df.union(null_rows)


@pytest.fixture
def mock_stream_run():
    with patch("pyspark.sql.streaming.StreamingQuery") as mock:
        query = MagicMock()
        query.isActive = True
        query.lastProgress = {
            "numInputRows": 50,
            "inputRowsPerSecond": 10.0,
            "processedRowsPerSecond": 9.8,
            "batchId": 42,
        }
        query.exception.return_value = None
        mock.return_value = query
        yield query


@pytest.fixture
def mock_spark_stream(spark):
    with patch.object(spark, "readStream", new_callable=PropertyMock) as mock:
        stream_reader = MagicMock()
        mock.return_value = stream_reader
        stream_reader.format.return_value = stream_reader
        stream_reader.option.return_value = stream_reader
        stream_reader.schema.return_value = stream_reader
        stream_reader.load.return_value = MagicMock()
        yield stream_reader


# ─────────────────────────────────────────────────────────────────────────────
# v12: Observability fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_prometheus_registry():
    """Mock prometheus_client registry with pre-populated metrics."""
    with patch("prometheus_client.REGISTRY") as mock_registry:
        mock_registry.get_sample_value.side_effect = lambda name, labels=None: {
            ("kafka_consumergroup_lag", frozenset({"consumergroup": "football-streaming"}.items())): 42.0,
            ("airflow_dag_run_duration", frozenset({"dag_id": "football_pipeline"}.items())): 1200.0,
            ("airflow_dag_run_failed", frozenset({"dag_id": "football_pipeline"}.items())): 0.0,
            ("airflow_scheduler_heartbeat", frozenset()): 5.0,
            ("pg_up", frozenset()): 1.0,
            ("spark_streaming_batch_duration_ms", frozenset()): 8500.0,
        }.get(
            (name, frozenset((labels or {}).items())), None
        )
        yield mock_registry


@pytest.fixture
def prometheus_alert_rules():
    """Parsed alert rules for structural validation."""
    import yaml
    with open("monitoring/alert_rules.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture
def prometheus_config():
    """Parsed Prometheus scrape config for structural validation."""
    import yaml
    with open("monitoring/prometheus.yml") as f:
        return yaml.safe_load(f)


@pytest.fixture
def grafana_dashboard():
    """Parsed Grafana dashboard JSON for structural validation."""
    with open("monitoring/grafana/dashboards/football_pipeline.json") as f:
        return json.load(f)


@pytest.fixture
def mock_pushgateway():
    """Mock Prometheus Pushgateway for Airflow task metric pushes."""
    with patch("prometheus_client.push_to_gateway") as mock:
        mock.return_value = None
        yield mock


@pytest.fixture
def mock_grafana_api():
    """Mock Grafana HTTP API responses."""
    with patch("requests.get") as mock_get, patch("requests.post") as mock_post:
        health_response = MagicMock()
        health_response.status_code = 200
        health_response.json.return_value = {"database": "ok", "version": "11.0.0"}
        mock_get.return_value = health_response

        ds_response = MagicMock()
        ds_response.status_code = 200
        ds_response.json.return_value = [{"id": 1, "name": "Prometheus", "type": "prometheus"}]
        mock_post.return_value = ds_response

        yield {"get": mock_get, "post": mock_post}


@pytest.fixture
def sample_lag_series():
    """Simulated Kafka consumer lag time-series for alert threshold tests."""
    return [
        {"timestamp": 1700000000 + i * 15, "lag": i * 100}
        for i in range(60)  # 15-minute window, lag climbing to 5900
    ]
