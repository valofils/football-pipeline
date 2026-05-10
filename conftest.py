"""
tests/conftest.py
-----------------
Shared pytest fixtures for football-pipeline v10.
Extends v9 fixtures with MLflow + ML-specific stubs.
"""

from __future__ import annotations

import json
import os
from typing import Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from moto import mock_s3
import boto3

# ---------------------------------------------------------------------------
# Env setup (must happen before any module-level imports that read env vars)
# ---------------------------------------------------------------------------
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("GX_S3_BUCKET", "football-data-lake")
os.environ.setdefault("DELTA_MATCHES_PATH", "s3a://football-data-lake/delta/matches")
os.environ.setdefault(
    "DELTA_PREDICTIONS_PATH", "s3a://football-data-lake/delta/predictions"
)
os.environ.setdefault("MLFLOW_TRACKING_URI", "http://localhost:5000")
os.environ.setdefault("MLFLOW_EXPERIMENT", "football_outcome_predictor_test")

# ---------------------------------------------------------------------------
# Sample data helpers
# ---------------------------------------------------------------------------
SAMPLE_ROWS = [
    {
        "match_id": f"m{i:03d}",
        "season": "2023-24",
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_goals": i % 4,
        "away_goals": (i + 1) % 4,
        "match_date": "2024-01-01",
    }
    for i in range(50)
]


@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(SAMPLE_ROWS)


@pytest.fixture()
def sample_spark_df(spark, sample_df):
    return spark.createDataFrame(sample_df)


# ---------------------------------------------------------------------------
# Spark (reused from v8/v9 — lightweight local session)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("football-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# S3 (moto)
# ---------------------------------------------------------------------------
@pytest.fixture()
def s3_bucket() -> Generator[str, None, None]:
    with mock_s3():
        client = boto3.client("s3", region_name="eu-west-1")
        client.create_bucket(
            Bucket="football-data-lake",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )
        yield "football-data-lake"


# ---------------------------------------------------------------------------
# Great Expectations (from v9)
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_validate():
    from dags.gx_utils import ValidationSummary

    mock = MagicMock(
        return_value=ValidationSummary(success=True, run_id="test-run")
    )
    return mock


@pytest.fixture()
def mock_validate_failure():
    from dags.gx_utils import ValidationSummary, DataQualityError

    summary = ValidationSummary(
        success=False,
        run_id="fail-run",
        failed_expectations=[
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "match_id"}, "observed_value": None}
        ],
    )
    mock = MagicMock(side_effect=DataQualityError("Quality failed", summary=summary))
    return mock


# ---------------------------------------------------------------------------
# MLflow fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_mlflow_client():
    """Stub MlflowClient with one registered model version."""
    client = MagicMock()
    version = MagicMock()
    version.version = "1"
    version.run_id = "test-mlflow-run-id"
    client.get_latest_versions.return_value = [version]
    return client


@pytest.fixture()
def mock_xgb_model():
    """Stub XGBoost model that returns deterministic predictions."""
    import numpy as np

    model = MagicMock()
    model.predict.return_value = np.array([2, 0, 1] * 17)[:50]  # 50 labels
    model.predict_proba.return_value = np.tile([0.2, 0.3, 0.5], (50, 1))
    return model


@pytest.fixture()
def mock_ml_train():
    return MagicMock(return_value="mock-run-id-abc123")


@pytest.fixture()
def mock_ml_predict():
    return MagicMock(return_value=10)
