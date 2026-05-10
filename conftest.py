"""
conftest.py — v9 test fixtures.

Adds on top of v8:
  - ge_context         : FileDataContext pointed at a temp gx root
  - ge_suite           : loaded matches_suite (from JSON on disk)
  - ge_validator       : GE Validator wrapping the in-memory sample DataFrame
  - mock_validate      : monkeypatch for validate_dataframe (unit tests)
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Shared sample rows (reused across all test classes)
# ---------------------------------------------------------------------------
SAMPLE_ROWS = [
    {
        "match_id": 1,
        "season": "2023-24",
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_goals": 2,
        "away_goals": 1,
        "match_date": "2024-01-10",
    },
    {
        "match_id": 2,
        "season": "2023-24",
        "home_team": "Liverpool",
        "away_team": "Man City",
        "home_goals": 1,
        "away_goals": 1,
        "match_date": "2024-01-11",
    },
]

GX_ROOT = Path(__file__).parent.parent / "gx"


# ---------------------------------------------------------------------------
# Spark (session-scoped, shared by all tests)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    tmp_derby = tempfile.mkdtemp(prefix="derby_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("football-pipeline-v9-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={tmp_derby}")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(tmp_derby, ignore_errors=True)


# ---------------------------------------------------------------------------
# Delta table fixture (function-scoped for test isolation)
# ---------------------------------------------------------------------------
@pytest.fixture()
def delta_matches_path(spark, tmp_path):
    path = str(tmp_path / "delta" / "matches")
    df = spark.createDataFrame(SAMPLE_ROWS)
    df.write.format("delta").mode("overwrite").save(path)
    return path


# ---------------------------------------------------------------------------
# AWS / moto fixtures (unchanged from v8)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def aws_credentials():
    os.environ.update(
        {
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_SECURITY_TOKEN": "testing",
            "AWS_SESSION_TOKEN": "testing",
            "AWS_DEFAULT_REGION": "us-east-1",
        }
    )


@pytest.fixture(scope="session")
def s3_bucket(aws_credentials):
    import boto3
    from moto import mock_aws

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "football-test-bucket"
        s3.create_bucket(Bucket=bucket)
        os.environ["S3_BUCKET_NAME"] = bucket
        yield bucket


# ---------------------------------------------------------------------------
# Great Expectations fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def ge_context(tmp_path_factory):
    """FileDataContext backed by a temp directory; expectations loaded from disk."""
    import great_expectations as gx

    tmp_gx = tmp_path_factory.mktemp("gx_root")
    # Copy gx/ tree so tests use real suite JSON without touching project files
    shutil.copytree(GX_ROOT, tmp_gx / "gx")
    ctx = gx.get_context(context_root_dir=str(tmp_gx / "gx"))
    return ctx


@pytest.fixture(scope="session")
def ge_suite(ge_context):
    """Load (or create) the matches_suite in the temp context."""
    suite_path = GX_ROOT / "expectations" / "matches_suite.json"
    import great_expectations as gx

    try:
        return ge_context.get_expectation_suite("matches_suite")
    except Exception:
        with suite_path.open() as fh:
            suite_dict = json.load(fh)
        suite = ge_context.create_expectation_suite(
            "matches_suite", overwrite_existing=True
        )
        for exp in suite_dict["expectations"]:
            suite.add_expectation_configuration(
                gx.core.ExpectationConfiguration(
                    expectation_type=exp["expectation_type"],
                    kwargs=exp["kwargs"],
                )
            )
        ge_context.save_expectation_suite(suite)
        return suite


@pytest.fixture()
def ge_validator(ge_context, ge_suite, spark, delta_matches_path):
    """GE Validator wrapping the sample Delta DataFrame."""
    from great_expectations.core.batch import RuntimeBatchRequest

    df = spark.read.format("delta").load(delta_matches_path)
    batch_request = RuntimeBatchRequest(
        datasource_name="spark_delta_datasource",
        data_connector_name="runtime_connector",
        data_asset_name="matches_delta",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": "pytest", "season": "2023-24"},
    )
    return ge_context.get_validator(
        batch_request=batch_request,
        expectation_suite=ge_suite,
    )


@pytest.fixture()
def mock_validate(monkeypatch):
    """Replace validate_dataframe with a no-op returning a success summary."""
    success_summary = {
        "success": True,
        "evaluated": 15,
        "successful": 15,
        "failed_count": 0,
        "failed_expectations": [],
        "run_id": "mock-run",
    }
    mock = MagicMock(return_value=success_summary)
    monkeypatch.setattr("dags.gx_utils.validate_dataframe", mock)
    return mock


# ---------------------------------------------------------------------------
# Kafka mocks (unchanged from v8)
# ---------------------------------------------------------------------------
class _FakeMessage:
    def __init__(self, value, partition=0, offset=0):
        self._value = value
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


class MockConsumer:
    def __init__(self, messages, hwm):
        self._messages = iter(messages)
        self._hwm = hwm
        self.committed = False

    def subscribe(self, topics):
        pass

    def assignment(self):
        from confluent_kafka import TopicPartition
        return [TopicPartition("matches", 0)]

    def get_watermark_offsets(self, tp):
        return (0, self._hwm)

    def poll(self, timeout=1.0):
        return next(self._messages, None)

    def commit(self, asynchronous=False):
        self.committed = True

    def close(self):
        pass


@pytest.fixture()
def mock_producer():
    with patch("kafka.producer.produce_matches.Producer") as m:
        yield m.return_value


@pytest.fixture()
def pg_conn():
    with patch("kafka.consumer.consume_matches.psycopg2.connect") as m:
        yield m.return_value
