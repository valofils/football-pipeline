"""
conftest.py — shared pytest fixtures (v7)

New vs v6:
- s3_bucket: moto-mocked S3 bucket (no real AWS calls)
- spark: S3A configured to hit the moto mock endpoint
- Existing fixtures (mock_producer, MockConsumer, pg_conn) unchanged
"""
from __future__ import annotations

import json
import os
from typing import Generator
from unittest.mock import MagicMock

import boto3
import psycopg2
import pytest
from moto import mock_aws
from pyspark.sql import SparkSession

# ── Constants ─────────────────────────────────────────────────────────────────

TEST_BUCKET   = "test-football-parquet-lake"
TEST_REGION   = "eu-west-1"
KAFKA_TOPIC   = "match-events"


# ── S3 / moto ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def aws_credentials():
    """Fake AWS credentials so moto never calls real AWS."""
    os.environ["AWS_ACCESS_KEY_ID"]     = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"]    = "testing"
    os.environ["AWS_SESSION_TOKEN"]     = "testing"
    os.environ["AWS_DEFAULT_REGION"]    = TEST_REGION
    os.environ["S3_BUCKET_NAME"]        = TEST_BUCKET


@pytest.fixture(scope="session")
def s3_bucket(aws_credentials):
    """
    Session-scoped moto S3 mock.
    Yields a boto3 Bucket resource pointing at the test bucket.
    """
    with mock_aws():
        s3 = boto3.resource("s3", region_name=TEST_REGION)
        s3.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
        )
        yield s3.Bucket(TEST_BUCKET)


# ── Spark ─────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark(s3_bucket) -> Generator[SparkSession, None, None]:
    """
    Session-scoped SparkSession.
    Configured for local mode; S3A pointed at moto mock via hadoop-aws.
    """
    spark_jars = os.environ.get("SPARK_JARS", "")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("football-pipeline-v7-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key",    "testing")
        .config("spark.hadoop.fs.s3a.secret.key",    "testing")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint",
                "http://localhost:5555")  # moto server or override in CI
        .config("spark.jars", spark_jars)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Kafka mocks ───────────────────────────────────────────────────────────────

class _FakeMessage:
    def __init__(self, value: dict, partition: int = 0, offset: int = 0):
        self._value     = json.dumps(value).encode()
        self._partition = partition
        self._offset    = offset

    def value(self):     return self._value
    def partition(self): return self._partition
    def offset(self):    return self._offset
    def error(self):     return None


class MockConsumer:
    """Deterministic replay of a fixed message list — no real broker needed."""

    def __init__(self, messages: list[dict]):
        self._messages = [_FakeMessage(m, partition=0, offset=i) for i, m in enumerate(messages)]
        self._index    = 0
        self._hwm      = {0: len(messages)}

    def subscribe(self, topics): pass
    def close(self):             pass
    def commit(self, message=None): pass

    def poll(self, timeout=1.0):
        if self._index >= len(self._messages):
            return None
        msg = self._messages[self._index]
        self._index += 1
        return msg

    def get_watermark_offsets(self, tp, timeout=10):
        return (0, self._hwm.get(tp.partition, 0))

    def list_topics(self, topic, timeout=10):
        from types import SimpleNamespace
        partition = SimpleNamespace(id=0)
        topic_obj = SimpleNamespace(partitions={0: partition})
        return SimpleNamespace(topics={topic: topic_obj})


@pytest.fixture
def mock_producer():
    return MagicMock()


# ── Postgres ──────────────────────────────────────────────────────────────────

SAMPLE_MATCHES = [
    {
        "match_id": 1, "season": "2023-24", "date": "2023-08-12",
        "home_team": "Arsenal", "away_team": "Nottm Forest",
        "home_goals": 2, "away_goals": 1, "result": "H",
    },
    {
        "match_id": 2, "season": "2023-24", "date": "2023-08-12",
        "home_team": "Burnley", "away_team": "Man City",
        "home_goals": 0, "away_goals": 3, "result": "A",
    },
]


@pytest.fixture
def pg_conn():
    """
    Function-scoped Postgres connection.
    Each test rolls back so DB state stays clean.
    Auto-skipped when Postgres is unreachable.
    """
    dsn = {
        "host":     os.environ.get("FOOTBALL_DB_HOST", "localhost"),
        "port":     int(os.environ.get("FOOTBALL_DB_PORT", 5432)),
        "dbname":   os.environ.get("FOOTBALL_DB_NAME", "football"),
        "user":     os.environ.get("FOOTBALL_DB_USER", "football_user"),
        "password": os.environ.get("FOOTBALL_DB_PASSWORD", ""),
    }
    try:
        conn = psycopg2.connect(**dsn)
    except psycopg2.OperationalError:
        pytest.skip("Postgres not reachable")
        return
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()
