"""
conftest.py — pytest fixtures for football-pipeline-v8.

Changes from v7
---------------
* ``spark`` fixture: Delta extensions added to builder; local metastore
  via derby created in a temp dir so tests don't collide.
* ``delta_matches_path`` fixture: creates a throwaway Delta table from
  sample rows and yields its path — consumed by Delta-specific tests.
* ``aws_credentials`` and ``s3_bucket`` fixtures unchanged from v7.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_ROWS: list[dict[str, Any]] = [
    {
        "match_id": 1,
        "season": "2023-24",
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_goals": 2,
        "away_goals": 1,
        "result": "H",
        "xg_home": 1.8,
        "xg_away": 0.9,
    },
    {
        "match_id": 2,
        "season": "2023-24",
        "home_team": "Liverpool",
        "away_team": "Man City",
        "home_goals": 1,
        "away_goals": 1,
        "result": "D",
        "xg_home": 1.2,
        "xg_away": 1.4,
    },
    {
        "match_id": 3,
        "season": "2022-23",
        "home_team": "Man City",
        "away_team": "Arsenal",
        "home_goals": 4,
        "away_goals": 1,
        "result": "H",
        "xg_home": 3.5,
        "xg_away": 0.7,
    },
]

MATCH_SCHEMA = StructType(
    [
        StructField("match_id", IntegerType(), nullable=False),
        StructField("season", StringType(), nullable=False),
        StructField("home_team", StringType(), nullable=False),
        StructField("away_team", StringType(), nullable=False),
        StructField("home_goals", IntegerType(), nullable=True),
        StructField("away_goals", IntegerType(), nullable=True),
        StructField("result", StringType(), nullable=True),
        StructField("xg_home", DoubleType(), nullable=True),
        StructField("xg_away", DoubleType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Spark — session-scoped with Delta extensions
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, None, None]:
    """
    Session-scoped SparkSession with Delta Lake extensions enabled.

    Uses a per-session temp dir for the derby metastore so parallel test
    runs don't conflict.
    """
    from delta import configure_spark_with_delta_pip

    derby_dir = tmp_path_factory.mktemp("derby")
    warehouse_dir = tmp_path_factory.mktemp("warehouse")

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("football-pipeline-v8-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Delta table fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def delta_matches_path(spark: SparkSession, tmp_path: Any) -> str:
    """
    Write SAMPLE_ROWS to a local Delta table and return its path.

    Function-scoped so each test starts with a clean table.
    """
    path = str(tmp_path / "delta" / "matches")
    df = spark.createDataFrame(SAMPLE_ROWS, schema=MATCH_SCHEMA)
    df.write.format("delta").mode("overwrite").partitionBy("season").save(path)
    return path


# ---------------------------------------------------------------------------
# AWS / moto fixtures (unchanged from v7)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def aws_credentials() -> None:
    """Inject fake AWS credentials so moto intercepts all boto3 calls."""
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
    os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="session")
def s3_bucket(aws_credentials: None):  # noqa: F811
    """Create a mocked S3 bucket and set S3_BUCKET_NAME env var."""
    import boto3
    from moto import mock_aws

    bucket_name = "football-test-bucket"
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=bucket_name)
        os.environ["S3_BUCKET_NAME"] = bucket_name
        yield s3


# ---------------------------------------------------------------------------
# Kafka mock fixtures (unchanged from v7)
# ---------------------------------------------------------------------------

class _FakeMessage:
    """Minimal stand-in for a confluent_kafka Message."""

    def __init__(
        self,
        value: dict[str, Any],
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        self._value = json.dumps(value).encode()
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes:
        return self._value

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> None:  # type: ignore[return]
        return None


class MockConsumer:
    """Deterministic finite consumer that exhausts after all sample messages."""

    def __init__(self, rows: list[dict[str, Any]], hwm: int = 0) -> None:
        self._messages = [_FakeMessage(r, offset=i) for i, r in enumerate(rows)]
        self._index = 0
        self._hwm = hwm or len(rows)

    def list_topics(self, *args: Any, **kwargs: Any) -> MagicMock:
        meta = MagicMock()
        meta.topics = {
            "football-matches": MagicMock(partitions={0: MagicMock()})
        }
        return meta

    def get_watermark_offsets(self, *args: Any, **kwargs: Any) -> tuple[int, int]:
        return (0, self._hwm)

    def assign(self, *args: Any, **kwargs: Any) -> None:
        pass

    def poll(self, timeout: float = 1.0) -> _FakeMessage | None:
        if self._index >= len(self._messages):
            return None
        msg = self._messages[self._index]
        self._index += 1
        return msg

    def commit(self, *args: Any, **kwargs: Any) -> None:
        pass

    def close(self) -> None:
        pass


@pytest.fixture
def mock_producer() -> MagicMock:
    return MagicMock()


@pytest.fixture
def pg_conn() -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: s
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn
