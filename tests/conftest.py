"""
conftest.py
-----------
Shared pytest fixtures for football-pipeline-v6.

Fixtures:
    spark           — session-scoped SparkSession (local[2], no UI)
    mock_producer   — function-scoped MagicMock replacing confluent_kafka.Producer
    mock_consumer   — function-scoped MockConsumer (deterministic message replay)
    pg_conn         — function-scoped psycopg2 connection to a test DB
    sample_rows     — list[dict] of match dicts mirroring Kafka message payloads
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .appName("test-football-v6")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_rows() -> list[dict]:
    """Ten match rows as they would arrive from Kafka (already parsed JSON)."""
    return [
        {
            "match_id": str(i),
            "season": "2023-24",
            "home_team": f"Team{i}",
            "away_team": f"Team{i + 1}",
            "home_goals": str(i % 4),
            "away_goals": str((i + 1) % 3),
            "match_date": "2024-01-15",
            "referee": f"Ref{i}",
        }
        for i in range(1, 11)
    ]


# ---------------------------------------------------------------------------
# Kafka mocks
# ---------------------------------------------------------------------------

class _FakeMessage:
    """Minimal Kafka message stub — mirrors the confluent_kafka Message API."""

    def __init__(self, key: str, value: dict, partition: int = 0, offset: int = 0, error=None):
        self._key = key.encode()
        self._value = json.dumps(value).encode()
        self._partition = partition
        self._offset = offset
        self._error = error
        self._topic = "match-events"

    def key(self):       return self._key
    def value(self):     return self._value
    def partition(self): return self._partition
    def offset(self):    return self._offset
    def error(self):     return self._error
    def topic(self):     return self._topic


class MockConsumer:
    """
    Deterministic Kafka consumer replay for tests.

    Feed it a list of dicts; each poll() returns one message until exhausted,
    then returns None indefinitely — simulating the end-of-topic condition.
    """

    def __init__(self, messages: list[dict]):
        self._messages = [
            _FakeMessage(
                key=f"2023-24:{m['match_id']}",
                value=m,
                partition=0,
                offset=i,
            )
            for i, m in enumerate(messages)
        ]
        self._index = 0
        self._committed: list[Any] = []
        self._subscribed: list[str] = []

    def subscribe(self, topics):
        self._subscribed = topics

    def poll(self, timeout=1.0):
        if self._index < len(self._messages):
            msg = self._messages[self._index]
            self._index += 1
            return msg
        return None

    def commit(self, asynchronous=True):
        self._committed.append(self._index)

    def assignment(self):
        from confluent_kafka import TopicPartition
        return [TopicPartition("match-events", 0)]

    def get_watermark_offsets(self, tp, timeout=10):
        return (0, len(self._messages))

    def list_topics(self, topic=None, timeout=10):
        # Return a minimal metadata stub
        meta = MagicMock()
        meta.topics = {
            "match-events": MagicMock(
                partitions={0: MagicMock()}
            )
        }
        return meta

    def close(self):
        pass


@pytest.fixture()
def mock_producer():
    """MagicMock replacing confluent_kafka.Producer."""
    producer = MagicMock()
    producer.flush.return_value = 0   # 0 un-delivered messages = success
    return producer


@pytest.fixture()
def mock_consumer(sample_rows):
    """MockConsumer pre-loaded with sample_rows."""
    return MockConsumer(sample_rows)


# ---------------------------------------------------------------------------
# Test Postgres (requires a running football-db or a local test instance)
# ---------------------------------------------------------------------------

def _test_db_url() -> dict:
    import os
    return {
        "host":     os.getenv("TEST_DB_HOST",     "localhost"),
        "port":     int(os.getenv("TEST_DB_PORT", "5433")),
        "dbname":   os.getenv("TEST_DB_NAME",     "football"),
        "user":     os.getenv("TEST_DB_USER",     "airflow"),
        "password": os.getenv("TEST_DB_PASSWORD", "airflow"),
    }


@pytest.fixture()
def pg_conn():
    """
    psycopg2 connection to the test database.
    Rolls back all changes after each test (no persistent side-effects).
    Skipped automatically if the test DB is not reachable.
    """
    try:
        conn = psycopg2.connect(**_test_db_url(), connect_timeout=3)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Test DB not reachable: {exc}")
        return

    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()
