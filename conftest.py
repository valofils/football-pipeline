"""
tests/conftest.py — v13 fixtures.

Adds OpenLineage / Marquez mock fixtures on top of the v12 base.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest


# ===========================================================================
# v12 base fixtures (inline — no file import in fresh session)
# ===========================================================================

@pytest.fixture()
def sample_match() -> dict:
    return {
        "match_id": "m001",
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_score": 2,
        "away_score": 1,
        "match_date": "2024-03-15",
        "competition": "Premier League",
        "season": "2023-24",
        "stadium": "Emirates Stadium",
        "referee": "M. Oliver",
    }


@pytest.fixture()
def sample_matches(sample_match) -> list[dict]:
    base = sample_match.copy()
    second = {
        **base,
        "match_id": "m002",
        "home_team": "Liverpool",
        "away_team": "Manchester City",
        "home_score": 1,
        "away_score": 1,
    }
    return [base, second]


@pytest.fixture()
def kafka_producer_mock():
    with patch("kafka.KafkaProducer") as mock:
        producer = MagicMock()
        mock.return_value = producer
        yield producer


@pytest.fixture()
def postgres_conn_mock():
    with patch("psycopg2.connect") as mock:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: cursor
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock.return_value.__enter__ = lambda s: conn
        mock.return_value.__exit__ = MagicMock(return_value=False)
        yield conn, cursor


# ===========================================================================
# v13 OpenLineage / Marquez fixtures
# ===========================================================================

@pytest.fixture()
def mock_ol_client() -> Generator[MagicMock, None, None]:
    """Patch get_client() so no real HTTP calls are made during tests."""
    with patch("lineage.ol_client.get_client") as mock_get:
        client = MagicMock()
        mock_get.return_value = client
        yield client


@pytest.fixture()
def mock_emit_run_event(mock_ol_client) -> Generator[MagicMock, None, None]:
    """Patch emit_run_event at the module level for emitter tests."""
    with patch("lineage.ol_client.emit_run_event") as mock_emit:
        yield mock_emit


@pytest.fixture()
def marquez_health_response() -> dict:
    """Simulate a healthy Marquez /api/v1/namespaces response."""
    return {
        "namespaces": [
            {
                "name": "football_pipeline",
                "createdAt": "2024-01-01T00:00:00Z",
                "updatedAt": "2024-03-15T06:00:00Z",
                "ownerNames": ["football-pipeline-team"],
                "description": "Football data pipeline namespace",
            }
        ]
    }


@pytest.fixture()
def marquez_lineage_graph() -> dict:
    """
    Six-node lineage graph as Marquez would return it.

    Nodes: kafka/football_matches → delta/matches → postgres/public.matches
           → postgres/public.mart_team_season_stats
           → mlflow/football_outcome_predictor → delta/predictions
    """
    return {
        "graph": [
            {
                "id": "dataset:kafka://kafka:9092:football_matches",
                "type": "DATASET",
                "data": {"name": "football_matches", "namespace": "kafka://kafka:9092"},
                "inEdges": [],
                "outEdges": [{"origin": "job:football_pipeline:streaming_ingest"}],
            },
            {
                "id": "job:football_pipeline:streaming_ingest",
                "type": "JOB",
                "data": {"name": "streaming_ingest", "namespace": "football_pipeline"},
                "inEdges": [{"origin": "dataset:kafka://kafka:9092:football_matches"}],
                "outEdges": [{"origin": "dataset:s3://football-data:delta/matches"}],
            },
            {
                "id": "dataset:s3://football-data:delta/matches",
                "type": "DATASET",
                "data": {"name": "delta/matches", "namespace": "s3://football-data"},
                "inEdges": [{"origin": "job:football_pipeline:streaming_ingest"}],
                "outEdges": [
                    {"origin": "job:football_pipeline:validate_matches"},
                    {"origin": "job:football_pipeline:load_postgres"},
                ],
            },
            {
                "id": "dataset:postgresql://postgres:5432:public.matches",
                "type": "DATASET",
                "data": {"name": "public.matches", "namespace": "postgresql://postgres:5432"},
                "inEdges": [{"origin": "job:football_pipeline:load_postgres"}],
                "outEdges": [
                    {"origin": "job:football_pipeline:dbt_transform"},
                    {"origin": "job:football_pipeline:ml_train"},
                ],
            },
            {
                "id": "dataset:mlflow://mlflow:5000:football_outcome_predictor",
                "type": "DATASET",
                "data": {
                    "name": "football_outcome_predictor",
                    "namespace": "mlflow://mlflow:5000",
                },
                "inEdges": [{"origin": "job:football_pipeline:ml_train"}],
                "outEdges": [{"origin": "job:football_pipeline:batch_predict"}],
            },
            {
                "id": "dataset:s3://football-data:delta/predictions",
                "type": "DATASET",
                "data": {"name": "delta/predictions", "namespace": "s3://football-data"},
                "inEdges": [{"origin": "job:football_pipeline:batch_predict"}],
                "outEdges": [],
            },
        ]
    }


@pytest.fixture()
def mock_marquez_api(requests_mock, marquez_health_response, marquez_lineage_graph):
    """
    Registers requests-mock stubs for commonly used Marquez endpoints.

    Endpoints mocked:
      GET  /api/v1/namespaces
      POST /api/v1/lineage
      GET  /api/v1/lineage?nodeId=...
    """
    base = "http://localhost:5002"
    requests_mock.get(f"{base}/api/v1/namespaces", json=marquez_health_response)
    requests_mock.post(f"{base}/api/v1/lineage", status_code=201, json={})
    requests_mock.get(f"{base}/api/v1/lineage", json=marquez_lineage_graph)
    return requests_mock


@pytest.fixture()
def ol_run_event_payload() -> dict:
    """A minimal but valid OpenLineage RunEvent payload (dict form)."""
    run_id = str(uuid.uuid4())
    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {"runId": run_id},
        "job": {"namespace": "football_pipeline", "name": "streaming_ingest"},
        "inputs": [
            {
                "namespace": "kafka://kafka:9092",
                "name": "football_matches",
            }
        ],
        "outputs": [
            {
                "namespace": "s3://football-data",
                "name": "delta/matches",
            }
        ],
        "producer": "https://github.com/your-org/football-pipeline",
    }
