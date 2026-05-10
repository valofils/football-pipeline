"""
tests/conftest.py  —  v13
Adds OpenLineage + Marquez mock fixtures on top of v12 fixtures.
"""

from __future__ import annotations

import json
import uuid
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

# ============================================================
# v12 fixtures (prometheus / grafana)  — kept verbatim
# ============================================================

@pytest.fixture
def mock_prometheus_registry():
    with patch("prometheus_client.CollectorRegistry") as mock_registry:
        registry = MagicMock()
        mock_registry.return_value = registry
        yield registry


@pytest.fixture
def prometheus_alert_rules():
    return {
        "groups": [
            {
                "name": "kafka_lag",
                "rules": [
                    {
                        "alert": "KafkaConsumerLagHigh",
                        "expr": "kafka_consumergroup_lag > 1000",
                        "for": "5m",
                        "labels": {"severity": "warning"},
                    },
                    {
                        "alert": "KafkaConsumerLagCritical",
                        "expr": "kafka_consumergroup_lag > 5000",
                        "for": "2m",
                        "labels": {"severity": "critical"},
                    },
                ],
            },
            {
                "name": "airflow_slo",
                "rules": [
                    {
                        "alert": "AirflowDAGFailure",
                        "expr": "airflow_dag_run_failed_total > 0",
                        "for": "1m",
                        "labels": {"severity": "critical"},
                    }
                ],
            },
        ]
    }


@pytest.fixture
def prometheus_config():
    return {
        "global": {"scrape_interval": "15s", "evaluation_interval": "15s"},
        "scrape_configs": [
            {"job_name": "airflow", "static_configs": [{"targets": ["airflow-webserver:8080"]}]},
            {"job_name": "kafka", "static_configs": [{"targets": ["kafka-exporter:9308"]}]},
            {"job_name": "postgres", "static_configs": [{"targets": ["postgres-exporter:9187"]}]},
        ],
    }


@pytest.fixture
def grafana_dashboard():
    return {
        "title": "Football Pipeline",
        "uid": "football-pipeline",
        "panels": [
            {"title": "Kafka Consumer Lag", "type": "timeseries"},
            {"title": "Airflow DAG Duration", "type": "timeseries"},
            {"title": "MLflow Model Accuracy", "type": "stat"},
            {"title": "Postgres Connections", "type": "gauge"},
        ],
    }


@pytest.fixture
def mock_pushgateway():
    with patch("prometheus_client.push_to_gateway") as mock_push:
        mock_push.return_value = None
        yield mock_push


@pytest.fixture
def mock_grafana_api():
    with patch("requests.get") as mock_get, patch("requests.post") as mock_post:
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {"state": "ok"})
        mock_post.return_value = MagicMock(status_code=200, json=lambda: {"id": 1})
        yield {"get": mock_get, "post": mock_post}


@pytest.fixture
def sample_lag_series():
    import random
    return [{"timestamp": i * 15, "lag": random.randint(0, 2000)} for i in range(20)]


# ============================================================
# v13 fixtures — OpenLineage / Marquez
# ============================================================

@pytest.fixture
def sample_run_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def mock_ol_client():
    """Patch the OpenLineage client so no HTTP calls are made in tests."""
    with patch("lineage.ol_client._CLIENT", None):
        with patch("lineage.ol_client.OpenLineageClient") as MockClient:
            client_instance = MagicMock()
            MockClient.return_value = client_instance
            yield client_instance


@pytest.fixture
def mock_emit_run_event(mock_ol_client):
    """Convenience fixture: returns the mock emit call recorder."""
    return mock_ol_client.emit


@pytest.fixture
def marquez_health_response():
    return {"status": "healthy", "version": "0.47.0", "build": {"version": "0.47.0"}}


@pytest.fixture
def marquez_lineage_graph():
    """Minimal lineage graph payload returned by GET /api/v1/lineage."""
    return {
        "graph": [
            {
                "id": "job:football_pipeline:streaming_ingest",
                "type": "JOB",
                "data": {"namespace": "football_pipeline", "name": "streaming_ingest"},
                "inEdges": [{"origin": "dataset:kafka://kafka:9092:football_matches"}],
                "outEdges": [{"destination": "dataset:s3://football-data:delta/matches"}],
            },
            {
                "id": "job:football_pipeline:validate",
                "type": "JOB",
                "data": {"namespace": "football_pipeline", "name": "validate"},
                "inEdges": [{"origin": "dataset:s3://football-data:delta/matches"}],
                "outEdges": [{"destination": "dataset:s3://football-data:delta/matches"}],
            },
            {
                "id": "job:football_pipeline:load_postgres",
                "type": "JOB",
                "data": {"namespace": "football_pipeline", "name": "load_postgres"},
                "inEdges": [{"origin": "dataset:s3://football-data:delta/matches"}],
                "outEdges": [{"destination": "dataset:postgres://football-db:5432/football:public.matches"}],
            },
            {
                "id": "job:football_pipeline:dbt_run",
                "type": "JOB",
                "data": {"namespace": "football_pipeline", "name": "dbt_run"},
                "inEdges": [{"origin": "dataset:postgres://football-db:5432/football:public.matches"}],
                "outEdges": [{"destination": "dataset:postgres://football-db:5432/football:public.mart_team_season_stats"}],
            },
            {
                "id": "job:football_pipeline:ml_train",
                "type": "JOB",
                "data": {"namespace": "football_pipeline", "name": "ml_train"},
                "inEdges": [{"origin": "dataset:s3://football-data:delta/matches"}],
                "outEdges": [{"destination": "dataset:mlflow://mlflow:5000:football_outcome_predictor"}],
            },
            {
                "id": "job:football_pipeline:predict",
                "type": "JOB",
                "data": {"namespace": "football_pipeline", "name": "predict"},
                "inEdges": [
                    {"origin": "dataset:s3://football-data:delta/matches"},
                    {"origin": "dataset:mlflow://mlflow:5000:football_outcome_predictor"},
                ],
                "outEdges": [{"destination": "dataset:s3://football-data:delta/predictions"}],
            },
        ]
    }


@pytest.fixture
def mock_marquez_api(marquez_health_response, marquez_lineage_graph):
    """Mock requests against the Marquez REST API."""
    with patch("requests.get") as mock_get, patch("requests.post") as mock_post:
        def _get_side_effect(url, **kwargs):
            resp = MagicMock()
            if "healthcheck" in url:
                resp.status_code = 200
                resp.json.return_value = marquez_health_response
            elif "lineage" in url:
                resp.status_code = 200
                resp.json.return_value = marquez_lineage_graph
            elif "jobs" in url:
                resp.status_code = 200
                resp.json.return_value = {"jobs": []}
            elif "datasets" in url:
                resp.status_code = 200
                resp.json.return_value = {"datasets": []}
            else:
                resp.status_code = 404
            return resp

        mock_get.side_effect = _get_side_effect
        mock_post.return_value = MagicMock(status_code=201)
        yield {"get": mock_get, "post": mock_post}


@pytest.fixture
def ol_run_event_payload(sample_run_id):
    """Minimal OpenLineage RunEvent JSON payload for assertion helpers."""
    return {
        "eventType": "COMPLETE",
        "eventTime": "2024-01-01T12:00:00+00:00",
        "run": {"runId": sample_run_id},
        "job": {"namespace": "football_pipeline", "name": "streaming_ingest"},
        "inputs": [{"namespace": "kafka://kafka:9092", "name": "football_matches"}],
        "outputs": [{"namespace": "s3://football-data", "name": "delta/matches"}],
    }
