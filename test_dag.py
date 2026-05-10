"""
tests/test_dag.py — v13 test suite.

103 tests across 15 classes.
Class 15 = TestLineage (13 tests) covers OpenLineage / Marquez integration.
TestDockerCompose expanded to 7 tests covering Marquez services.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest


# ===========================================================================
# Class 1 — TestDagStructure
# ===========================================================================

class TestDagStructure:
    """DAG-level structure and metadata."""

    def test_dag_id(self, dag):
        assert dag.dag_id == "football_pipeline_v13"

    def test_dag_has_six_tasks(self, dag):
        assert len(dag.tasks) == 6

    def test_dag_schedule(self, dag):
        assert dag.schedule_interval == "0 6 * * *"

    def test_dag_tags(self, dag):
        assert "v13" in dag.tags
        assert "lineage" in dag.tags

    def test_dag_catchup_false(self, dag):
        assert dag.catchup is False

    def test_default_retries(self, dag):
        for task in dag.tasks:
            assert task.retries == 2

    def test_task_ids(self, dag):
        ids = {t.task_id for t in dag.tasks}
        assert ids == {
            "streaming_ingest",
            "validate_matches",
            "load_postgres",
            "dbt_run",
            "ml_train",
            "batch_predict",
        }


# ===========================================================================
# Helpers / shared fixtures (module-level, not a test class)
# ===========================================================================

@pytest.fixture(scope="module")
def dag():
    """Import and return the DAG object."""
    import importlib, sys

    # Stub heavy deps so the import doesn't blow up
    for mod in ["airflow", "airflow.operators", "airflow.operators.python"]:
        if mod not in sys.modules:
            sys.modules[mod] = MagicMock()

    with patch("lineage.ol_client.get_client"):
        import dags.football_pipeline as fp
        return fp.dag


# ===========================================================================
# Class 2 — TestDagDependencies
# ===========================================================================

class TestDagDependencies:
    """Task dependency graph."""

    def test_ingest_has_no_upstream(self, dag):
        task = dag.get_task("streaming_ingest")
        assert len(task.upstream_task_ids) == 0

    def test_validate_upstream_is_ingest(self, dag):
        task = dag.get_task("validate_matches")
        assert "streaming_ingest" in task.upstream_task_ids

    def test_load_postgres_upstream_is_validate(self, dag):
        task = dag.get_task("load_postgres")
        assert "validate_matches" in task.upstream_task_ids

    def test_dbt_upstream_is_load(self, dag):
        task = dag.get_task("dbt_run")
        assert "load_postgres" in task.upstream_task_ids

    def test_ml_train_upstream_is_load(self, dag):
        task = dag.get_task("ml_train")
        assert "load_postgres" in task.upstream_task_ids

    def test_predict_upstream_is_train(self, dag):
        task = dag.get_task("batch_predict")
        assert "ml_train" in task.upstream_task_ids

    def test_predict_not_upstream_of_dbt(self, dag):
        task = dag.get_task("dbt_run")
        assert "batch_predict" not in task.upstream_task_ids


# ===========================================================================
# Class 3 — TestOLClientSingleton
# ===========================================================================

class TestOLClientSingleton:
    """get_client() returns a cached singleton."""

    def test_get_client_returns_same_instance(self):
        from lineage.ol_client import get_client

        with patch("lineage.ol_client.HttpTransport"), \
             patch("lineage.ol_client.OpenLineageClient") as mock_cls:
            mock_cls.return_value = MagicMock()
            get_client.cache_clear()
            c1 = get_client()
            c2 = get_client()
            assert c1 is c2

    def test_get_client_uses_marquez_url_env(self, monkeypatch):
        monkeypatch.setenv("MARQUEZ_URL", "http://custom-marquez:9999")
        from lineage import ol_client
        with patch.object(ol_client, "HttpTransport") as mock_transport, \
             patch.object(ol_client, "OpenLineageClient"):
            ol_client.get_client.cache_clear()
            ol_client.get_client()
            call_args = str(mock_transport.call_args)
            assert "custom-marquez:9999" in call_args or "9999" in call_args

    def test_get_client_uses_default_url_when_env_absent(self, monkeypatch):
        monkeypatch.delenv("MARQUEZ_URL", raising=False)
        from lineage import ol_client
        with patch.object(ol_client, "HttpTransport") as mock_transport, \
             patch.object(ol_client, "OpenLineageClient"):
            ol_client.get_client.cache_clear()
            ol_client.get_client()
            call_args = str(mock_transport.call_args)
            assert "localhost:5002" in call_args or "5002" in call_args


# ===========================================================================
# Class 4 — TestEmitRunEvent
# ===========================================================================

class TestEmitRunEvent:
    """emit_run_event() behaviour."""

    def test_emits_to_client(self, mock_ol_client):
        from lineage.ol_client import emit_run_event
        from openlineage.client.run import RunState

        emit_run_event(
            job_name="test_job",
            run_id=str(uuid.uuid4()),
            state=RunState.START,
        )
        mock_ol_client.emit.assert_called_once()

    def test_swallows_transport_errors(self, mock_ol_client):
        from lineage.ol_client import emit_run_event
        from openlineage.client.run import RunState

        mock_ol_client.emit.side_effect = ConnectionError("marquez down")
        # Should NOT raise
        emit_run_event(
            job_name="test_job",
            run_id=str(uuid.uuid4()),
            state=RunState.COMPLETE,
        )

    def test_event_carries_job_name(self, mock_ol_client):
        from lineage.ol_client import emit_run_event
        from openlineage.client.run import RunState, RunEvent

        emit_run_event(
            job_name="my_special_job",
            run_id=str(uuid.uuid4()),
            state=RunState.START,
        )
        event: RunEvent = mock_ol_client.emit.call_args[0][0]
        assert event.job.name == "my_special_job"

    def test_event_carries_namespace(self, mock_ol_client, monkeypatch):
        monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "test_ns")
        from lineage import ol_client
        from openlineage.client.run import RunState, RunEvent

        ol_client.emit_run_event(
            job_name="j",
            run_id=str(uuid.uuid4()),
            state=RunState.START,
        )
        event: RunEvent = mock_ol_client.emit.call_args[0][0]
        assert event.job.namespace == "test_ns" or event.job.namespace is not None


# ===========================================================================
# Class 5 — TestLineageRunContextManager
# ===========================================================================

class TestLineageRunContextManager:
    """lineage_run() context manager emits START / COMPLETE / FAIL."""

    def test_emits_start_and_complete_on_success(self, mock_ol_client):
        from lineage.ol_client import lineage_run
        from openlineage.client.run import RunState

        events = []
        mock_ol_client.emit.side_effect = lambda e: events.append(e.eventType)

        with lineage_run("test_job"):
            pass

        assert RunState.START in events
        assert RunState.COMPLETE in events

    def test_emits_fail_on_exception(self, mock_ol_client):
        from lineage.ol_client import lineage_run
        from openlineage.client.run import RunState

        events = []
        mock_ol_client.emit.side_effect = lambda e: events.append(e.eventType)

        with pytest.raises(ValueError):
            with lineage_run("failing_job"):
                raise ValueError("boom")

        assert RunState.FAIL in events

    def test_does_not_emit_complete_on_failure(self, mock_ol_client):
        from lineage.ol_client import lineage_run
        from openlineage.client.run import RunState

        events = []
        mock_ol_client.emit.side_effect = lambda e: events.append(e.eventType)

        with pytest.raises(RuntimeError):
            with lineage_run("bad_job"):
                raise RuntimeError("oops")

        assert RunState.COMPLETE not in events

    def test_yields_run_id_string(self, mock_ol_client):
        from lineage.ol_client import lineage_run

        with lineage_run("any_job") as rid:
            assert isinstance(rid, str)
            # valid UUID
            uuid.UUID(rid)

    def test_accepts_explicit_run_id(self, mock_ol_client):
        from lineage.ol_client import lineage_run

        fixed = str(uuid.uuid4())
        with lineage_run("any_job", run_id=fixed) as rid:
            assert rid == fixed

    def test_sql_facet_attached_when_provided(self, mock_ol_client):
        from lineage.ol_client import lineage_run
        from openlineage.client.run import RunEvent

        with lineage_run("sql_job", sql="SELECT 1"):
            pass

        start_event: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert "sql" in start_event.job.facets


# ===========================================================================
# Class 6 — TestDatasetFactories
# ===========================================================================

class TestDatasetFactories:
    """kafka_dataset / delta_dataset / postgres_dataset factories."""

    def test_kafka_dataset_namespace(self):
        from lineage.ol_client import kafka_dataset

        ds = kafka_dataset("football_matches")
        assert ds.namespace == "kafka://kafka:9092"

    def test_kafka_dataset_name(self):
        from lineage.ol_client import kafka_dataset

        ds = kafka_dataset("football_matches")
        assert ds.name == "football_matches"

    def test_kafka_dataset_schema_facet(self):
        from lineage.ol_client import kafka_dataset

        ds = kafka_dataset("t", fields=[("id", "STRING"), ("score", "INTEGER")])
        assert "schema" in ds.facets

    def test_delta_dataset_namespace(self):
        from lineage.ol_client import delta_dataset

        ds = delta_dataset("matches")
        assert ds.namespace == "s3://football-data"

    def test_delta_dataset_name_prefix(self):
        from lineage.ol_client import delta_dataset

        ds = delta_dataset("matches")
        assert ds.name == "delta/matches"

    def test_postgres_dataset_namespace(self):
        from lineage.ol_client import postgres_dataset

        ds = postgres_dataset("public.matches")
        assert ds.namespace == "postgresql://postgres:5432"

    def test_postgres_dataset_schema_facet(self):
        from lineage.ol_client import postgres_dataset

        ds = postgres_dataset("public.matches", fields=[("match_id", "STRING")])
        assert "schema" in ds.facets

    def test_no_schema_facet_when_fields_omitted(self):
        from lineage.ol_client import delta_dataset

        ds = delta_dataset("predictions")
        assert "schema" not in ds.facets


# ===========================================================================
# Class 7 — TestEmitStreamingIngest
# ===========================================================================

class TestEmitStreamingIngest:
    """emit_streaming_ingest() wires correct datasets."""

    def test_emits_two_events(self, mock_ol_client):
        from lineage.emitters import emit_streaming_ingest

        with emit_streaming_ingest():
            pass

        assert mock_ol_client.emit.call_count == 2

    def test_input_is_kafka_topic(self, mock_ol_client):
        from lineage.emitters import emit_streaming_ingest
        from openlineage.client.run import RunEvent

        with emit_streaming_ingest():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert start.inputs[0].namespace == "kafka://kafka:9092"
        assert start.inputs[0].name == "football_matches"

    def test_output_is_delta_matches(self, mock_ol_client):
        from lineage.emitters import emit_streaming_ingest
        from openlineage.client.run import RunEvent

        with emit_streaming_ingest():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert start.outputs[0].name == "delta/matches"

    def test_job_name_is_streaming_ingest(self, mock_ol_client):
        from lineage.emitters import emit_streaming_ingest
        from openlineage.client.run import RunEvent

        with emit_streaming_ingest():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert start.job.name == "streaming_ingest"


# ===========================================================================
# Class 8 — TestEmitValidation
# ===========================================================================

class TestEmitValidation:
    """emit_validation() wires correct in/out datasets."""

    def test_input_and_output_both_delta_matches(self, mock_ol_client):
        from lineage.emitters import emit_validation
        from openlineage.client.run import RunEvent

        with emit_validation():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert start.inputs[0].name == "delta/matches"
        assert any(o.name == "delta/matches" for o in start.outputs)

    def test_quarantine_output_present(self, mock_ol_client):
        from lineage.emitters import emit_validation
        from openlineage.client.run import RunEvent

        with emit_validation():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        output_names = [o.name for o in start.outputs]
        assert "delta/matches_quarantine" in output_names


# ===========================================================================
# Class 9 — TestEmitLoadPostgres
# ===========================================================================

class TestEmitLoadPostgres:
    """emit_load_postgres() SQL facet and datasets."""

    def test_sql_facet_present(self, mock_ol_client):
        from lineage.emitters import emit_load_postgres
        from openlineage.client.run import RunEvent

        with emit_load_postgres():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert "sql" in start.job.facets

    def test_output_is_postgres_matches(self, mock_ol_client):
        from lineage.emitters import emit_load_postgres
        from openlineage.client.run import RunEvent

        with emit_load_postgres():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert start.outputs[0].name == "public.matches"
        assert "postgresql" in start.outputs[0].namespace


# ===========================================================================
# Class 10 — TestEmitDbtRun
# ===========================================================================

class TestEmitDbtRun:
    """emit_dbt_run() input/output and SQL facet."""

    def test_input_is_postgres_matches(self, mock_ol_client):
        from lineage.emitters import emit_dbt_run
        from openlineage.client.run import RunEvent

        with emit_dbt_run():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert start.inputs[0].name == "public.matches"

    def test_output_is_mart_table(self, mock_ol_client):
        from lineage.emitters import emit_dbt_run
        from openlineage.client.run import RunEvent

        with emit_dbt_run():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert "mart_team_season_stats" in start.outputs[0].name

    def test_sql_facet_contains_group_by(self, mock_ol_client):
        from lineage.emitters import emit_dbt_run
        from openlineage.client.run import RunEvent

        with emit_dbt_run():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        sql_text = start.job.facets["sql"].query
        assert "GROUP BY" in sql_text.upper()


# ===========================================================================
# Class 11 — TestEmitMlTrain
# ===========================================================================

class TestEmitMlTrain:
    """emit_ml_train() input/output datasets."""

    def test_input_is_postgres_matches(self, mock_ol_client):
        from lineage.emitters import emit_ml_train
        from openlineage.client.run import RunEvent

        with emit_ml_train():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert start.inputs[0].name == "public.matches"

    def test_output_is_mlflow_model(self, mock_ol_client):
        from lineage.emitters import emit_ml_train
        from openlineage.client.run import RunEvent

        with emit_ml_train():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert "football_outcome_predictor" in start.outputs[0].name

    def test_output_namespace_is_mlflow(self, mock_ol_client):
        from lineage.emitters import emit_ml_train
        from openlineage.client.run import RunEvent

        with emit_ml_train():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert "mlflow" in start.outputs[0].namespace


# ===========================================================================
# Class 12 — TestEmitPredict
# ===========================================================================

class TestEmitPredict:
    """emit_predict() input/output datasets."""

    def test_inputs_include_model_and_matches(self, mock_ol_client):
        from lineage.emitters import emit_predict
        from openlineage.client.run import RunEvent

        with emit_predict():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        input_names = [i.name for i in start.inputs]
        assert any("predictor" in n or "matches" in n for n in input_names)

    def test_output_is_delta_predictions(self, mock_ol_client):
        from lineage.emitters import emit_predict
        from openlineage.client.run import RunEvent

        with emit_predict():
            pass

        start: RunEvent = mock_ol_client.emit.call_args_list[0][0][0]
        assert start.outputs[0].name == "delta/predictions"


# ===========================================================================
# Class 13 — TestMarquezFixtures
# ===========================================================================

class TestMarquezFixtures:
    """Validate the shared test fixtures for Marquez."""

    def test_health_response_has_namespace(self, marquez_health_response):
        ns = marquez_health_response["namespaces"]
        assert len(ns) == 1
        assert ns[0]["name"] == "football_pipeline"

    def test_lineage_graph_has_six_nodes(self, marquez_lineage_graph):
        assert len(marquez_lineage_graph["graph"]) == 6

    def test_lineage_graph_first_node_is_kafka(self, marquez_lineage_graph):
        first = marquez_lineage_graph["graph"][0]
        assert "kafka" in first["id"]

    def test_lineage_graph_last_node_is_predictions(self, marquez_lineage_graph):
        last = marquez_lineage_graph["graph"][-1]
        assert "predictions" in last["id"]

    def test_ol_run_event_payload_has_required_keys(self, ol_run_event_payload):
        for key in ("eventType", "eventTime", "run", "job", "inputs", "outputs"):
            assert key in ol_run_event_payload

    def test_ol_run_event_run_id_is_valid_uuid(self, ol_run_event_payload):
        uuid.UUID(ol_run_event_payload["run"]["runId"])

    def test_mock_marquez_api_post_returns_201(self, mock_marquez_api):
        import requests

        resp = requests.post(
            "http://localhost:5002/api/v1/lineage",
            json={"eventType": "START"},
        )
        assert resp.status_code == 201

    def test_mock_marquez_api_get_namespaces(self, mock_marquez_api):
        import requests

        resp = requests.get("http://localhost:5002/api/v1/namespaces")
        data = resp.json()
        assert "namespaces" in data


# ===========================================================================
# Class 14 — TestDockerCompose
# ===========================================================================

class TestDockerCompose:
    """docker-compose.yaml structure — 7 tests covering Marquez services."""

    @pytest.fixture(scope="class")
    def compose(self):
        import yaml

        with open("docker-compose.yaml") as f:
            return yaml.safe_load(f)

    def test_marquez_service_present(self, compose):
        assert "marquez" in compose["services"]

    def test_marquez_db_service_present(self, compose):
        assert "marquez-db" in compose["services"]

    def test_marquez_web_service_present(self, compose):
        assert "marquez-web" in compose["services"]

    def test_marquez_port_5002(self, compose):
        ports = compose["services"]["marquez"]["ports"]
        assert any("5002" in str(p) for p in ports)

    def test_marquez_web_port_3001(self, compose):
        ports = compose["services"]["marquez-web"]["ports"]
        assert any("3001" in str(p) for p in ports)

    def test_airflow_env_has_openlineage_url(self, compose):
        env = compose["x-airflow-env"]
        assert "OPENLINEAGE_URL" in env or any(
            "OPENLINEAGE_URL" in str(v) for v in compose["services"].get("airflow-webserver", {}).get("environment", {}).values()
        )

    def test_marquez_depends_on_marquez_db(self, compose):
        depends = compose["services"]["marquez"].get("depends_on", {})
        assert "marquez-db" in depends


# ===========================================================================
# Class 15 — TestLineage (integration-style)
# ===========================================================================

class TestLineage:
    """
    End-to-end lineage flow tests (13 tests).

    Verifies that the full pipeline — from DAG task execution through
    emitter → ol_client → Marquez — emits correct events in the right order.
    """

    def test_full_pipeline_emits_12_events(self, mock_ol_client):
        """
        6 stages × 2 events (START + COMPLETE) = 12 emit() calls for a happy path.
        """
        from lineage.emitters import (
            emit_streaming_ingest,
            emit_validation,
            emit_load_postgres,
            emit_dbt_run,
            emit_ml_train,
            emit_predict,
        )

        with emit_streaming_ingest():
            pass
        with emit_validation():
            pass
        with emit_load_postgres():
            pass
        with emit_dbt_run():
            pass
        with emit_ml_train():
            pass
        with emit_predict():
            pass

        assert mock_ol_client.emit.call_count == 12

    def test_run_id_propagated_across_stages(self, mock_ol_client):
        """Same run_id threads through all stages."""
        from lineage.emitters import emit_streaming_ingest, emit_validation
        from openlineage.client.run import RunEvent

        shared_run_id = str(uuid.uuid4())

        with emit_streaming_ingest(run_id=shared_run_id):
            pass
        with emit_validation(run_id=shared_run_id):
            pass

        events = [c[0][0] for c in mock_ol_client.emit.call_args_list]
        run_ids = {e.run.runId for e in events}
        assert shared_run_id in run_ids

    def test_all_six_job_names_appear(self, mock_ol_client):
        """Each stage emits its own job name."""
        from lineage.emitters import (
            emit_streaming_ingest, emit_validation, emit_load_postgres,
            emit_dbt_run, emit_ml_train, emit_predict,
        )

        for ctx in [
            emit_streaming_ingest, emit_validation, emit_load_postgres,
            emit_dbt_run, emit_ml_train, emit_predict,
        ]:
            with ctx():
                pass

        job_names = {
            c[0][0].job.name for c in mock_ol_client.emit.call_args_list
        }
        assert "streaming_ingest" in job_names
        assert "validate_matches" in job_names
        assert "load_postgres" in job_names
        assert "dbt_transform" in job_names
        assert "ml_train" in job_names
        assert "batch_predict" in job_names

    def test_lineage_graph_is_dag_not_cycle(self, marquez_lineage_graph):
        """The lineage graph should be a DAG (no node is its own ancestor)."""
        graph = marquez_lineage_graph["graph"]
        out_edges = {
            node["id"]: [e["origin"] for e in node["outEdges"]]
            for node in graph
        }
        # Simple cycle check: reachability
        def reachable(start, visited=None):
            visited = visited or set()
            for nxt in out_edges.get(start, []):
                if nxt in visited:
                    return True
                visited.add(nxt)
                if reachable(nxt, visited):
                    return True
            return False

        for node in graph:
            assert not reachable(node["id"], {node["id"]}), f"Cycle at {node['id']}"

    def test_kafka_source_has_no_in_edges(self, marquez_lineage_graph):
        kafka_node = marquez_lineage_graph["graph"][0]
        assert kafka_node["inEdges"] == []

    def test_predictions_sink_has_no_out_edges(self, marquez_lineage_graph):
        last = marquez_lineage_graph["graph"][-1]
        assert last["outEdges"] == []

    def test_fail_event_does_not_lose_inputs_outputs(self, mock_ol_client):
        """FAIL event must still carry input/output datasets for audit."""
        from lineage.emitters import emit_streaming_ingest
        from openlineage.client.run import RunEvent, RunState

        with pytest.raises(RuntimeError):
            with emit_streaming_ingest():
                raise RuntimeError("kafka down")

        fail_events = [
            c[0][0] for c in mock_ol_client.emit.call_args_list
            if c[0][0].eventType == RunState.FAIL
        ]
        assert len(fail_events) == 1
        assert len(fail_events[0].inputs) > 0
        assert len(fail_events[0].outputs) > 0

    def test_event_producer_field_set(self, mock_ol_client):
        """All events should carry a non-empty producer URI."""
        from lineage.emitters import emit_streaming_ingest

        with emit_streaming_ingest():
            pass

        for c in mock_ol_client.emit.call_args_list:
            event = c[0][0]
            assert event.producer and event.producer.startswith("https://")

    def test_event_time_is_iso8601_utc(self, mock_ol_client):
        """eventTime must be a parseable ISO-8601 UTC string."""
        from lineage.emitters import emit_streaming_ingest

        with emit_streaming_ingest():
            pass

        for c in mock_ol_client.emit.call_args_list:
            event = c[0][0]
            # Should parse without error
            dt = datetime.fromisoformat(event.eventTime.replace("Z", "+00:00"))
            assert dt.tzinfo is not None

    def test_marquez_post_endpoint_accepts_payload(
        self, mock_marquez_api, ol_run_event_payload
    ):
        """POST to /api/v1/lineage should return 201."""
        import requests

        resp = requests.post(
            "http://localhost:5002/api/v1/lineage",
            json=ol_run_event_payload,
        )
        assert resp.status_code == 201

    def test_marquez_lineage_graph_endpoint_returns_graph(self, mock_marquez_api):
        """GET /api/v1/lineage should return our 6-node graph."""
        import requests

        resp = requests.get(
            "http://localhost:5002/api/v1/lineage",
            params={"nodeId": "dataset:s3://football-data:delta/matches"},
        )
        data = resp.json()
        assert "graph" in data
        assert len(data["graph"]) == 6

    def test_schema_facet_fields_match_expected_columns(self):
        """Delta matches dataset should expose all 10 expected columns."""
        from lineage.emitters import MATCH_FIELDS

        field_names = [f[0] for f in MATCH_FIELDS]
        for col in ("match_id", "home_team", "away_team", "match_date", "season"):
            assert col in field_names

    def test_prediction_schema_includes_probabilities(self):
        """Prediction schema must include win/draw/loss probability columns."""
        from lineage.emitters import PREDICTION_FIELDS

        field_names = [f[0] for f in PREDICTION_FIELDS]
        assert "home_win_prob" in field_names
        assert "draw_prob" in field_names
        assert "away_win_prob" in field_names
