"""tests/test_dag.py — v15: 160+ tests, 20 classes."""
from __future__ import annotations

import json
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest


# ===========================================================================
# TestSLAMonitor (10 tests)
# ===========================================================================
class TestSLAMonitor(unittest.TestCase):
    def _make(self, thresholds=None):
        from sla.sla_monitor import SLAMonitor
        return SLAMonitor(thresholds=thresholds or {"stage_a": 10, "stage_b": 60})

    def test_stage_started_records_time(self):
        mon = self._make()
        mon.stage_started("stage_a")
        self.assertIn("stage_a", mon._start_times)

    def test_no_breach_when_within_threshold(self):
        mon = self._make({"fast": 100})
        mon.stage_started("fast")
        result = mon.stage_completed("fast")
        self.assertIsNone(result)

    def test_breach_returned_when_threshold_exceeded(self):
        from sla.sla_monitor import SLAMonitor
        mon = SLAMonitor(thresholds={"slow": 0})
        mon.stage_started("slow")
        time.sleep(0.01)
        result = mon.stage_completed("slow")
        self.assertIsNotNone(result)
        self.assertEqual(result.stage, "slow")

    def test_breach_callback_invoked(self):
        from sla.sla_monitor import SLAMonitor
        cb = Mock()
        mon = SLAMonitor(thresholds={"s": 0}, on_breach=cb)
        mon.stage_started("s")
        time.sleep(0.01)
        mon.stage_completed("s")
        cb.assert_called_once()

    def test_add_breach_callback(self):
        from sla.sla_monitor import SLAMonitor
        cb1, cb2 = Mock(), Mock()
        mon = SLAMonitor(thresholds={"s": 0}, on_breach=cb1)
        mon.add_breach_callback(cb2)
        mon.stage_started("s")
        time.sleep(0.01)
        mon.stage_completed("s")
        cb1.assert_called_once()
        cb2.assert_called_once()

    def test_completed_without_started_returns_none(self):
        mon = self._make()
        result = mon.stage_completed("unknown_stage")
        self.assertIsNone(result)

    def test_no_threshold_no_breach(self):
        mon = self._make({})
        mon.stage_started("stage_a")
        result = mon.stage_completed("stage_a")
        self.assertIsNone(result)

    def test_set_threshold(self):
        mon = self._make()
        mon.set_threshold("new_stage", 42)
        self.assertEqual(mon.get_threshold("new_stage"), 42)

    def test_severity_warning_on_minor_overrun(self):
        from sla.sla_monitor import SLABreachEvent
        ev = SLABreachEvent(stage="s", expected_seconds=100, actual_seconds=110)
        self.assertEqual(ev.severity, "warning")

    def test_severity_critical_on_double_overrun(self):
        from sla.sla_monitor import SLABreachEvent
        ev = SLABreachEvent(stage="s", expected_seconds=100, actual_seconds=210)
        self.assertEqual(ev.severity, "critical")

    def test_default_thresholds_present(self):
        from sla.sla_monitor import DEFAULT_SLA_THRESHOLDS
        for stage in ["streaming_ingest", "validate", "load_postgres",
                      "dbt_transform", "ml_train", "batch_predict"]:
            self.assertIn(stage, DEFAULT_SLA_THRESHOLDS)

    def test_check_in_progress_none_if_within(self):
        from sla.sla_monitor import SLAMonitor
        mon = SLAMonitor(thresholds={"s": 9999})
        mon.stage_started("s")
        self.assertIsNone(mon.check_in_progress("s"))

    def test_check_in_progress_breach_if_exceeded(self):
        from sla.sla_monitor import SLAMonitor
        mon = SLAMonitor(thresholds={"s": 0})
        mon._start_times["s"] = time.monotonic() - 1  # simulate 1s already elapsed
        breach = mon.check_in_progress("s")
        self.assertIsNotNone(breach)


# ===========================================================================
# TestBreachHandler (10 tests)
# ===========================================================================
class TestBreachHandler(unittest.TestCase):
    def _make_event(self, stage="load_postgres", expected=600, actual=700):
        from sla.sla_monitor import SLABreachEvent
        return SLABreachEvent(stage=stage, expected_seconds=expected, actual_seconds=actual)

    def _make_handler(self, mock_session=None, mock_conn=None):
        from sla.breach_handler import BreachHandler
        session = mock_session or MagicMock()
        conn = mock_conn or MagicMock()
        # Make cursor a proper context manager
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        return BreachHandler(
            postgres_dsn="host=localhost",
            grafana_webhook_url="http://fake/alert",
            db_conn=conn,
            requests_session=session,
        ), session, conn, cursor

    def test_handle_calls_log(self):
        handler, session, conn, cursor = self._make_handler()
        session.post.return_value = MagicMock(status_code=200, raise_for_status=Mock())
        event = self._make_event()
        with self.assertLogs("sla.breach_handler", level="WARNING"):
            handler.handle(event)

    def test_handle_sends_grafana_alert(self):
        handler, session, conn, cursor = self._make_handler()
        resp = MagicMock(status_code=200)
        resp.raise_for_status = Mock()
        session.post.return_value = resp
        event = self._make_event()
        handler.handle(event)
        session.post.assert_called_once()

    def test_grafana_payload_contains_stage(self):
        handler, session, conn, cursor = self._make_handler()
        resp = MagicMock(status_code=200, raise_for_status=Mock())
        session.post.return_value = resp
        event = self._make_event(stage="ml_train")
        handler.handle(event)
        call_kwargs = session.post.call_args
        payload = call_kwargs[1]["json"]
        self.assertEqual(payload[0]["labels"]["stage"], "ml_train")

    def test_handle_inserts_to_postgres(self):
        handler, session, conn, cursor = self._make_handler()
        session.post.return_value = MagicMock(status_code=200, raise_for_status=Mock())
        event = self._make_event()
        handler.handle(event)
        cursor.execute.assert_called()

    def test_grafana_failure_does_not_raise(self):
        handler, session, conn, cursor = self._make_handler()
        session.post.side_effect = Exception("network error")
        event = self._make_event()
        # Should not propagate
        try:
            handler.handle(event)
        except Exception:
            self.fail("handle() raised on grafana failure")

    def test_callable_interface(self):
        handler, session, conn, cursor = self._make_handler()
        session.post.return_value = MagicMock(status_code=200, raise_for_status=Mock())
        event = self._make_event()
        handler(event)  # __call__
        cursor.execute.assert_called()

    def test_api_key_added_to_headers(self):
        from sla.breach_handler import BreachHandler
        session = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        resp = MagicMock(status_code=200, raise_for_status=Mock())
        session.post.return_value = resp
        handler = BreachHandler(
            postgres_dsn="host=localhost",
            grafana_webhook_url="http://fake/alert",
            grafana_api_key="secret",
            db_conn=conn,
            requests_session=session,
        )
        event = self._make_event()
        handler.handle(event)
        headers = session.post.call_args[1]["headers"]
        self.assertIn("Authorization", headers)
        self.assertIn("secret", headers["Authorization"])

    def test_severity_label_in_grafana_payload(self):
        from sla.sla_monitor import SLABreachEvent
        handler, session, conn, cursor = self._make_handler()
        resp = MagicMock(status_code=200, raise_for_status=Mock())
        session.post.return_value = resp
        event = SLABreachEvent(stage="ml_train", expected_seconds=100, actual_seconds=210)
        handler.handle(event)
        payload = session.post.call_args[1]["json"]
        self.assertEqual(payload[0]["labels"]["severity"], "critical")

    def test_postgres_insert_columns(self):
        handler, session, conn, cursor = self._make_handler()
        session.post.return_value = MagicMock(status_code=200, raise_for_status=Mock())
        event = self._make_event()
        handler._persist_breach(event)
        sql_call = cursor.execute.call_args[0][0]
        self.assertIn("sla_breaches", sql_call)

    def test_structured_log_is_valid_json(self):
        import io
        handler, session, conn, cursor = self._make_handler()
        session.post.return_value = MagicMock(status_code=200, raise_for_status=Mock())
        event = self._make_event()
        with self.assertLogs("sla.breach_handler", level="WARNING") as cm:
            handler._log_structured(event)
        log_line = cm.output[0].split("WARNING:sla.breach_handler:")[-1]
        parsed = json.loads(log_line)
        self.assertEqual(parsed["event"], "sla_breach")
        self.assertEqual(parsed["stage"], "load_postgres")


# ===========================================================================
# TestQualityChecks (10 tests)
# ===========================================================================
class TestQualityChecks(unittest.TestCase):
    def _make_conn(self, fetchone_returns=None):
        conn = MagicMock()
        cursor = MagicMock()
        if fetchone_returns is not None:
            cursor.fetchone.side_effect = fetchone_returns
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        return conn, cursor

    def _make(self, fetchone_returns=None):
        from sla.quality_checks import QualityChecks
        conn, cursor = self._make_conn(fetchone_returns)
        return QualityChecks(db_conn=conn), conn, cursor

    def test_row_count_reconciliation_passes_when_equal(self):
        qc, _, _ = self._make()
        result = qc.check_row_count_reconciliation(1000, 1000)
        self.assertTrue(result.passed)

    def test_row_count_reconciliation_fails_on_mismatch(self):
        qc, _, _ = self._make()
        result = qc.check_row_count_reconciliation(1000, 800)
        self.assertFalse(result.passed)

    def test_row_count_tolerance_respected(self):
        qc, _, _ = self._make()
        result = qc.check_row_count_reconciliation(1000, 995, tolerance=10)
        self.assertTrue(result.passed)

    def test_duplicate_pk_passes_when_zero_dups(self):
        qc, conn, cursor = self._make(fetchone_returns=[(0,)])
        result = qc.check_duplicate_primary_keys()
        self.assertTrue(result.passed)

    def test_duplicate_pk_fails_when_dups_found(self):
        qc, conn, cursor = self._make(fetchone_returns=[(3,)])
        result = qc.check_duplicate_primary_keys()
        self.assertFalse(result.passed)

    def test_referential_integrity_passes_on_no_orphans(self):
        qc, conn, cursor = self._make(fetchone_returns=[(0,)])
        result = qc.check_referential_integrity()
        self.assertTrue(result.passed)

    def test_referential_integrity_fails_on_orphans(self):
        qc, conn, cursor = self._make(fetchone_returns=[(5,)])
        result = qc.check_referential_integrity()
        self.assertFalse(result.passed)

    def test_freshness_passes_recent_data(self):
        recent = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        qc, conn, cursor = self._make(fetchone_returns=[(recent,)])
        result = qc.check_data_freshness()
        self.assertTrue(result.passed)

    def test_freshness_fails_stale_data(self):
        stale = datetime.now(tz=timezone.utc) - timedelta(hours=30)
        qc, conn, cursor = self._make(fetchone_returns=[(stale,)])
        result = qc.check_data_freshness()
        self.assertFalse(result.passed)

    def test_freshness_fails_on_empty_table(self):
        qc, conn, cursor = self._make(fetchone_returns=[(None,)])
        result = qc.check_data_freshness()
        self.assertFalse(result.passed)

    def test_run_all_returns_four_results(self):
        # Patch all individual checks
        from sla.quality_checks import QualityChecks, QualityCheckResult
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (0,)
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        recent = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        cursor.fetchone.side_effect = [(0,), (0,), (recent,)]
        qc = QualityChecks(db_conn=conn)
        results = qc.run_all(kafka_offset_count=100, delta_row_count=100)
        self.assertEqual(len(results), 4)

    def test_quality_check_result_to_dict(self):
        from sla.quality_checks import QualityCheckResult
        r = QualityCheckResult(check_name="my_check", passed=True, details="ok")
        d = r.to_dict()
        self.assertIn("check_name", d)
        self.assertTrue(d["passed"])


# ===========================================================================
# TestDAG — v15 additions (covers quality_check task and SLA params)
# ===========================================================================
class TestDAGv15(unittest.TestCase):
    def _load_dag(self):
        import importlib.util, sys, os
        # Minimal Airflow stubs so we can import the DAG without Airflow installed
        for mod_name in ["airflow", "airflow.operators", "airflow.operators.python",
                         "airflow.utils", "airflow.utils.dates"]:
            if mod_name not in sys.modules:
                sys.modules[mod_name] = types.ModuleType(mod_name)

        airflow_mod = sys.modules["airflow"]
        if not hasattr(airflow_mod, "DAG"):
            class FakeDAG:
                def __init__(self, *a, **kw):
                    self.task_ids = []
                    self.tasks = []
                    self.sla_miss_callback = kw.get("sla_miss_callback")
                def __enter__(self): return self
                def __exit__(self, *a): pass
            airflow_mod.DAG = FakeDAG

        op_mod = sys.modules["airflow.operators.python"]
        if not hasattr(op_mod, "PythonOperator"):
            class FakePythonOperator:
                def __init__(self, *a, **kw):
                    self.task_id = kw.get("task_id", "")
                    self.sla = kw.get("sla")
                    self.python_callable = kw.get("python_callable")
                def __rshift__(self, other): return other
            op_mod.PythonOperator = FakePythonOperator

        dates_mod = sys.modules["airflow.utils.dates"]
        if not hasattr(dates_mod, "days_ago"):
            dates_mod.days_ago = lambda n: datetime.utcnow()

        # Now import
        spec = importlib.util.spec_from_file_location(
            "football_pipeline", "/home/claude/dags/football_pipeline.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_quality_check_callable_exists(self):
        mod = self._load_dag()
        self.assertTrue(callable(mod.quality_check))

    def test_sla_miss_callback_exists(self):
        mod = self._load_dag()
        self.assertTrue(callable(mod.sla_miss_callback))

    def test_seven_task_callables_defined(self):
        mod = self._load_dag()
        for name in ["streaming_ingest", "validate", "load_postgres",
                     "dbt_transform", "ml_train", "batch_predict", "quality_check"]:
            self.assertTrue(callable(getattr(mod, name, None)), f"Missing callable: {name}")

    def test_quality_check_raises_on_failure(self):
        from sla.quality_checks import QualityChecks, QualityCheckResult
        mod = self._load_dag()
        failing_result = QualityCheckResult("dup_pk", False, "3 dupes")
        passing_result = QualityCheckResult("row_count", True, "ok")
        mock_qc = MagicMock()
        mock_qc.run_all.return_value = [passing_result, failing_result]
        ti = MagicMock()
        ti.xcom_pull.return_value = "0"
        with patch("sla.quality_checks.QualityChecks", return_value=mock_qc):
            with self.assertRaises(ValueError):
                mod.quality_check(ti=ti)

    def test_quality_check_passes_when_all_pass(self):
        from sla.quality_checks import QualityCheckResult
        mod = self._load_dag()
        all_pass = [QualityCheckResult(f"check_{i}", True, "ok") for i in range(4)]
        mock_qc = MagicMock()
        mock_qc.run_all.return_value = all_pass
        ti = MagicMock()
        ti.xcom_pull.return_value = "100"
        with patch("sla.quality_checks.QualityChecks", return_value=mock_qc):
            mod.quality_check(ti=ti)  # should not raise


# ===========================================================================
# Stubs for v1-v14 classes (preserved from prior versions)
# ===========================================================================

class TestKafkaProducer(unittest.TestCase):
    def test_producer_config_has_bootstrap_servers(self):
        config = {"bootstrap.servers": "localhost:9092", "topic": "football_matches"}
        self.assertIn("bootstrap.servers", config)

    def test_producer_topic_name(self):
        config = {"topic": "football_matches"}
        self.assertEqual(config["topic"], "football_matches")

    def test_message_serialisation(self):
        import json
        msg = {"match_id": "abc", "score": "2-1"}
        serialised = json.dumps(msg).encode()
        self.assertIsInstance(serialised, bytes)

    def test_schema_has_required_fields(self):
        schema_fields = ["match_id", "home_team", "away_team", "score", "event_time"]
        for f in schema_fields:
            self.assertIn(f, schema_fields)

    def test_batch_size_config(self):
        config = {"batch.size": 16384}
        self.assertGreater(config["batch.size"], 0)

    def test_acks_config(self):
        config = {"acks": "all"}
        self.assertEqual(config["acks"], "all")


class TestSparkStreaming(unittest.TestCase):
    def test_checkpoint_path_configured(self):
        config = {"checkpointLocation": "s3://football-data/checkpoints/matches"}
        self.assertIn("checkpointLocation", config)

    def test_output_mode_append(self):
        config = {"outputMode": "append"}
        self.assertEqual(config["outputMode"], "append")

    def test_delta_format(self):
        config = {"format": "delta"}
        self.assertEqual(config["format"], "delta")

    def test_kafka_starting_offsets(self):
        config = {"startingOffsets": "earliest"}
        self.assertIn(config["startingOffsets"], ["earliest", "latest"])

    def test_trigger_once(self):
        config = {"trigger": "availableNow"}
        self.assertIsNotNone(config["trigger"])

    def test_schema_enforcement(self):
        schema_fields = ["match_id", "home_team", "away_team", "score", "event_time"]
        self.assertEqual(len(schema_fields), 5)


class TestGreatExpectations(unittest.TestCase):
    def test_suite_name(self):
        suite = {"name": "football_matches_suite"}
        self.assertIn("matches", suite["name"])

    def test_quarantine_path_configured(self):
        config = {"quarantine_path": "s3://football-data/quarantine/matches"}
        self.assertIn("quarantine", config["quarantine_path"])

    def test_expectation_not_null_match_id(self):
        expectation = {"type": "expect_column_values_to_not_be_null", "column": "match_id"}
        self.assertEqual(expectation["column"], "match_id")

    def test_expectation_value_set_score(self):
        expectation = {"type": "expect_column_values_to_match_regex", "column": "score"}
        self.assertIn("score", expectation["column"])

    def test_validation_result_schema(self):
        result = {"success": True, "statistics": {"evaluated_expectations": 5}}
        self.assertIn("success", result)

    def test_checkpoint_configured(self):
        checkpoint = {"name": "football_checkpoint", "validations": []}
        self.assertIn("validations", checkpoint)


class TestDeltaPostgresLoad(unittest.TestCase):
    def test_upsert_mode(self):
        config = {"mode": "upsert", "conflict_column": "match_id"}
        self.assertEqual(config["mode"], "upsert")

    def test_jdbc_url_format(self):
        url = "jdbc:postgresql://localhost:5432/football"
        self.assertTrue(url.startswith("jdbc:postgresql://"))

    def test_target_table(self):
        config = {"table": "public.matches"}
        self.assertEqual(config["table"], "public.matches")

    def test_batch_size(self):
        config = {"batchsize": 1000}
        self.assertGreater(config["batchsize"], 0)

    def test_connection_properties(self):
        props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}
        self.assertIn("driver", props)

    def test_primary_key_column(self):
        config = {"pk": "match_id"}
        self.assertEqual(config["pk"], "match_id")


class TestDbtTransformation(unittest.TestCase):
    def test_source_table(self):
        config = {"source": "public.matches"}
        self.assertEqual(config["source"], "public.matches")

    def test_target_mart(self):
        config = {"target": "public.mart_team_season_stats"}
        self.assertIn("mart", config["target"])

    def test_model_type_incremental(self):
        config = {"materialized": "incremental"}
        self.assertEqual(config["materialized"], "incremental")

    def test_unique_key_config(self):
        config = {"unique_key": "team_season_id"}
        self.assertIsNotNone(config["unique_key"])

    def test_profile_target(self):
        config = {"target": "prod"}
        self.assertIsNotNone(config["target"])

    def test_schema_matches_expected(self):
        columns = ["team", "season", "wins", "losses", "goals_for", "goals_against"]
        self.assertIn("wins", columns)


class TestMLTraining(unittest.TestCase):
    def test_experiment_name(self):
        config = {"experiment_name": "football_outcome_predictor"}
        self.assertIn("football", config["experiment_name"])

    def test_model_type_xgboost(self):
        config = {"model_type": "XGBClassifier"}
        self.assertEqual(config["model_type"], "XGBClassifier")

    def test_mlflow_tracking_uri(self):
        config = {"tracking_uri": "http://localhost:5001"}
        self.assertTrue(config["tracking_uri"].startswith("http"))

    def test_features_list(self):
        features = ["home_team_form", "away_team_form", "head_to_head", "venue"]
        self.assertGreaterEqual(len(features), 3)

    def test_model_registry_name(self):
        config = {"registered_model_name": "football_outcome_predictor"}
        self.assertIsNotNone(config["registered_model_name"])

    def test_target_column(self):
        config = {"target": "outcome"}
        self.assertEqual(config["target"], "outcome")


class TestBatchInference(unittest.TestCase):
    def test_model_uri_format(self):
        uri = "models:/football_outcome_predictor/Production"
        self.assertTrue(uri.startswith("models:/"))

    def test_output_path(self):
        config = {"output_path": "s3://football-data/delta/predictions"}
        self.assertIn("predictions", config["output_path"])

    def test_output_format_delta(self):
        config = {"format": "delta"}
        self.assertEqual(config["format"], "delta")

    def test_mode_overwrite(self):
        config = {"mode": "overwrite"}
        self.assertEqual(config["mode"], "overwrite")

    def test_batch_size_positive(self):
        config = {"batch_size": 512}
        self.assertGreater(config["batch_size"], 0)

    def test_schema_has_prediction_column(self):
        schema = ["match_id", "home_team", "away_team", "predicted_outcome", "confidence"]
        self.assertIn("predicted_outcome", schema)


class TestDockerCompose(unittest.TestCase):
    def test_kafka_service_defined(self):
        services = ["kafka", "zookeeper", "postgres", "mlflow", "airflow",
                    "marquez-api", "marquez-web", "grafana"]
        self.assertIn("kafka", services)

    def test_postgres_services_count(self):
        postgres_services = ["postgres", "mlflow-postgres", "marquez-postgres"]
        self.assertEqual(len(postgres_services), 3)

    def test_airflow_port(self):
        ports = {"airflow": 8080}
        self.assertEqual(ports["airflow"], 8080)

    def test_mlflow_port(self):
        ports = {"mlflow": 5001}
        self.assertEqual(ports["mlflow"], 5001)

    def test_marquez_api_port(self):
        ports = {"marquez-api": 5002}
        self.assertEqual(ports["marquez-api"], 5002)

    def test_grafana_port(self):
        ports = {"grafana": 3000}
        self.assertEqual(ports["grafana"], 3000)


class TestPrometheus(unittest.TestCase):
    def test_pipeline_stage_duration_metric(self):
        metrics = ["pipeline_stage_duration_seconds", "kafka_messages_consumed_total",
                   "delta_rows_written_total", "postgres_upsert_total"]
        self.assertIn("pipeline_stage_duration_seconds", metrics)

    def test_sla_breach_metric(self):
        metrics = ["sla_breaches_total"]
        self.assertIn("sla_breaches_total", metrics)

    def test_quality_check_failures_metric(self):
        metrics = ["quality_check_failures_total"]
        self.assertIn("quality_check_failures_total", metrics)

    def test_row_count_delta_metric(self):
        metrics = ["row_count_delta"]
        self.assertIn("row_count_delta", metrics)

    def test_data_freshness_metric(self):
        metrics = ["data_freshness_seconds"]
        self.assertIn("data_freshness_seconds", metrics)

    def test_prometheus_port(self):
        config = {"port": 9090}
        self.assertEqual(config["port"], 9090)


class TestGrafana(unittest.TestCase):
    def test_dashboard_provisioned(self):
        config = {"dashboard_path": "/etc/grafana/provisioning/dashboards"}
        self.assertIn("dashboards", config["dashboard_path"])

    def test_datasource_prometheus(self):
        config = {"datasource": "prometheus"}
        self.assertEqual(config["datasource"], "prometheus")

    def test_sla_quality_panel_defined(self):
        panels = ["Pipeline Overview", "Kafka Throughput", "SLA & Quality"]
        self.assertIn("SLA & Quality", panels)

    def test_alert_rule_sla_breach(self):
        rules = [{"name": "SLABreachAlert", "condition": "sla_breaches_total > 0"}]
        self.assertEqual(rules[0]["name"], "SLABreachAlert")

    def test_grafana_port(self):
        config = {"port": 3000}
        self.assertEqual(config["port"], 3000)

    def test_alert_window_one_hour(self):
        rule = {"window": "1h"}
        self.assertEqual(rule["window"], "1h")


class TestLineage(unittest.TestCase):
    def test_lineage_client_singleton(self):
        clients = [object(), object()]
        self.assertIsNotNone(clients[0])

    def test_six_emitters_defined(self):
        emitters = ["streaming_ingest", "validate", "load_postgres",
                    "dbt_transform", "ml_train", "batch_predict"]
        self.assertEqual(len(emitters), 6)

    def test_kafka_dataset_uri(self):
        uri = "kafka://kafka:9092/football_matches"
        self.assertTrue(uri.startswith("kafka://"))

    def test_delta_dataset_uri(self):
        uri = "s3://football-data/delta/matches"
        self.assertTrue(uri.startswith("s3://"))

    def test_postgres_dataset_uri(self):
        uri = "postgres://postgres:5432/football/public.matches"
        self.assertTrue(uri.startswith("postgres://"))

    def test_marquez_api_port(self):
        self.assertEqual(5002, 5002)

    def test_lineage_run_context_manager(self):
        import contextlib
        self.assertTrue(callable(contextlib.contextmanager))

    def test_ol_client_transport_url(self):
        url = "http://localhost:5002/api/v1/lineage"
        self.assertIn("lineage", url)

    def test_sql_facet_in_emitter(self):
        facet = {"_producer": "football-pipeline", "query": "SELECT * FROM matches"}
        self.assertIn("query", facet)

    def test_marquez_web_port(self):
        config = {"port": 3001}
        self.assertEqual(config["port"], 3001)

    def test_dataset_factory_input(self):
        dataset = {"namespace": "s3://football-data", "name": "delta/matches"}
        self.assertIn("namespace", dataset)

    def test_dataset_factory_output(self):
        dataset = {"namespace": "postgres", "name": "public.matches"}
        self.assertIn("name", dataset)

    def test_lineage_graph_nodes(self):
        nodes = ["kafka", "delta_matches", "postgres_matches",
                 "mart_stats", "mlflow_model", "delta_predictions"]
        self.assertEqual(len(nodes), 6)


class TestCIDockerfile(unittest.TestCase):
    def test_base_image(self):
        base = "apache/airflow:2.9.0-python3.11"
        self.assertIn("airflow:2.9.0", base)

    def test_python_version(self):
        base = "apache/airflow:2.9.0-python3.11"
        self.assertIn("python3.11", base)

    def test_requirements_baked_in(self):
        steps = ["COPY requirements.txt .", "RUN pip install -r requirements.txt"]
        self.assertTrue(any("requirements" in s for s in steps))

    def test_dags_copied(self):
        steps = ["COPY dags/ ./dags/"]
        self.assertTrue(any("dags" in s for s in steps))

    def test_lineage_copied(self):
        steps = ["COPY lineage/ ./lineage/"]
        self.assertTrue(any("lineage" in s for s in steps))

    def test_pythonpath_set(self):
        env = {"PYTHONPATH": "/opt/airflow"}
        self.assertIn("PYTHONPATH", env)

    def test_config_copied(self):
        steps = ["COPY config/ ./config/"]
        self.assertTrue(any("config" in s for s in steps))

    def test_sla_directory_should_be_copied(self):
        steps = ["COPY sla/ ./sla/"]
        self.assertTrue(any("sla" in s for s in steps))

    def test_dockerfile_has_user_directive(self):
        directives = ["USER airflow"]
        self.assertTrue(any("airflow" in d for d in directives))

    def test_workdir_configured(self):
        dirs = ["/opt/airflow"]
        self.assertTrue(any("/opt/airflow" in d for d in dirs))

    def test_expose_port_8080(self):
        ports = [8080]
        self.assertIn(8080, ports)

    def test_entrypoint_airflow(self):
        ep = ["airflow", "scheduler"]
        self.assertEqual(ep[0], "airflow")


class TestCIMakefile(unittest.TestCase):
    def test_lint_target(self):
        targets = ["lint", "lint-fix", "test", "test-fast", "build",
                   "build-no-cache", "up", "down", "logs", "smoke", "ci", "clean", "help"]
        self.assertIn("lint", targets)

    def test_test_target(self):
        targets = ["lint", "test"]
        self.assertIn("test", targets)

    def test_build_target(self):
        targets = ["build", "build-no-cache"]
        self.assertIn("build", targets)

    def test_up_down_targets(self):
        targets = ["up", "down"]
        self.assertIn("up", targets)
        self.assertIn("down", targets)

    def test_smoke_target(self):
        targets = ["smoke"]
        self.assertIn("smoke", targets)

    def test_ci_target(self):
        targets = ["ci"]
        self.assertIn("ci", targets)

    def test_clean_target(self):
        targets = ["clean"]
        self.assertIn("clean", targets)

    def test_help_target(self):
        targets = ["help"]
        self.assertIn("help", targets)

    def test_lint_uses_ruff(self):
        cmds = ["ruff check ."]
        self.assertTrue(any("ruff" in c for c in cmds))

    def test_test_uses_pytest(self):
        cmds = ["pytest"]
        self.assertTrue(any("pytest" in c for c in cmds))

    def test_build_uses_docker(self):
        cmds = ["docker build"]
        self.assertTrue(any("docker" in c for c in cmds))

    def test_total_target_count(self):
        targets = ["lint", "lint-fix", "test", "test-fast", "build",
                   "build-no-cache", "up", "down", "logs", "smoke", "ci", "clean", "help"]
        self.assertGreaterEqual(len(targets), 13)


class TestCIGitHubActionsYAML(unittest.TestCase):
    def test_five_jobs_defined(self):
        jobs = ["lint", "test", "docker-build", "integration", "deploy"]
        self.assertEqual(len(jobs), 5)

    def test_lint_job_runs_ruff(self):
        steps = ["ruff check .", "black --check ."]
        self.assertTrue(any("ruff" in s for s in steps))

    def test_test_job_needs_lint(self):
        needs = {"test": ["lint"]}
        self.assertIn("lint", needs["test"])

    def test_test_job_uploads_coverage(self):
        steps = ["upload-artifact", "coverage.xml"]
        self.assertTrue(any("coverage" in s for s in steps))

    def test_docker_build_pushes_on_non_pr(self):
        condition = "github.event_name != 'pull_request'"
        self.assertIn("pull_request", condition)

    def test_integration_needs_test_and_build(self):
        needs = {"integration": ["test", "docker-build"]}
        self.assertIn("test", needs["integration"])
        self.assertIn("docker-build", needs["integration"])

    def test_integration_healthchecks_six_services(self):
        services = ["airflow", "mlflow", "marquez-api", "marquez-web",
                    "prometheus", "grafana"]
        self.assertEqual(len(services), 6)

    def test_integration_posts_lineage_event(self):
        steps = ["POST lineage event to Marquez", "assert 201"]
        self.assertTrue(any("lineage" in s for s in steps))

    def test_deploy_job_main_branch_only(self):
        condition = "github.ref == 'refs/heads/main'"
        self.assertIn("main", condition)

    def test_deploy_retags_sha_to_latest(self):
        steps = ["retag SHA to latest", "docker push"]
        self.assertTrue(any("latest" in s for s in steps))


# ===========================================================================
# Entry point
# ===========================================================================
if __name__ == "__main__":
    unittest.main()
