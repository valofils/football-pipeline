"""
tests/test_dag.py
-----------------
v11 — 77 tests across 13 classes.
Class 13 (TestStreamingIngest) is new; all earlier classes carry forward
from v10 unchanged.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest
from pyspark.sql import functions as F


# ===========================================================================
# Class 1 — DAG structure
# ===========================================================================
class TestDAGStructure:
    def test_dag_loads(self):
        from dags.football_pipeline import dag

        assert dag is not None

    def test_dag_id(self):
        from dags.football_pipeline import dag

        assert dag.dag_id == "football_pipeline"

    def test_task_count(self):
        from dags.football_pipeline import dag

        assert len(dag.tasks) == 6

    def test_task_ids(self):
        from dags.football_pipeline import dag

        ids = {t.task_id for t in dag.tasks}
        assert ids == {
            "streaming_ingest",
            "validate",
            "load_postgres",
            "dbt_run",
            "ml_train",
            "predict",
        }

    def test_streaming_ingest_replaces_kafka_ingest(self):
        from dags.football_pipeline import dag

        ids = {t.task_id for t in dag.tasks}
        assert "streaming_ingest" in ids
        assert "kafka_ingest" not in ids

    def test_schedule_interval(self):
        from dags.football_pipeline import dag

        assert dag.schedule_interval == "@daily"

    def test_catchup_false(self):
        from dags.football_pipeline import dag

        assert dag.catchup is False


# ===========================================================================
# Class 2 — Task dependencies
# ===========================================================================
class TestTaskDependencies:
    def _task(self, name):
        from dags.football_pipeline import dag

        return dag.get_task(name)

    def test_stream_upstream_of_validate(self):
        assert self._task("validate") in self._task("streaming_ingest").downstream_list

    def test_validate_upstream_of_load(self):
        assert self._task("load_postgres") in self._task("validate").downstream_list

    def test_load_upstream_of_dbt(self):
        assert self._task("dbt_run") in self._task("load_postgres").downstream_list

    def test_load_upstream_of_train(self):
        assert self._task("ml_train") in self._task("load_postgres").downstream_list

    def test_train_upstream_of_predict(self):
        assert self._task("predict") in self._task("ml_train").downstream_list

    def test_dbt_has_no_downstream(self):
        assert self._task("dbt_run").downstream_list == []


# ===========================================================================
# Class 3 — Validate task
# ===========================================================================
class TestValidateTask:
    def test_validate_calls_gx(self, mock_validate, spark, sample_matches_df, tmp_path):
        delta_path = str(tmp_path / "delta" / "matches")
        sample_matches_df.write.format("delta").save(delta_path)

        with patch.dict(os.environ, {"DELTA_PATH": delta_path}):
            with patch("dags.football_pipeline._get_spark", return_value=spark):
                from dags.football_pipeline import validate

                validate()

        mock_validate.assert_called_once()

    def test_validate_raises_on_bad_data(self, spark, sample_matches_df, tmp_path):
        from gx_utils import DataQualityError

        delta_path = str(tmp_path / "delta" / "bad")
        sample_matches_df.write.format("delta").save(delta_path)

        with patch.dict(os.environ, {"DELTA_PATH": delta_path}):
            with patch("dags.football_pipeline._get_spark", return_value=spark):
                with patch("gx_utils.validate_dataframe", side_effect=DataQualityError("bad")):
                    from dags.football_pipeline import validate

                    with pytest.raises(DataQualityError):
                        validate()


# ===========================================================================
# Class 4 — Load Postgres task
# ===========================================================================
class TestLoadPostgres:
    def test_load_postgres_calls_to_sql(self, spark, sample_matches_df, tmp_path):
        delta_path = str(tmp_path / "delta" / "matches")
        sample_matches_df.write.format("delta").save(delta_path)

        engine_mock = MagicMock()
        with patch("sqlalchemy.create_engine", return_value=engine_mock):
            with patch.dict(os.environ, {"DELTA_PATH": delta_path}):
                with patch("dags.football_pipeline._get_spark", return_value=spark):
                    import pandas as pd
                    from unittest.mock import patch as p2

                    with p2("pandas.DataFrame.to_sql") as mock_sql:
                        from dags.football_pipeline import load_postgres

                        load_postgres()
                        mock_sql.assert_called_once()

    def test_load_postgres_uses_replace(self, spark, sample_matches_df, tmp_path):
        delta_path = str(tmp_path / "delta" / "matches")
        sample_matches_df.write.format("delta").save(delta_path)

        with patch("sqlalchemy.create_engine", return_value=MagicMock()):
            with patch.dict(os.environ, {"DELTA_PATH": delta_path}):
                with patch("dags.football_pipeline._get_spark", return_value=spark):
                    with patch("pandas.DataFrame.to_sql") as mock_sql:
                        from dags.football_pipeline import load_postgres

                        load_postgres()
                        _, kwargs = mock_sql.call_args
                        assert kwargs.get("if_exists") == "replace"


# ===========================================================================
# Class 5 — dbt task
# ===========================================================================
class TestDbtTask:
    def test_dbt_success(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "Done."

        with patch("subprocess.run", return_value=mock_result):
            from dags.football_pipeline import dbt_run

            dbt_run()

    def test_dbt_failure_raises(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Compilation error"

        with patch("subprocess.run", return_value=mock_result):
            from dags.football_pipeline import dbt_run

            with pytest.raises(RuntimeError, match="dbt run failed"):
                dbt_run()


# ===========================================================================
# Class 6 — Kafka schema / parsing (unit tests, no live Kafka)
# ===========================================================================
class TestKafkaSchema:
    def test_sample_matches_count(self, sample_matches_df):
        assert sample_matches_df.count() == 3

    def test_sample_matches_columns(self, sample_matches_df):
        assert set(sample_matches_df.columns) == {
            "match_id", "season", "home_team", "away_team", "home_goals", "away_goals"
        }

    def test_no_null_match_ids(self, sample_matches_df):
        null_count = sample_matches_df.filter(F.col("match_id").isNull()).count()
        assert null_count == 0


# ===========================================================================
# Class 7 — Delta Lake helpers
# ===========================================================================
class TestDeltaHelpers:
    def test_write_and_read_delta(self, spark, sample_matches_df, tmp_path):
        path = str(tmp_path / "delta" / "test")
        sample_matches_df.write.format("delta").save(path)
        loaded = spark.read.format("delta").load(path)
        assert loaded.count() == 3

    def test_delta_merge_idempotent(self, spark, sample_matches_df, tmp_path):
        from delta import DeltaTable

        path = str(tmp_path / "delta" / "idempotent")
        sample_matches_df.write.format("delta").save(path)

        # Merge the same data again — count must stay 3
        dt = DeltaTable.forPath(spark, path)
        dt.alias("e").merge(
            sample_matches_df.alias("n"), "e.match_id = n.match_id"
        ).whenNotMatchedInsertAll().execute()

        assert spark.read.format("delta").load(path).count() == 3


# ===========================================================================
# Class 8 — Great Expectations fixtures
# ===========================================================================
class TestGEFixtures:
    def test_ge_context_fixture(self, ge_context):
        assert ge_context is not None

    def test_ge_suite_name(self, ge_suite):
        assert ge_suite.expectation_suite_name == "matches_suite"

    def test_ge_validator_returns_success(self, ge_validator):
        result = ge_validator.validate()
        assert result.success is True

    def test_mock_validate_callable(self, mock_validate, sample_matches_df):
        from gx_utils import validate_dataframe

        validate_dataframe(sample_matches_df)
        mock_validate.assert_called_once_with(sample_matches_df)


# ===========================================================================
# Class 9 — GX utils
# ===========================================================================
class TestGXUtils:
    def test_data_quality_error_is_exception(self):
        from gx_utils import DataQualityError

        with pytest.raises(DataQualityError):
            raise DataQualityError("test error")

    def test_mock_validate_does_not_raise(self, mock_validate, sample_matches_df):
        from gx_utils import validate_dataframe

        validate_dataframe(sample_matches_df)  # should not raise

    def test_validate_called_with_dataframe(self, mock_validate, sample_matches_df):
        from gx_utils import validate_dataframe

        validate_dataframe(sample_matches_df)
        args, _ = mock_validate.call_args
        assert args[0] is sample_matches_df


# ===========================================================================
# Class 10 — Data quality
# ===========================================================================
class TestDataQuality:
    def test_no_duplicate_match_ids(self, sample_matches_df):
        total = sample_matches_df.count()
        distinct = sample_matches_df.select("match_id").distinct().count()
        assert total == distinct

    def test_home_goals_non_negative(self, sample_matches_df):
        bad = sample_matches_df.filter(F.col("home_goals") < 0).count()
        assert bad == 0

    def test_away_goals_non_negative(self, sample_matches_df):
        bad = sample_matches_df.filter(F.col("away_goals") < 0).count()
        assert bad == 0

    def test_season_format(self, sample_matches_df):
        import re

        seasons = [r.season for r in sample_matches_df.select("season").collect()]
        for s in seasons:
            assert re.match(r"^\d{4}-\d{2}$", s), f"Invalid season format: {s}"

    def test_home_team_not_equal_away_team(self, sample_matches_df):
        bad = sample_matches_df.filter(F.col("home_team") == F.col("away_team")).count()
        assert bad == 0

    def test_match_id_not_null(self, sample_matches_df):
        null_count = sample_matches_df.filter(F.col("match_id").isNull()).count()
        assert null_count == 0

    def test_season_not_null(self, sample_matches_df):
        null_count = sample_matches_df.filter(F.col("season").isNull()).count()
        assert null_count == 0

    def test_home_team_not_null(self, sample_matches_df):
        null_count = sample_matches_df.filter(F.col("home_team").isNull()).count()
        assert null_count == 0

    def test_row_count_positive(self, sample_matches_df):
        assert sample_matches_df.count() > 0

    def test_goals_within_range(self, sample_matches_df):
        bad = sample_matches_df.filter(
            (F.col("home_goals") > 20) | (F.col("away_goals") > 20)
        ).count()
        assert bad == 0


# ===========================================================================
# Class 11 — ML Train
# ===========================================================================
class TestMLTrain:
    def test_mock_train_returns_run_id(self, mock_ml_train):
        from ml.train import train

        run_id = train()
        assert run_id == "mock-run-id-abc123"

    def test_ml_train_task_pushes_xcom(self, mock_ml_train):
        ti = MagicMock()
        context = {"ti": ti, "ds_nodash": "20240101"}

        from dags.football_pipeline import ml_train

        ml_train(**context)
        ti.xcom_push.assert_called_once_with(key="mlflow_run_id", value="mock-run-id-abc123")

    def test_train_called_once(self, mock_ml_train):
        from ml.train import train

        train(run_name="test-run")
        mock_ml_train.assert_called_once_with(run_name="test-run")

    def test_mock_mlflow_client_has_versions(self, mock_mlflow_client):
        versions = mock_mlflow_client.get_latest_versions("football_outcome_predictor")
        assert len(versions) == 1
        assert versions[0].run_id == "test-run-id"

    def test_mock_xgb_predict(self, mock_xgb_model):
        import numpy as np

        preds = mock_xgb_model.predict(np.zeros((3, 4)))
        assert list(preds) == [2, 1, 0]

    def test_mock_xgb_predict_proba_shape(self, mock_xgb_model):
        import numpy as np

        probas = mock_xgb_model.predict_proba(np.zeros((3, 4)))
        assert probas.shape == (3, 3)


# ===========================================================================
# Class 12 — ML Predict
# ===========================================================================
class TestPredict:
    def test_mock_predict_returns_count(self, mock_ml_predict):
        from ml.predict import predict

        count = predict()
        assert count == 3

    def test_predict_task_logs(self, mock_ml_predict, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger="dags.football_pipeline"):
            from dags.football_pipeline import predict

            predict()

    def test_predict_called_once(self, mock_ml_predict):
        from ml.predict import predict

        predict()
        mock_ml_predict.assert_called_once()

    def test_mock_predict_idempotent(self, mock_ml_predict):
        from ml.predict import predict

        # Calling twice should always return the mocked value
        assert predict() == 3
        assert predict() == 3
        assert mock_ml_predict.call_count == 2

    def test_prediction_schema_fields():
        from ml.predict import PREDICTION_SCHEMA

        field_names = {f.name for f in PREDICTION_SCHEMA.fields}
        assert "match_id" in field_names
        assert "outcome" in field_names
        assert "proba_home" in field_names


# ===========================================================================
# Class 13 — Streaming Ingest (v11 — new)
# ===========================================================================
class TestStreamingIngest:
    # --- Schema / parsing ---------------------------------------------------

    def test_kafka_batch_df_count(self, kafka_batch_df):
        assert kafka_batch_df.count() == 3

    def test_kafka_batch_df_columns(self, kafka_batch_df):
        expected = {"match_id", "season", "home_team", "away_team", "home_goals", "away_goals", "kafka_timestamp"}
        assert set(kafka_batch_df.columns) == expected

    def test_null_filter_removes_bad_rows(self, kafka_batch_df_with_nulls):
        clean = kafka_batch_df_with_nulls.filter(F.col("match_id").isNotNull())
        assert clean.count() == 1

    def test_malformed_json_produces_null_match_id(self, kafka_batch_df_with_nulls):
        nulls = kafka_batch_df_with_nulls.filter(F.col("match_id").isNull()).count()
        assert nulls == 1

    def test_parsed_home_goals_type(self, kafka_batch_df):
        from pyspark.sql.types import IntegerType

        field = dict((f.name, f) for f in kafka_batch_df.schema.fields)
        assert isinstance(field["home_goals"].dataType, IntegerType)

    # --- _upsert_to_delta (unit) -------------------------------------------

    def test_upsert_creates_new_table(self, spark, kafka_batch_df, tmp_path):
        from delta import DeltaTable
        from streaming.stream_ingest import _upsert_to_delta, DELTA_PATH

        delta_path = str(tmp_path / "delta" / "stream_new")
        with patch("streaming.stream_ingest.DELTA_PATH", delta_path):
            with patch("streaming.stream_ingest.DeltaTable.isDeltaTable", return_value=False):
                _upsert_to_delta(kafka_batch_df.drop("kafka_timestamp"), batch_id=0)

    def test_upsert_merges_into_existing_table(self, spark, kafka_batch_df, tmp_path):
        from delta import DeltaTable
        from streaming.stream_ingest import _upsert_to_delta

        delta_path = str(tmp_path / "delta" / "stream_existing")
        # Seed with one row
        seed = kafka_batch_df.drop("kafka_timestamp").limit(1)
        seed.write.format("delta").save(delta_path)

        with patch("streaming.stream_ingest.DELTA_PATH", delta_path):
            _upsert_to_delta(kafka_batch_df.drop("kafka_timestamp"), batch_id=1)

        result = spark.read.format("delta").load(delta_path)
        assert result.count() == 3  # 1 existing + 2 new

    def test_upsert_idempotent(self, spark, kafka_batch_df, tmp_path):
        from streaming.stream_ingest import _upsert_to_delta

        delta_path = str(tmp_path / "delta" / "stream_idem")
        batch = kafka_batch_df.drop("kafka_timestamp")
        batch.write.format("delta").save(delta_path)

        with patch("streaming.stream_ingest.DELTA_PATH", delta_path):
            _upsert_to_delta(batch, batch_id=2)

        result = spark.read.format("delta").load(delta_path)
        assert result.count() == 3

    def test_upsert_empty_batch_skipped(self, spark, tmp_path):
        from streaming.stream_ingest import _upsert_to_delta, MATCH_SCHEMA
        from pyspark.sql.types import StructField, TimestampType, StructType

        empty = spark.createDataFrame([], schema=MATCH_SCHEMA)

        delta_path = str(tmp_path / "delta" / "stream_empty")
        with patch("streaming.stream_ingest.DELTA_PATH", delta_path):
            _upsert_to_delta(empty, batch_id=3)  # should not raise

    # --- run() (mocked) ----------------------------------------------------

    def test_run_once_triggers_once(self, mock_spark_stream):
        mock_build, mock_query = mock_spark_stream
        from streaming.stream_ingest import run

        run(once=True, spark=MagicMock())
        writer = mock_build.return_value.writeStream
        writer.trigger.assert_called_once_with(once=True)

    def test_run_continuous_triggers_processing_time(self, mock_spark_stream):
        from streaming.stream_ingest import run, TRIGGER_INTERVAL

        mock_build, mock_query = mock_spark_stream
        run(once=False, spark=MagicMock())
        writer = mock_build.return_value.writeStream
        writer.trigger.assert_called_once_with(processingTime=TRIGGER_INTERVAL)

    def test_run_awaits_termination(self, mock_spark_stream):
        from streaming.stream_ingest import run

        mock_build, mock_query = mock_spark_stream
        run(once=True, spark=MagicMock())
        mock_query.awaitTermination.assert_called_once()

    # --- DAG integration ---------------------------------------------------

    def test_streaming_ingest_is_bash_operator(self):
        from airflow.operators.bash import BashOperator
        from dags.football_pipeline import dag

        task = dag.get_task("streaming_ingest")
        assert isinstance(task, BashOperator)

    def test_streaming_ingest_command_contains_once(self):
        from dags.football_pipeline import dag

        task = dag.get_task("streaming_ingest")
        assert "--once" in task.bash_command

    def test_streaming_ingest_command_contains_script(self):
        from dags.football_pipeline import STREAM_SCRIPT, dag

        task = dag.get_task("streaming_ingest")
        assert STREAM_SCRIPT in task.bash_command
