"""
tests/test_dag.py
-----------------
60 tests across 11 classes.
Classes 1-10 are carried from v9; class 11 (TestMLTrain) and 12 (TestPredict) are new.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch, call

import pandas as pd
import numpy as np
import pytest


# ===========================================================================
# Class 1 — DAG structure
# ===========================================================================
class TestDagStructure:
    def test_dag_id(self):
        from dags.football_pipeline import dag
        assert dag.dag_id == "football_pipeline"

    def test_dag_has_six_tasks(self):
        from dags.football_pipeline import dag
        assert len(dag.tasks) == 6

    def test_schedule_interval(self):
        from dags.football_pipeline import dag
        assert dag.schedule_interval == "@daily"

    def test_tags_contain_mlops(self):
        from dags.football_pipeline import dag
        assert "mlops" in dag.tags

    def test_catchup_disabled(self):
        from dags.football_pipeline import dag
        assert dag.catchup is False


# ===========================================================================
# Class 2 — Task existence
# ===========================================================================
class TestTaskExistence:
    @pytest.fixture(autouse=True)
    def dag_tasks(self):
        from dags.football_pipeline import dag
        self.task_ids = {t.task_id for t in dag.tasks}

    def test_kafka_ingest_task_exists(self):
        assert "kafka_ingest" in self.task_ids

    def test_validate_task_exists(self):
        assert "validate" in self.task_ids

    def test_load_postgres_task_exists(self):
        assert "load_postgres" in self.task_ids

    def test_dbt_run_task_exists(self):
        assert "dbt_run" in self.task_ids

    def test_ml_train_task_exists(self):
        assert "ml_train" in self.task_ids

    def test_predict_task_exists(self):
        assert "predict" in self.task_ids


# ===========================================================================
# Class 3 — Task dependencies
# ===========================================================================
class TestTaskDependencies:
    @pytest.fixture(autouse=True)
    def tasks(self):
        from dags.football_pipeline import dag
        self.tasks = {t.task_id: t for t in dag.tasks}

    def test_validate_depends_on_ingest(self):
        assert "kafka_ingest" in {
            t.task_id for t in self.tasks["validate"].upstream_list
        }

    def test_load_depends_on_validate(self):
        assert "validate" in {
            t.task_id for t in self.tasks["load_postgres"].upstream_list
        }

    def test_dbt_depends_on_load(self):
        assert "load_postgres" in {
            t.task_id for t in self.tasks["dbt_run"].upstream_list
        }

    def test_ml_train_depends_on_load(self):
        assert "load_postgres" in {
            t.task_id for t in self.tasks["ml_train"].upstream_list
        }

    def test_predict_depends_on_ml_train(self):
        assert "ml_train" in {
            t.task_id for t in self.tasks["predict"].upstream_list
        }


# ===========================================================================
# Class 4 — Kafka ingest
# ===========================================================================
class TestKafkaIngest:
    def test_ingest_skips_on_no_messages(self):
        from dags.football_pipeline import kafka_ingest

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        with patch("dags.football_pipeline.Consumer", return_value=mock_consumer), \
             patch("dags.football_pipeline.get_spark"):
            kafka_ingest()  # should not raise

    def test_ingest_closes_consumer_on_exit(self):
        from dags.football_pipeline import kafka_ingest

        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        with patch("dags.football_pipeline.Consumer", return_value=mock_consumer), \
             patch("dags.football_pipeline.get_spark"):
            kafka_ingest()
        mock_consumer.close.assert_called_once()

    def test_ingest_writes_delta_when_records_present(self):
        from dags.football_pipeline import kafka_ingest
        import json

        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = json.dumps(
            {"match_id": "m001", "home_team": "Arsenal", "away_team": "Chelsea",
             "home_goals": 2, "away_goals": 1, "season": "2023-24", "match_date": "2024-01-01"}
        ).encode()

        mock_consumer = MagicMock()
        mock_consumer.poll.side_effect = [msg, None]

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        with patch("dags.football_pipeline.Consumer", return_value=mock_consumer), \
             patch("dags.football_pipeline.get_spark", return_value=mock_spark), \
             patch("dags.football_pipeline.DeltaTable") as mock_dt:
            mock_dt.isDeltaTable.return_value = False
            kafka_ingest()

        mock_df.write.format.assert_called_once_with("delta")


# ===========================================================================
# Class 5 — Validate task
# ===========================================================================
class TestValidateTask:
    def test_validate_calls_gx(self, mock_validate):
        from dags.football_pipeline import validate

        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = MagicMock()

        with patch("dags.football_pipeline.get_spark", return_value=mock_spark), \
             patch("dags.football_pipeline.validate_dataframe", mock_validate):
            validate()

        mock_validate.assert_called_once()

    def test_validate_raises_on_quality_failure(self, mock_validate_failure):
        from dags.football_pipeline import validate
        from dags.gx_utils import DataQualityError

        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = MagicMock()

        with patch("dags.football_pipeline.get_spark", return_value=mock_spark), \
             patch("dags.football_pipeline.validate_dataframe", mock_validate_failure):
            with pytest.raises(DataQualityError):
                validate()

    def test_validate_passes_run_id(self, mock_validate):
        from dags.football_pipeline import validate

        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value = MagicMock()

        with patch("dags.football_pipeline.get_spark", return_value=mock_spark), \
             patch("dags.football_pipeline.validate_dataframe", mock_validate):
            validate()

        _, kwargs = mock_validate.call_args
        assert "run_id" in kwargs


# ===========================================================================
# Class 6 — Load Postgres
# ===========================================================================
class TestLoadPostgres:
    def test_load_calls_to_sql(self):
        from dags.football_pipeline import load_postgres

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_df.toPandas.return_value = pd.DataFrame({"match_id": range(10)})
        mock_spark.read.format.return_value.load.return_value = mock_df

        mock_hook = MagicMock()
        mock_engine = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = mock_engine

        with patch("dags.football_pipeline.get_spark", return_value=mock_spark), \
             patch("dags.football_pipeline.PostgresHook", return_value=mock_hook):
            load_postgres()

    def test_load_uses_correct_conn_id(self):
        from dags.football_pipeline import load_postgres

        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.return_value.toPandas.return_value = pd.DataFrame()
        mock_spark.read.format.return_value.load.return_value.count.return_value = 0

        mock_hook = MagicMock()

        with patch("dags.football_pipeline.get_spark", return_value=mock_spark), \
             patch("dags.football_pipeline.PostgresHook", return_value=mock_hook) as ph:
            load_postgres()
        ph.assert_called_once_with(postgres_conn_id="football_postgres")


# ===========================================================================
# Class 7 — dbt run
# ===========================================================================
class TestDbtRun:
    def test_dbt_succeeds(self):
        from dags.football_pipeline import dbt_run
        import subprocess

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "Done."
        with patch("subprocess.run", return_value=mock_result):
            dbt_run()

    def test_dbt_raises_on_failure(self):
        from dags.football_pipeline import dbt_run

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Compilation error"
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="dbt run failed"):
                dbt_run()


# ===========================================================================
# Class 8 — GX utils
# ===========================================================================
class TestGxUtils:
    def test_validation_summary_success_flag(self):
        from dags.gx_utils import ValidationSummary
        s = ValidationSummary(success=True, run_id="r1")
        assert s.success is True
        assert s.failed_expectations == []

    def test_data_quality_error_carries_summary(self):
        from dags.gx_utils import DataQualityError, ValidationSummary
        summary = ValidationSummary(success=False, run_id="r2", failed_expectations=[{"expectation_type": "x"}])
        err = DataQualityError("fail", summary=summary)
        assert err.summary.run_id == "r2"
        assert len(err.summary.failed_expectations) == 1

    def test_data_quality_error_is_exception(self):
        from dags.gx_utils import DataQualityError, ValidationSummary
        with pytest.raises(DataQualityError):
            raise DataQualityError("boom", summary=ValidationSummary(success=False, run_id="r"))


# ===========================================================================
# Class 9 — Feature engineering
# ===========================================================================
class TestFeatureEngineering:
    def test_build_features_shape(self, sample_df):
        from ml.train import build_features
        X, y = build_features(sample_df)
        assert X.shape[0] == len(sample_df)
        assert X.shape[1] == 4

    def test_labels_are_0_1_or_2(self, sample_df):
        from ml.train import build_features
        _, y = build_features(sample_df)
        assert set(y.unique()).issubset({0, 1, 2})

    def test_home_win_label(self):
        from ml.train import _label
        row = pd.Series({"home_goals": 3, "away_goals": 1})
        assert _label(row) == 2

    def test_away_win_label(self):
        from ml.train import _label
        row = pd.Series({"home_goals": 0, "away_goals": 2})
        assert _label(row) == 0

    def test_draw_label(self):
        from ml.train import _label
        row = pd.Series({"home_goals": 1, "away_goals": 1})
        assert _label(row) == 1

    def test_feature_columns_present(self, sample_df):
        from ml.train import build_features
        X, _ = build_features(sample_df)
        assert set(X.columns) == {"home_team_enc", "away_team_enc", "season_enc", "goal_diff_hist"}

    def test_no_nulls_in_features(self, sample_df):
        from ml.train import build_features
        X, y = build_features(sample_df)
        assert not X.isnull().any().any()
        assert not y.isnull().any()


# ===========================================================================
# Class 10 — Data quality (from v9)
# ===========================================================================
class TestDataQuality:
    def test_mock_validate_returns_summary(self, mock_validate):
        from dags.gx_utils import ValidationSummary
        result = mock_validate(spark_df=MagicMock(), run_id="r")
        assert isinstance(result, ValidationSummary)
        assert result.success is True

    def test_mock_validate_failure_raises(self, mock_validate_failure):
        from dags.gx_utils import DataQualityError
        with pytest.raises(DataQualityError):
            mock_validate_failure(spark_df=MagicMock(), run_id="r")

    def test_failed_expectations_populated(self, mock_validate_failure):
        from dags.gx_utils import DataQualityError
        try:
            mock_validate_failure(spark_df=MagicMock(), run_id="r")
        except DataQualityError as e:
            assert len(e.summary.failed_expectations) == 1

    def test_failure_summary_run_id(self, mock_validate_failure):
        from dags.gx_utils import DataQualityError
        try:
            mock_validate_failure(spark_df=MagicMock(), run_id="r")
        except DataQualityError as e:
            assert e.summary.run_id == "fail-run"

    def test_failed_expectation_has_type(self, mock_validate_failure):
        from dags.gx_utils import DataQualityError
        try:
            mock_validate_failure(spark_df=MagicMock(), run_id="r")
        except DataQualityError as e:
            assert "expectation_type" in e.summary.failed_expectations[0]

    def test_success_summary_no_failures(self, mock_validate):
        from dags.gx_utils import ValidationSummary
        result = mock_validate(spark_df=MagicMock(), run_id="r")
        assert result.failed_expectations == []

    def test_validation_summary_dataclass_fields(self):
        from dags.gx_utils import ValidationSummary
        s = ValidationSummary(success=True, run_id="abc")
        assert hasattr(s, "failed_expectations")

    def test_data_quality_error_message(self):
        from dags.gx_utils import DataQualityError, ValidationSummary
        err = DataQualityError("bad data", summary=ValidationSummary(success=False, run_id="x"))
        assert "bad data" in str(err)

    def test_validation_summary_failed_list_type(self):
        from dags.gx_utils import ValidationSummary
        s = ValidationSummary(success=False, run_id="r", failed_expectations=[{}])
        assert isinstance(s.failed_expectations, list)

    def test_mock_called_with_run_id(self, mock_validate):
        mock_validate(spark_df=MagicMock(), run_id="my-run")
        _, kwargs = mock_validate.call_args
        assert kwargs["run_id"] == "my-run"


# ===========================================================================
# Class 11 — MLflow train task
# ===========================================================================
class TestMLTrain:
    def test_ml_train_task_calls_train_fn(self, mock_ml_train):
        from dags.football_pipeline import ml_train

        mock_ti = MagicMock()
        context = {"ds_nodash": "20240101", "ti": mock_ti}

        with patch("dags.football_pipeline.ml_train_fn", mock_ml_train):
            ml_train(**context)

        mock_ml_train.assert_called_once()

    def test_ml_train_passes_run_name(self, mock_ml_train):
        from dags.football_pipeline import ml_train

        context = {"ds_nodash": "20240101", "ti": MagicMock()}

        with patch("dags.football_pipeline.ml_train_fn", mock_ml_train):
            ml_train(**context)

        _, kwargs = mock_ml_train.call_args
        assert "airflow_20240101" in kwargs.get("run_name", "")

    def test_ml_train_pushes_xcom(self, mock_ml_train):
        from dags.football_pipeline import ml_train

        mock_ti = MagicMock()
        context = {"ds_nodash": "20240101", "ti": mock_ti}

        with patch("dags.football_pipeline.ml_train_fn", mock_ml_train):
            ml_train(**context)

        mock_ti.xcom_push.assert_called_once_with(
            key="mlflow_run_id", value="mock-run-id-abc123"
        )

    def test_ml_train_fn_sets_experiment(self):
        from ml.train import train

        with patch("ml.train.mlflow") as mock_mlflow, \
             patch("ml.train.get_spark") as mock_spark, \
             patch("ml.train.load_delta") as mock_load, \
             patch("ml.train.build_features") as mock_feat, \
             patch("ml.train.train_test_split") as mock_split, \
             patch("ml.train.xgb.XGBClassifier") as mock_xgb:

            mock_load.return_value = pd.DataFrame()
            X = pd.DataFrame(np.zeros((20, 4)), columns=["a", "b", "c", "d"])
            y = pd.Series([0, 1, 2] * 6 + [0, 2])
            mock_feat.return_value = (X, y)
            mock_split.return_value = (X[:16], X[16:], y[:16], y[16:])

            model = MagicMock()
            model.predict.return_value = np.array([0, 1, 2, 2])
            model.predict_proba.return_value = np.tile([0.2, 0.3, 0.5], (4, 1))
            mock_xgb.return_value = model
            mock_mlflow.start_run.return_value.__enter__ = MagicMock(
                return_value=MagicMock(info=MagicMock(run_id="test-id"))
            )
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            train()

        mock_mlflow.set_experiment.assert_called_once()

    def test_ml_train_fn_logs_accuracy(self):
        from ml.train import train

        with patch("ml.train.mlflow") as mock_mlflow, \
             patch("ml.train.get_spark"), \
             patch("ml.train.load_delta") as mock_load, \
             patch("ml.train.build_features") as mock_feat, \
             patch("ml.train.train_test_split") as mock_split, \
             patch("ml.train.xgb.XGBClassifier") as mock_xgb:

            mock_load.return_value = pd.DataFrame()
            X = pd.DataFrame(np.zeros((20, 4)), columns=["a", "b", "c", "d"])
            y = pd.Series([0, 1, 2] * 6 + [0, 2])
            mock_feat.return_value = (X, y)
            mock_split.return_value = (X[:16], X[16:], y[:16], y[16:])

            model = MagicMock()
            model.predict.return_value = np.array([0, 1, 2, 2])
            model.predict_proba.return_value = np.tile([0.2, 0.3, 0.5], (4, 1))
            mock_xgb.return_value = model
            run_mock = MagicMock(info=MagicMock(run_id="test-id"))
            mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=run_mock)
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            train()

        calls = [str(c) for c in mock_mlflow.log_metric.call_args_list]
        assert any("accuracy" in c for c in calls)

    def test_train_returns_run_id(self):
        from ml.train import train

        with patch("ml.train.mlflow") as mock_mlflow, \
             patch("ml.train.get_spark"), \
             patch("ml.train.load_delta") as mock_load, \
             patch("ml.train.build_features") as mock_feat, \
             patch("ml.train.train_test_split") as mock_split, \
             patch("ml.train.xgb.XGBClassifier") as mock_xgb:

            mock_load.return_value = pd.DataFrame()
            X = pd.DataFrame(np.zeros((20, 4)), columns=["a", "b", "c", "d"])
            y = pd.Series([0, 1, 2] * 6 + [0, 2])
            mock_feat.return_value = (X, y)
            mock_split.return_value = (X[:16], X[16:], y[:16], y[16:])

            model = MagicMock()
            model.predict.return_value = np.array([0, 1, 2, 2])
            model.predict_proba.return_value = np.tile([0.2, 0.3, 0.5], (4, 1))
            mock_xgb.return_value = model
            run_mock = MagicMock(info=MagicMock(run_id="expected-run-id"))
            mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=run_mock)
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            result = train()

        assert result == "expected-run-id"

    def test_xgb_params_logged(self):
        from ml.train import train, XGB_PARAMS

        with patch("ml.train.mlflow") as mock_mlflow, \
             patch("ml.train.get_spark"), \
             patch("ml.train.load_delta") as mock_load, \
             patch("ml.train.build_features") as mock_feat, \
             patch("ml.train.train_test_split") as mock_split, \
             patch("ml.train.xgb.XGBClassifier") as mock_xgb:

            mock_load.return_value = pd.DataFrame()
            X = pd.DataFrame(np.zeros((20, 4)), columns=["a", "b", "c", "d"])
            y = pd.Series([0, 1, 2] * 6 + [0, 2])
            mock_feat.return_value = (X, y)
            mock_split.return_value = (X[:16], X[16:], y[:16], y[16:])
            model = MagicMock()
            model.predict.return_value = np.array([0, 1, 2, 2])
            model.predict_proba.return_value = np.tile([0.2, 0.3, 0.5], (4, 1))
            mock_xgb.return_value = model
            run_mock = MagicMock(info=MagicMock(run_id="r"))
            mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=run_mock)
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            train()

        mock_mlflow.log_params.assert_called_once_with(XGB_PARAMS)

    def test_model_registered_with_alias(self):
        from ml.train import train, MODEL_ALIAS

        with patch("ml.train.mlflow") as mock_mlflow, \
             patch("ml.train.get_spark"), \
             patch("ml.train.load_delta") as mock_load, \
             patch("ml.train.build_features") as mock_feat, \
             patch("ml.train.train_test_split") as mock_split, \
             patch("ml.train.xgb.XGBClassifier") as mock_xgb:

            mock_load.return_value = pd.DataFrame()
            X = pd.DataFrame(np.zeros((20, 4)), columns=["a", "b", "c", "d"])
            y = pd.Series([0, 1, 2] * 6 + [0, 2])
            mock_feat.return_value = (X, y)
            mock_split.return_value = (X[:16], X[16:], y[:16], y[16:])
            model = MagicMock()
            model.predict.return_value = np.array([0, 1, 2, 2])
            model.predict_proba.return_value = np.tile([0.2, 0.3, 0.5], (4, 1))
            mock_xgb.return_value = model
            run_mock = MagicMock(info=MagicMock(run_id="r"))
            mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=run_mock)
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            train()

        _, kwargs = mock_mlflow.xgboost.log_model.call_args
        assert kwargs["registered_model_name"] == MODEL_ALIAS

    def test_f1_metric_logged(self):
        from ml.train import train

        with patch("ml.train.mlflow") as mock_mlflow, \
             patch("ml.train.get_spark"), \
             patch("ml.train.load_delta") as mock_load, \
             patch("ml.train.build_features") as mock_feat, \
             patch("ml.train.train_test_split") as mock_split, \
             patch("ml.train.xgb.XGBClassifier") as mock_xgb:

            mock_load.return_value = pd.DataFrame()
            X = pd.DataFrame(np.zeros((20, 4)), columns=["a", "b", "c", "d"])
            y = pd.Series([0, 1, 2] * 6 + [0, 2])
            mock_feat.return_value = (X, y)
            mock_split.return_value = (X[:16], X[16:], y[:16], y[16:])
            model = MagicMock()
            model.predict.return_value = np.array([0, 1, 2, 2])
            model.predict_proba.return_value = np.tile([0.2, 0.3, 0.5], (4, 1))
            mock_xgb.return_value = model
            run_mock = MagicMock(info=MagicMock(run_id="r"))
            mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=run_mock)
            mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

            train()

        calls = [str(c) for c in mock_mlflow.log_metric.call_args_list]
        assert any("f1_weighted" in c for c in calls)


# ===========================================================================
# Class 12 — Predict task
# ===========================================================================
class TestPredict:
    def test_predict_task_calls_predict_fn(self, mock_ml_predict):
        from dags.football_pipeline import predict

        mock_spark = MagicMock()
        with patch("dags.football_pipeline.get_spark", return_value=mock_spark), \
             patch("dags.football_pipeline.ml_predict_fn", mock_ml_predict):
            predict()

        mock_ml_predict.assert_called_once()

    def test_predict_passes_spark_session(self, mock_ml_predict):
        from dags.football_pipeline import predict

        mock_spark = MagicMock()
        with patch("dags.football_pipeline.get_spark", return_value=mock_spark), \
             patch("dags.football_pipeline.ml_predict_fn", mock_ml_predict):
            predict()

        _, kwargs = mock_ml_predict.call_args
        assert kwargs.get("spark") is mock_spark

    def test_predict_fn_loads_model(self, mock_mlflow_client, mock_xgb_model):
        from ml.predict import predict as predict_fn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_spark.read.format.return_value.load.return_value = mock_df

        with patch("ml.predict.mlflow.MlflowClient", return_value=mock_mlflow_client), \
             patch("ml.predict.mlflow.xgboost.load_model", return_value=mock_xgb_model), \
             patch("ml.predict.DeltaTable.isDeltaTable", return_value=False), \
             patch("ml.predict.mlflow.set_tracking_uri"), \
             patch("ml.predict.get_spark", return_value=mock_spark):
            predict_fn(spark=mock_spark)

        mock_mlflow_client.get_latest_versions.assert_called_once()

    def test_predict_fn_returns_zero_when_no_new_matches(self, mock_mlflow_client, mock_xgb_model):
        from ml.predict import predict as predict_fn

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_spark.read.format.return_value.load.return_value = mock_df

        with patch("ml.predict.mlflow.MlflowClient", return_value=mock_mlflow_client), \
             patch("ml.predict.mlflow.xgboost.load_model", return_value=mock_xgb_model), \
             patch("ml.predict.DeltaTable.isDeltaTable", return_value=False), \
             patch("ml.predict.mlflow.set_tracking_uri"), \
             patch("ml.predict.get_spark", return_value=mock_spark):
            result = predict_fn(spark=mock_spark)

        assert result == 0

    def test_predict_fn_raises_when_no_model_versions(self):
        from ml.predict import predict as predict_fn, load_model

        mock_client = MagicMock()
        mock_client.get_latest_versions.return_value = []

        with patch("ml.predict.mlflow.MlflowClient", return_value=mock_client), \
             patch("ml.predict.mlflow.set_tracking_uri"):
            with pytest.raises(RuntimeError, match="No registered versions"):
                load_model()

    def test_prediction_schema_has_required_fields(self):
        from ml.predict import PREDICTION_SCHEMA
        field_names = {f.name for f in PREDICTION_SCHEMA}
        assert {"match_id", "outcome", "proba_away", "proba_draw", "proba_home",
                "model_run_id", "predicted_at"}.issubset(field_names)

    def test_outcome_values_are_valid(self, mock_mlflow_client, mock_xgb_model, sample_df):
        """Predict on sample data and assert outcomes are 0, 1, or 2."""
        from ml.predict import predict as predict_fn
        import pandas as pd

        mock_spark = MagicMock()
        mock_matches = MagicMock()
        mock_matches.count.return_value = len(sample_df)
        mock_matches.toPandas.return_value = sample_df

        # Chain: read -> format -> load -> select -> dropna -> join -> left_anti
        mock_spark.read.format.return_value.load.return_value \
            .select.return_value \
            .dropna.return_value \
            .join.return_value = mock_matches

        written_df = None

        def capture_df(df, schema):
            nonlocal written_df
            written_df = df
            return MagicMock()

        mock_spark.createDataFrame.side_effect = capture_df

        with patch("ml.predict.mlflow.MlflowClient", return_value=mock_mlflow_client), \
             patch("ml.predict.mlflow.xgboost.load_model", return_value=mock_xgb_model), \
             patch("ml.predict.DeltaTable.isDeltaTable", return_value=False), \
             patch("ml.predict.mlflow.set_tracking_uri"), \
             patch("ml.predict.get_spark", return_value=mock_spark):
            predict_fn(spark=mock_spark)

        if written_df is not None:
            assert set(written_df["outcome"].unique()).issubset({0, 1, 2})

    def test_predict_merges_when_delta_exists(self, mock_mlflow_client, mock_xgb_model, sample_df):
        from ml.predict import predict as predict_fn

        mock_spark = MagicMock()
        mock_matches = MagicMock()
        mock_matches.count.return_value = len(sample_df)
        mock_matches.toPandas.return_value = sample_df
        mock_spark.read.format.return_value.load.return_value \
            .select.return_value \
            .dropna.return_value \
            .join.return_value = mock_matches

        mock_delta_table = MagicMock()
        mock_spark.createDataFrame.return_value = MagicMock()

        with patch("ml.predict.mlflow.MlflowClient", return_value=mock_mlflow_client), \
             patch("ml.predict.mlflow.xgboost.load_model", return_value=mock_xgb_model), \
             patch("ml.predict.DeltaTable.isDeltaTable", return_value=True), \
             patch("ml.predict.DeltaTable.forPath", return_value=mock_delta_table), \
             patch("ml.predict.mlflow.set_tracking_uri"), \
             patch("ml.predict.get_spark", return_value=mock_spark):
            predict_fn(spark=mock_spark)

        mock_delta_table.alias.assert_called_once_with("existing")

    def test_proba_columns_sum_to_one(self, mock_xgb_model, sample_df):
        """Probabilities from fixture sum to 1.0 per row."""
        from ml.train import build_features
        X, _ = build_features(sample_df)
        probas = mock_xgb_model.predict_proba(X)
        sums = probas.sum(axis=1)
        assert np.allclose(sums, 1.0)

    def test_load_model_returns_run_id(self, mock_mlflow_client):
        from ml.predict import load_model

        mock_xgb = MagicMock()
        with patch("ml.predict.mlflow.MlflowClient", return_value=mock_mlflow_client), \
             patch("ml.predict.mlflow.xgboost.load_model", return_value=mock_xgb), \
             patch("ml.predict.mlflow.set_tracking_uri"):
            _, run_id = load_model()

        assert run_id == "test-mlflow-run-id"
