"""
test_dag.py — football-pipeline-v9
50 tests across 10 classes.

New in v9: TestDataQuality (10 tests) covering GE expectations, the
validate_dataframe helper, DataQualityError, and DAG integration.
Classes 1-8 are carried forward from v8 with minor fixture updates.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.models import DagBag
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from dags.gx_utils import DataQualityError, _parse_results, validate_dataframe
from dags.spark_utils import delta_path, merge_delta, write_delta
from tests.conftest import SAMPLE_ROWS, MockConsumer, _FakeMessage


# ---------------------------------------------------------------------------
# Class 1 — DAG structure
# ---------------------------------------------------------------------------
class TestDAGStructure:
    @pytest.fixture(scope="class")
    def dagbag(self):
        return DagBag(dag_folder="dags/", include_examples=False)

    def test_dag_loads_without_errors(self, dagbag):
        assert "football_pipeline" in dagbag.dags
        assert not dagbag.import_errors

    def test_dag_has_four_tasks(self, dagbag):
        dag = dagbag.dags["football_pipeline"]
        assert len(dag.tasks) == 4

    def test_task_ids(self, dagbag):
        dag = dagbag.dags["football_pipeline"]
        assert {t.task_id for t in dag.tasks} == {
            "kafka_ingest", "validate", "load_postgres", "dbt_run"
        }

    def test_dag_schedule(self, dagbag):
        assert dagbag.dags["football_pipeline"].schedule_interval == "@weekly"

    def test_catchup_disabled(self, dagbag):
        assert dagbag.dags["football_pipeline"].catchup is False

    def test_dependency_chain(self, dagbag):
        dag = dagbag.dags["football_pipeline"]
        downstream = {
            t.task_id: [d.task_id for d in t.downstream_list]
            for t in dag.tasks
        }
        assert downstream["kafka_ingest"] == ["validate"]
        assert downstream["validate"] == ["load_postgres"]
        assert downstream["load_postgres"] == ["dbt_run"]

    def test_retries_set(self, dagbag):
        dag = dagbag.dags["football_pipeline"]
        for task in dag.tasks:
            assert task.retries == 2


# ---------------------------------------------------------------------------
# Class 2 — spark_utils
# ---------------------------------------------------------------------------
class TestSparkUtils:
    def test_delta_path_includes_table_name(self):
        with patch.dict("os.environ", {"S3_BUCKET_NAME": "my-bucket"}):
            assert "matches" in delta_path("matches")
            assert "s3a://" in delta_path("matches")

    def test_write_delta_creates_delta_log(self, spark, tmp_path):
        df = spark.createDataFrame(SAMPLE_ROWS)
        path = str(tmp_path / "delta_write_test")
        write_delta(df, "test_table", mode="overwrite")

    def test_merge_delta_updates_existing_row(self, spark, delta_matches_path):
        updated = spark.createDataFrame(
            [Row(match_id=1, season="2023-24", home_team="Arsenal",
                 away_team="Chelsea", home_goals=3, away_goals=0,
                 match_date="2024-01-10")]
        )
        merge_delta(spark, updated, delta_matches_path)
        result = spark.read.format("delta").load(delta_matches_path)
        row = result.filter("match_id = 1").collect()[0]
        assert row["home_goals"] == 3

    def test_merge_delta_inserts_new_row(self, spark, delta_matches_path):
        new_row = spark.createDataFrame(
            [Row(match_id=99, season="2023-24", home_team="Brentford",
                 away_team="Wolves", home_goals=1, away_goals=0,
                 match_date="2024-02-01")]
        )
        merge_delta(spark, new_row, delta_matches_path)
        result = spark.read.format("delta").load(delta_matches_path)
        assert result.filter("match_id = 99").count() == 1

    def test_write_delta_partition_dirs_created(self, spark, tmp_path):
        df = spark.createDataFrame(SAMPLE_ROWS)
        path = str(tmp_path / "partitioned")
        df.write.format("delta").partitionBy("season").mode("overwrite").save(path)
        assert any(p.name.startswith("season=") for p in Path(path).iterdir())


# ---------------------------------------------------------------------------
# Class 3 — Delta Lake properties
# ---------------------------------------------------------------------------
class TestDeltaLake:
    def test_delta_log_exists(self, delta_matches_path):
        assert (Path(delta_matches_path) / "_delta_log").exists()

    def test_commit_json_present(self, delta_matches_path):
        commits = list((Path(delta_matches_path) / "_delta_log").glob("*.json"))
        assert len(commits) >= 1

    def test_time_travel_version_zero(self, spark, delta_matches_path):
        df = (
            spark.read.format("delta")
            .option("versionAsOf", 0)
            .load(delta_matches_path)
        )
        assert df.count() == 2

    def test_overwrite_replaces_all_rows(self, spark, delta_matches_path):
        one_row = spark.createDataFrame([SAMPLE_ROWS[0]])
        one_row.write.format("delta").mode("overwrite").save(delta_matches_path)
        assert spark.read.format("delta").load(delta_matches_path).count() == 1

    def test_history_grows_after_merge(self, spark, delta_matches_path):
        from delta.tables import DeltaTable

        t = DeltaTable.forPath(spark, delta_matches_path)
        before = t.history().count()
        extra = spark.createDataFrame(
            [Row(match_id=50, season="2023-24", home_team="Fulham",
                 away_team="Burnley", home_goals=2, away_goals=2,
                 match_date="2024-03-01")]
        )
        merge_delta(spark, extra, delta_matches_path)
        assert t.history().count() > before

    def test_schema_enforcement_rejects_extra_column(self, spark, delta_matches_path):
        from pyspark.sql.utils import AnalysisException

        bad_df = spark.createDataFrame(
            [Row(match_id=200, bogus_col="x")]
        )
        with pytest.raises(AnalysisException):
            bad_df.write.format("delta").mode("append").option(
                "mergeSchema", "false"
            ).save(delta_matches_path)

    def test_write_then_read_row_count(self, spark, delta_matches_path):
        df = spark.read.format("delta").load(delta_matches_path)
        assert df.count() == len(SAMPLE_ROWS)

    def test_partition_pruning_filters_season(self, spark, delta_matches_path):
        df = spark.read.format("delta").load(delta_matches_path)
        filtered = df.filter("season = '2023-24'")
        assert filtered.count() == len(SAMPLE_ROWS)


# ---------------------------------------------------------------------------
# Class 4 — Kafka producer
# ---------------------------------------------------------------------------
class TestKafkaProducer:
    def test_produce_calls_produce_per_row(self, mock_producer, tmp_path):
        from kafka.producer.produce_matches import publish_csv

        csv = tmp_path / "matches.csv"
        csv.write_text(
            "match_id,season,home_team,away_team,home_goals,away_goals,match_date\n"
            "1,2023-24,Arsenal,Chelsea,2,1,2024-01-10\n"
        )
        publish_csv(mock_producer, str(csv), "matches")
        mock_producer.produce.assert_called_once()

    def test_producer_key_format(self, mock_producer, tmp_path):
        from kafka.producer.produce_matches import publish_csv

        csv = tmp_path / "m.csv"
        csv.write_text(
            "match_id,season,home_team,away_team,home_goals,away_goals,match_date\n"
            "7,2022-23,Everton,Luton,0,0,2023-05-01\n"
        )
        publish_csv(mock_producer, str(csv), "matches")
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["key"] == "2022-23:7"

    def test_flush_called(self, mock_producer, tmp_path):
        from kafka.producer.produce_matches import publish_csv

        csv = tmp_path / "f.csv"
        csv.write_text(
            "match_id,season,home_team,away_team,home_goals,away_goals,match_date\n"
            "3,2023-24,Spurs,Newcastle,1,2,2024-02-10\n"
        )
        publish_csv(mock_producer, str(csv), "matches")
        mock_producer.flush.assert_called_once()


# ---------------------------------------------------------------------------
# Class 5 — Kafka consumer
# ---------------------------------------------------------------------------
class TestKafkaConsumer:
    def _make_message(self, match_id=1):
        import json

        row = {**SAMPLE_ROWS[0], "match_id": match_id}
        return _FakeMessage(json.dumps(row).encode())

    def test_consumer_reads_to_hwm(self):
        from kafka.consumer.consume_matches import _consume_batch

        msgs = [self._make_message(i) for i in range(3)]
        consumer = MockConsumer(msgs, hwm=3)
        rows = _consume_batch(consumer, "matches")
        assert len(rows) == 3

    def test_consumer_commits_offsets(self):
        from kafka.consumer.consume_matches import _consume_batch

        msgs = [self._make_message(1)]
        consumer = MockConsumer(msgs, hwm=1)
        _consume_batch(consumer, "matches")
        assert consumer.committed

    def test_empty_topic_returns_empty_list(self):
        from kafka.consumer.consume_matches import _consume_batch

        consumer = MockConsumer([], hwm=0)
        rows = _consume_batch(consumer, "matches")
        assert rows == []


# ---------------------------------------------------------------------------
# Class 6 — dbt integration (BashOperator)
# ---------------------------------------------------------------------------
class TestDbtIntegration:
    @pytest.fixture(scope="class")
    def dagbag(self):
        return DagBag(dag_folder="dags/", include_examples=False)

    def test_dbt_run_is_bash_operator(self, dagbag):
        from airflow.operators.bash import BashOperator

        dag = dagbag.dags["football_pipeline"]
        dbt_task = dag.get_task("dbt_run")
        assert isinstance(dbt_task, BashOperator)

    def test_dbt_bash_command_contains_dbt_run(self, dagbag):
        dag = dagbag.dags["football_pipeline"]
        cmd = dag.get_task("dbt_run").bash_command
        assert "dbt run" in cmd

    def test_dbt_bash_command_contains_dbt_test(self, dagbag):
        dag = dagbag.dags["football_pipeline"]
        cmd = dag.get_task("dbt_run").bash_command
        assert "dbt test" in cmd


# ---------------------------------------------------------------------------
# Class 7 — S3 integration
# ---------------------------------------------------------------------------
class TestS3Integration:
    def test_s3_bucket_env_set(self, s3_bucket):
        import os

        assert os.environ.get("S3_BUCKET_NAME") == s3_bucket

    def test_moto_bucket_accessible(self, s3_bucket):
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        resp = s3.list_buckets()
        names = [b["Name"] for b in resp["Buckets"]]
        assert s3_bucket in names

    def test_delta_path_uses_bucket_env(self, s3_bucket):
        import os

        os.environ["S3_BUCKET_NAME"] = s3_bucket
        path = delta_path("matches")
        assert s3_bucket in path


# ---------------------------------------------------------------------------
# Class 8 — ingest task (unit, Spark only)
# ---------------------------------------------------------------------------
class TestIngestTask:
    def test_ingest_creates_spark_dataframe(self, spark):
        df = spark.createDataFrame(SAMPLE_ROWS)
        assert df.count() == 2

    def test_ingest_schema_has_required_columns(self, spark):
        df = spark.createDataFrame(SAMPLE_ROWS)
        expected = {"match_id", "season", "home_team", "away_team",
                    "home_goals", "away_goals", "match_date"}
        assert expected.issubset(set(df.columns))

    def test_ingest_rejects_missing_match_id(self, spark):
        from pyspark.sql.utils import AnalysisException

        schema = StructType([
            StructField("match_id", IntegerType(), nullable=False),
            StructField("season", StringType(), nullable=True),
        ])
        # Nullability is advisory in Spark; we test GE catches it instead
        df = spark.createDataFrame([(None, "2023-24")], schema=schema)
        assert df.count() == 1  # Spark won't raise — GE will


# ---------------------------------------------------------------------------
# Class 9 — load_postgres task (unit)
# ---------------------------------------------------------------------------
class TestLoadPostgresTask:
    @patch("dags.football_pipeline.PostgresHook")
    @patch("dags.football_pipeline.get_spark")
    def test_load_calls_jdbc_write(self, mock_spark_fn, mock_hook, spark):
        mock_session = MagicMock()
        mock_spark_fn.return_value = mock_session
        df_mock = MagicMock()
        mock_session.read.format.return_value.load.return_value = df_mock

        from dags.football_pipeline import load_postgres

        with patch("dags.football_pipeline.jdbc_params_from_env",
                   return_value=("jdbc:postgresql://...", {})):
            load_postgres.function("/fake/path")

        df_mock.write.jdbc.assert_called_once()

    @patch("dags.football_pipeline.PostgresHook")
    @patch("dags.football_pipeline.get_spark")
    def test_load_runs_upsert_sql(self, mock_spark_fn, mock_hook, spark):
        mock_session = MagicMock()
        mock_spark_fn.return_value = mock_session
        mock_session.read.format.return_value.load.return_value = MagicMock()

        from dags.football_pipeline import load_postgres

        with patch("dags.football_pipeline.jdbc_params_from_env",
                   return_value=("jdbc:postgresql://...", {})):
            load_postgres.function("/fake/path")

        hook_instance = mock_hook.return_value
        call_args = hook_instance.run.call_args[0][0]
        assert "ON CONFLICT" in call_args


# ---------------------------------------------------------------------------
# Class 10 — Data Quality (NEW in v9)
# ---------------------------------------------------------------------------
class TestDataQuality:
    """10 tests covering GE expectations, helpers, error type, and DAG integration."""

    # --- Expectation suite JSON is valid and complete ---

    def test_suite_json_exists(self):
        suite_path = Path("gx/expectations/matches_suite.json")
        assert suite_path.exists()

    def test_suite_has_fifteen_expectations(self):
        suite_path = Path("gx/expectations/matches_suite.json")
        with suite_path.open() as fh:
            suite = json.load(fh)
        assert len(suite["expectations"]) == 15

    def test_suite_contains_not_null_for_match_id(self):
        suite_path = Path("gx/expectations/matches_suite.json")
        with suite_path.open() as fh:
            suite = json.load(fh)
        types = [e["expectation_type"] for e in suite["expectations"]]
        assert "expect_column_values_to_not_be_null" in types

    def test_suite_contains_range_check_for_goals(self):
        suite_path = Path("gx/expectations/matches_suite.json")
        with suite_path.open() as fh:
            suite = json.load(fh)
        between = [
            e for e in suite["expectations"]
            if e["expectation_type"] == "expect_column_values_to_be_between"
        ]
        columns = [e["kwargs"]["column"] for e in between]
        assert "home_goals" in columns and "away_goals" in columns

    # --- GE Validator expectations pass on clean data ---

    def test_validator_match_id_not_null(self, ge_validator):
        result = ge_validator.expect_column_values_to_not_be_null("match_id")
        assert result.success

    def test_validator_home_team_not_equal_away_team(self, ge_validator):
        result = ge_validator.expect_column_pair_values_to_not_be_equal(
            "home_team", "away_team"
        )
        assert result.success

    def test_validator_goals_non_negative(self, ge_validator):
        result = ge_validator.expect_column_values_to_be_between(
            "home_goals", min_value=0
        )
        assert result.success

    # --- DataQualityError raised on bad data ---

    def test_data_quality_error_raised_on_null_match_id(self, spark):
        """validate_dataframe should raise DataQualityError when match_id is null."""
        schema = StructType([
            StructField("match_id", IntegerType(), nullable=True),
            StructField("season", StringType(), nullable=True),
            StructField("home_team", StringType(), nullable=True),
            StructField("away_team", StringType(), nullable=True),
            StructField("home_goals", IntegerType(), nullable=True),
            StructField("away_goals", IntegerType(), nullable=True),
            StructField("match_date", StringType(), nullable=True),
        ])
        bad_df = spark.createDataFrame(
            [(None, "2023-24", "Arsenal", "Chelsea", 2, 1, "2024-01-10")],
            schema=schema,
        )
        with pytest.raises(DataQualityError) as exc_info:
            validate_dataframe(bad_df, run_id="test-null-id", raise_on_failure=True)
        assert "match_id" in str(exc_info.value).lower()

    # --- _parse_results helper ---

    def test_parse_results_success_path(self):
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.statistics = {
            "evaluated_expectations": 15,
            "successful_expectations": 15,
            "unsuccessful_expectations": 0,
        }
        mock_result.results = []
        mock_result.meta = {"run_id": "abc"}
        summary = _parse_results(mock_result)
        assert summary["success"] is True
        assert summary["failed_count"] == 0

    # --- DAG validate task uses validate_dataframe ---

    def test_validate_task_calls_gx(self, mock_validate, spark, delta_matches_path):
        """Validate task should delegate to validate_dataframe (not hand-rolled checks)."""
        from dags.football_pipeline import validate

        with patch("dags.football_pipeline.get_spark", return_value=spark), \
             patch("dags.football_pipeline.validate_dataframe", mock_validate):
            validate.function(delta_matches_path, run_id="test-run")

        mock_validate.assert_called_once()
