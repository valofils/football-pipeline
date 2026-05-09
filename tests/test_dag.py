from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

DAG_PATH = Path(__file__).parents[1] / "dags" / "football_pipeline_dag.py"

SAMPLE_ROWS = [
    (1, "2023-24", "Arsenal",   "Chelsea",   2, 1, "2023-08-12"),
    (2, "2023-24", "Liverpool", "Man City",  1, 1, "2023-08-13"),
    (3, "2023-24", "Arsenal",   "Liverpool", 3, 2, "2023-09-01"),
]

SCHEMA = pa.schema([
    pa.field("match_id",   pa.int64()),
    pa.field("season",     pa.string()),
    pa.field("home_team",  pa.string()),
    pa.field("away_team",  pa.string()),
    pa.field("home_goals", pa.int32()),
    pa.field("away_goals", pa.int32()),
    pa.field("match_date", pa.date32()),
])


def make_sample_df() -> pd.DataFrame:
    return pd.DataFrame(SAMPLE_ROWS,
                        columns=["match_id", "season", "home_team", "away_team",
                                 "home_goals", "away_goals", "match_date"])


def write_sample_parquet(base: Path) -> Path:
    df  = make_sample_df()
    out = base / "season=2023-24" / "matches.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, schema=SCHEMA), out)
    return out


class TestDagStructure:

    def test_dag_loads_without_error(self):
        spec = importlib.util.spec_from_file_location("football_pipeline_dag", DAG_PATH)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert hasattr(mod, "football_pipeline")

    def test_dag_id(self):
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder=str(DAG_PATH.parent), include_examples=False)
        assert "football_pipeline" in dagbag.dags
        assert dagbag.import_errors == {}

    def test_task_ids_present(self):
        from airflow.models import DagBag
        dag = DagBag(dag_folder=str(DAG_PATH.parent), include_examples=False).dags["football_pipeline"]
        assert {t.task_id for t in dag.tasks} == {"ingest", "validate", "load_postgres", "build_standings"}

    def test_task_dependencies(self):
        from airflow.models import DagBag
        dag = DagBag(dag_folder=str(DAG_PATH.parent), include_examples=False).dags["football_pipeline"]
        def ds(tid): return {t.task_id for t in dag.get_task(tid).downstream_list}
        assert "validate"        in ds("ingest")
        assert "load_postgres"   in ds("validate")
        assert "build_standings" in ds("load_postgres")

    def test_schedule_is_weekly(self):
        from airflow.models import DagBag
        dag = DagBag(dag_folder=str(DAG_PATH.parent), include_examples=False).dags["football_pipeline"]
        assert dag.schedule_interval == "@weekly"

    def test_catchup_disabled(self):
        from airflow.models import DagBag
        dag = DagBag(dag_folder=str(DAG_PATH.parent), include_examples=False).dags["football_pipeline"]
        assert dag.catchup is False

    def test_retries_configured(self):
        from airflow.models import DagBag
        dag = DagBag(dag_folder=str(DAG_PATH.parent), include_examples=False).dags["football_pipeline"]
        for task in dag.tasks:
            assert task.retries == 2


class TestIngestTask:

    def _patch(self, dag_mod, tmp_path):
        dag_mod.DATA_DIR    = tmp_path
        dag_mod.PARQUET_DIR = tmp_path / "parquet"

    def _restore(self, dag_mod):
        dag_mod.DATA_DIR    = Path("/opt/airflow/data")
        dag_mod.PARQUET_DIR = Path("/opt/airflow/data/parquet")

    def test_returns_row_count_and_seasons(self, tmp_path):
        (tmp_path / "raw").mkdir()
        make_sample_df().to_csv(tmp_path / "raw" / "matches.csv", index=False)
        import dags.football_pipeline_dag as dag_mod
        self._patch(dag_mod, tmp_path)
        try:
            result = dag_mod.football_pipeline.ingest.function()
        finally:
            self._restore(dag_mod)
        assert result["row_count"] == 3
        assert result["seasons"]   == 1

    def test_raises_when_no_csvs(self, tmp_path):
        (tmp_path / "raw").mkdir()
        import dags.football_pipeline_dag as dag_mod
        self._patch(dag_mod, tmp_path)
        try:
            with pytest.raises(FileNotFoundError, match="No CSV files"):
                dag_mod.football_pipeline.ingest.function()
        finally:
            self._restore(dag_mod)

    def test_parquet_written_with_correct_schema(self, tmp_path):
        (tmp_path / "raw").mkdir()
        make_sample_df().to_csv(tmp_path / "raw" / "matches.csv", index=False)
        import dags.football_pipeline_dag as dag_mod
        self._patch(dag_mod, tmp_path)
        try:
            dag_mod.football_pipeline.ingest.function()
        finally:
            self._restore(dag_mod)
        files = list((tmp_path / "parquet").rglob("*.parquet"))
        assert len(files) == 1
        cols = set(pq.read_table(files[0]).schema.names)
        assert cols >= {"match_id", "home_team", "away_team", "home_goals", "away_goals", "season"}


class TestValidateTask:

    def test_passes_valid_data(self, tmp_path):
        write_sample_parquet(tmp_path)
        import dags.football_pipeline_dag as dag_mod
        dag_mod.PARQUET_DIR = tmp_path
        try:
            result = dag_mod.football_pipeline.validate.function({"row_count": 3, "seasons": 1})
        finally:
            dag_mod.PARQUET_DIR = Path("/opt/airflow/data/parquet")
        assert result["row_count"] == 3

    def test_raises_on_zero_rows(self, tmp_path):
        write_sample_parquet(tmp_path)
        import dags.football_pipeline_dag as dag_mod
        dag_mod.PARQUET_DIR = tmp_path
        try:
            with pytest.raises(ValueError, match="0 rows"):
                dag_mod.football_pipeline.validate.function({"row_count": 0, "seasons": 0})
        finally:
            dag_mod.PARQUET_DIR = Path("/opt/airflow/data/parquet")

    def test_raises_on_null_home_team(self, tmp_path):
        df = make_sample_df()
        df.loc[0, "home_team"] = None
        out = tmp_path / "season=2023-24" / "matches.parquet"
        out.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pandas(df), out)
        import dags.football_pipeline_dag as dag_mod
        dag_mod.PARQUET_DIR = tmp_path
        try:
            with pytest.raises(ValueError, match="home_team"):
                dag_mod.football_pipeline.validate.function({"row_count": 3, "seasons": 1})
        finally:
            dag_mod.PARQUET_DIR = Path("/opt/airflow/data/parquet")


class TestLoadPostgresTask:

    def test_calls_hook_create_table_and_insert(self, tmp_path):
        write_sample_parquet(tmp_path)
        mock_hook = MagicMock()
        import dags.football_pipeline_dag as dag_mod
        dag_mod.PARQUET_DIR = tmp_path
        try:
            with patch("dags.football_pipeline_dag.PostgresHook", return_value=mock_hook):
                result = dag_mod.football_pipeline.load_postgres.function({"row_count": 3, "seasons": 1})
        finally:
            dag_mod.PARQUET_DIR = Path("/opt/airflow/data/parquet")
        ddl = mock_hook.run.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS matches" in ddl
        kwargs = mock_hook.insert_rows.call_args[1]
        assert kwargs["table"]         == "matches"
        assert kwargs["replace"]       is True
        assert kwargs["replace_index"] == ["match_id"]
        assert result["loaded"]        == 3

    def test_result_contains_loaded_count(self, tmp_path):
        write_sample_parquet(tmp_path)
        mock_hook = MagicMock()
        import dags.football_pipeline_dag as dag_mod
        dag_mod.PARQUET_DIR = tmp_path
        try:
            with patch("dags.football_pipeline_dag.PostgresHook", return_value=mock_hook):
                result = dag_mod.football_pipeline.load_postgres.function({"row_count": 3, "seasons": 1})
        finally:
            dag_mod.PARQUET_DIR = Path("/opt/airflow/data/parquet")
        assert result["loaded"] == 3


class TestBuildStandingsTask:

    def test_calls_hook_run_with_create_view(self):
        mock_hook = MagicMock()
        import dags.football_pipeline_dag as dag_mod
        with patch("dags.football_pipeline_dag.PostgresHook", return_value=mock_hook):
            dag_mod.football_pipeline.build_standings.function({"row_count": 3, "seasons": 1, "loaded": 3})
        sql = mock_hook.run.call_args[0][0]
        assert "CREATE OR REPLACE VIEW standings" in sql
        assert "UNION ALL" in sql

    def test_standings_sql_contains_points_logic(self):
        mock_hook = MagicMock()
        import dags.football_pipeline_dag as dag_mod
        with patch("dags.football_pipeline_dag.PostgresHook", return_value=mock_hook):
            dag_mod.football_pipeline.build_standings.function({"row_count": 3, "seasons": 1, "loaded": 3})
        sql = mock_hook.run.call_args[0][0]
        assert "CASE WHEN home_goals > away_goals THEN 3" in sql
        assert "goal_diff" in sql
