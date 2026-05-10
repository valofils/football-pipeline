"""
lineage/emitters.py — Stage-specific OpenLineage context managers.

Each function wraps one pipeline stage, hard-coding the correct
input/output datasets with schema and SQL facets where applicable.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Optional

from lineage.ol_client import (
    lineage_run,
    kafka_dataset,
    delta_dataset,
    postgres_dataset,
)

# ---------------------------------------------------------------------------
# Shared field schemas
# ---------------------------------------------------------------------------

MATCH_FIELDS = [
    ("match_id", "STRING"),
    ("home_team", "STRING"),
    ("away_team", "STRING"),
    ("home_score", "INTEGER"),
    ("away_score", "INTEGER"),
    ("match_date", "DATE"),
    ("competition", "STRING"),
    ("season", "STRING"),
    ("stadium", "STRING"),
    ("referee", "STRING"),
]

PREDICTION_FIELDS = [
    ("match_id", "STRING"),
    ("predicted_winner", "STRING"),
    ("home_win_prob", "DOUBLE"),
    ("draw_prob", "DOUBLE"),
    ("away_win_prob", "DOUBLE"),
    ("model_version", "STRING"),
    ("predicted_at", "TIMESTAMP"),
]

MART_FIELDS = [
    ("team", "STRING"),
    ("season", "STRING"),
    ("wins", "INTEGER"),
    ("draws", "INTEGER"),
    ("losses", "INTEGER"),
    ("goals_for", "INTEGER"),
    ("goals_against", "INTEGER"),
    ("points", "INTEGER"),
]


# ---------------------------------------------------------------------------
# Stage emitters
# ---------------------------------------------------------------------------

@contextmanager
def emit_streaming_ingest(run_id: Optional[str] = None):
    """
    Stage 1 — Kafka → Delta Lake (matches).

    Consumes football_matches topic; writes delta/matches.
    """
    src = kafka_dataset("football_matches", MATCH_FIELDS)
    dst = delta_dataset("matches", MATCH_FIELDS)
    with lineage_run(
        "streaming_ingest",
        run_id=run_id,
        inputs=[src],
        outputs=[dst],
        description="Consume raw match events from Kafka and land to Delta Lake",
    ) as rid:
        yield rid


@contextmanager
def emit_validation(run_id: Optional[str] = None):
    """
    Stage 2 — Validate Delta Lake matches (in-place read + write).

    Reads delta/matches, applies Great Expectations suite, rewrites
    valid records; quarantines bad rows to delta/matches_quarantine.
    """
    src = delta_dataset("matches", MATCH_FIELDS)
    dst = delta_dataset("matches", MATCH_FIELDS)
    quarantine = delta_dataset("matches_quarantine", MATCH_FIELDS)
    with lineage_run(
        "validate_matches",
        run_id=run_id,
        inputs=[src],
        outputs=[dst, quarantine],
        description="Great Expectations validation; quarantine invalid rows",
    ) as rid:
        yield rid


@contextmanager
def emit_load_postgres(run_id: Optional[str] = None):
    """
    Stage 3 — Delta Lake → PostgreSQL public.matches.
    """
    src = delta_dataset("matches", MATCH_FIELDS)
    dst = postgres_dataset("public.matches", MATCH_FIELDS)
    sql = (
        "INSERT INTO public.matches "
        "SELECT * FROM delta.matches "
        "ON CONFLICT (match_id) DO UPDATE SET "
        "home_score=EXCLUDED.home_score, away_score=EXCLUDED.away_score"
    )
    with lineage_run(
        "load_postgres",
        run_id=run_id,
        inputs=[src],
        outputs=[dst],
        sql=sql,
        description="Upsert validated matches from Delta into Postgres",
    ) as rid:
        yield rid


@contextmanager
def emit_dbt_run(run_id: Optional[str] = None):
    """
    Stage 4 — dbt transformation: public.matches → public.mart_team_season_stats.
    """
    src = postgres_dataset("public.matches", MATCH_FIELDS)
    dst = postgres_dataset("public.mart_team_season_stats", MART_FIELDS)
    sql = (
        "SELECT team, season, "
        "SUM(CASE WHEN winner=team THEN 1 ELSE 0 END) AS wins, "
        "SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws, "
        "SUM(CASE WHEN winner!=team AND winner IS NOT NULL THEN 1 ELSE 0 END) AS losses, "
        "SUM(goals_for) AS goals_for, SUM(goals_against) AS goals_against, "
        "SUM(CASE WHEN winner=team THEN 3 WHEN winner IS NULL THEN 1 ELSE 0 END) AS points "
        "FROM public.matches GROUP BY team, season"
    )
    with lineage_run(
        "dbt_transform",
        run_id=run_id,
        inputs=[src],
        outputs=[dst],
        sql=sql,
        description="dbt model: aggregate per-team season statistics",
    ) as rid:
        yield rid


@contextmanager
def emit_ml_train(run_id: Optional[str] = None):
    """
    Stage 5 — Train ML model: public.matches → MLflow model registry.
    """
    src = postgres_dataset("public.matches", MATCH_FIELDS)
    from openlineage.client.run import Dataset
    from openlineage.client.facet import DataSourceDatasetFacet

    model_ds = Dataset(
        namespace="mlflow://mlflow:5000",
        name="football_outcome_predictor",
        facets={
            "dataSource": DataSourceDatasetFacet(
                name="mlflow://mlflow:5000/football_outcome_predictor",
                uri="mlflow://mlflow:5000/football_outcome_predictor",
            )
        },
    )
    with lineage_run(
        "ml_train",
        run_id=run_id,
        inputs=[src],
        outputs=[model_ds],
        description="Train gradient-boosted outcome predictor; register in MLflow",
    ) as rid:
        yield rid


@contextmanager
def emit_predict(run_id: Optional[str] = None):
    """
    Stage 6 — Batch inference: MLflow model → Delta Lake predictions.
    """
    from openlineage.client.run import Dataset
    from openlineage.client.facet import DataSourceDatasetFacet

    model_ds = Dataset(
        namespace="mlflow://mlflow:5000",
        name="football_outcome_predictor",
        facets={
            "dataSource": DataSourceDatasetFacet(
                name="mlflow://mlflow:5000/football_outcome_predictor",
                uri="mlflow://mlflow:5000/football_outcome_predictor",
            )
        },
    )
    src = postgres_dataset("public.matches", MATCH_FIELDS)
    dst = delta_dataset("predictions", PREDICTION_FIELDS)
    with lineage_run(
        "batch_predict",
        run_id=run_id,
        inputs=[src, model_ds],
        outputs=[dst],
        description="Batch prediction run; write results to Delta Lake",
    ) as rid:
        yield rid
