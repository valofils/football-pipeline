"""
lineage/emitters.py
Per-stage lineage emitters for football-pipeline.

Each public function wraps one pipeline stage, calls emit_run_event
(START + COMPLETE / FAIL) and documents the datasets that stage reads
and writes.  Import these from the DAG or the stage's own module.
"""

from __future__ import annotations

import uuid
from typing import Optional

from openlineage.client.event_v2 import RunState

from lineage.ol_client import (
    delta_dataset,
    emit_run_event,
    kafka_dataset,
    lineage_run,
    postgres_dataset,
)

# ---------------------------------------------------------------------------
# Stage 1 – Kafka → Delta (streaming ingest)
# ---------------------------------------------------------------------------

MATCH_FIELDS = [
    {"name": "match_id", "type": "string"},
    {"name": "home_team", "type": "string"},
    {"name": "away_team", "type": "string"},
    {"name": "home_goals", "type": "integer"},
    {"name": "away_goals", "type": "integer"},
    {"name": "season", "type": "string"},
    {"name": "date", "type": "date"},
]


def emit_streaming_ingest(run_id: Optional[str] = None):
    """Context manager: Kafka football_matches → Delta delta/matches."""
    return lineage_run(
        job_name="streaming_ingest",
        inputs=[kafka_dataset("football_matches")],
        outputs=[delta_dataset("delta/matches", fields=MATCH_FIELDS, as_output=True)],
        description="Spark Structured Streaming: Kafka → Delta merge on match_id",
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Stage 2 – Great Expectations validation
# ---------------------------------------------------------------------------

def emit_validation(run_id: Optional[str] = None):
    """Context manager: Delta delta/matches → (validated in-place)."""
    return lineage_run(
        job_name="validate",
        inputs=[delta_dataset("delta/matches", fields=MATCH_FIELDS)],
        outputs=[delta_dataset("delta/matches", fields=MATCH_FIELDS, as_output=True)],
        description="Great Expectations suite: null checks, range checks, referential integrity",
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Stage 3 – Delta → Postgres load
# ---------------------------------------------------------------------------

POSTGRES_FIELDS = MATCH_FIELDS  # same schema

def emit_load_postgres(run_id: Optional[str] = None):
    """Context manager: Delta delta/matches → postgres matches table."""
    return lineage_run(
        job_name="load_postgres",
        inputs=[delta_dataset("delta/matches", fields=MATCH_FIELDS)],
        outputs=[postgres_dataset("public.matches", fields=POSTGRES_FIELDS, as_output=True)],
        description="JDBC batch load from Delta Lake into Postgres matches table",
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Stage 4 – dbt run
# ---------------------------------------------------------------------------

DBT_FIELDS = [
    {"name": "home_team", "type": "string"},
    {"name": "season", "type": "string"},
    {"name": "wins", "type": "integer"},
    {"name": "draws", "type": "integer"},
    {"name": "losses", "type": "integer"},
    {"name": "goals_for", "type": "integer"},
    {"name": "goals_against", "type": "integer"},
    {"name": "points", "type": "integer"},
]

DBT_MODELS_SQL = """
-- mart_team_season_stats
SELECT
    home_team,
    season,
    SUM(CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS draws,
    SUM(CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END) AS losses,
    SUM(home_goals) AS goals_for,
    SUM(away_goals) AS goals_against,
    SUM(CASE WHEN home_goals > away_goals THEN 3
             WHEN home_goals = away_goals THEN 1
             ELSE 0 END) AS points
FROM {{ ref('stg_matches') }}
GROUP BY 1, 2
"""


def emit_dbt_run(run_id: Optional[str] = None):
    """Context manager: postgres matches → postgres mart_team_season_stats."""
    return lineage_run(
        job_name="dbt_run",
        inputs=[postgres_dataset("public.matches")],
        outputs=[
            postgres_dataset(
                "public.mart_team_season_stats",
                fields=DBT_FIELDS,
                as_output=True,
            )
        ],
        sql=DBT_MODELS_SQL,
        description="dbt transformation: stg_matches → mart_team_season_stats",
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Stage 5 – ML training
# ---------------------------------------------------------------------------

MLFLOW_FIELDS = [
    {"name": "run_id", "type": "string"},
    {"name": "model_name", "type": "string"},
    {"name": "accuracy", "type": "double"},
    {"name": "registered_at", "type": "timestamp"},
]


def emit_ml_train(run_id: Optional[str] = None):
    """Context manager: Delta delta/matches → MLflow model registry."""
    return lineage_run(
        job_name="ml_train",
        inputs=[delta_dataset("delta/matches", fields=MATCH_FIELDS)],
        outputs=[
            {
                "namespace": "mlflow://mlflow:5000",
                "name": "football_outcome_predictor",
                "facets": {},
            }
        ],
        description="XGBoost training: feature engineering + MLflow logging + model registration",
        run_id=run_id,
    )


# ---------------------------------------------------------------------------
# Stage 6 – Predictions
# ---------------------------------------------------------------------------

PREDICTION_FIELDS = MATCH_FIELDS + [
    {"name": "predicted_outcome", "type": "integer"},
    {"name": "prob_away_win", "type": "double"},
    {"name": "prob_draw", "type": "double"},
    {"name": "prob_home_win", "type": "double"},
    {"name": "scored_at", "type": "timestamp"},
]


def emit_predict(run_id: Optional[str] = None):
    """Context manager: Delta delta/matches + MLflow model → Delta delta/predictions."""
    return lineage_run(
        job_name="predict",
        inputs=[
            delta_dataset("delta/matches", fields=MATCH_FIELDS),
            {"namespace": "mlflow://mlflow:5000", "name": "football_outcome_predictor", "facets": {}},
        ],
        outputs=[
            delta_dataset("delta/predictions", fields=PREDICTION_FIELDS, as_output=True)
        ],
        description="Batch inference: fetch registered model, score unscored matches, merge to delta/predictions",
        run_id=run_id,
    )
