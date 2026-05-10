"""
ml/predict.py
-------------
Load the latest production model from the MLflow Model Registry,
score a batch of matches from the Delta table, and write predictions
back to a separate Delta table (delta/predictions).

Prediction schema
  match_id   STRING
  outcome    INT       (0=away win, 1=draw, 2=home win)
  proba_away DOUBLE
  proba_draw DOUBLE
  proba_home DOUBLE
  run_id     STRING    (MLflow run that produced the model)
  predicted_at TIMESTAMP
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

import mlflow
import mlflow.xgboost
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ml.train import build_features, get_spark

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_ALIAS = os.getenv("ML_MODEL_ALIAS", "football_outcome_predictor")
DELTA_MATCHES_PATH = os.getenv(
    "DELTA_MATCHES_PATH", "s3a://football-data-lake/delta/matches"
)
DELTA_PREDICTIONS_PATH = os.getenv(
    "DELTA_PREDICTIONS_PATH", "s3a://football-data-lake/delta/predictions"
)

PREDICTION_SCHEMA = StructType(
    [
        StructField("match_id", StringType(), False),
        StructField("outcome", IntegerType(), False),
        StructField("proba_away", DoubleType(), False),
        StructField("proba_draw", DoubleType(), False),
        StructField("proba_home", DoubleType(), False),
        StructField("model_run_id", StringType(), False),
        StructField("predicted_at", TimestampType(), False),
    ]
)


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------
def load_model(alias: str = MODEL_ALIAS):
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.MlflowClient()
    # Fetch latest version in any stage (production-ready alias pattern)
    versions = client.get_latest_versions(alias)
    if not versions:
        raise RuntimeError(f"No registered versions found for model '{alias}'")
    latest = versions[-1]
    model_uri = f"models:/{alias}/{latest.version}"
    logger.info("Loading model %s version %s", alias, latest.version)
    return mlflow.xgboost.load_model(model_uri), latest.run_id


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def predict(
    spark: SparkSession | None = None,
    delta_path: str = DELTA_MATCHES_PATH,
    predictions_path: str = DELTA_PREDICTIONS_PATH,
) -> int:
    """
    Score all matches without an existing prediction.
    Returns count of new predictions written.
    """
    spark = spark or get_spark()
    model, run_id = load_model()

    matches_df = (
        spark.read.format("delta")
        .load(delta_path)
        .select("match_id", "season", "home_team", "away_team", "home_goals", "away_goals")
        .dropna()
    )

    # Exclude already-predicted match_ids
    if DeltaTable.isDeltaTable(spark, predictions_path):
        existing_ids = (
            spark.read.format("delta")
            .load(predictions_path)
            .select("match_id")
        )
        matches_df = matches_df.join(existing_ids, on="match_id", how="left_anti")

    count = matches_df.count()
    if count == 0:
        logger.info("No new matches to score.")
        return 0

    pandas_df = matches_df.toPandas()
    X, _ = build_features(pandas_df)

    probas = model.predict_proba(X)
    outcomes = model.predict(X)
    now = datetime.now(timezone.utc)

    results = pd.DataFrame(
        {
            "match_id": pandas_df["match_id"].values,
            "outcome": outcomes.astype(int),
            "proba_away": probas[:, 0],
            "proba_draw": probas[:, 1],
            "proba_home": probas[:, 2],
            "model_run_id": run_id,
            "predicted_at": now,
        }
    )

    result_spark = spark.createDataFrame(results, schema=PREDICTION_SCHEMA)

    if DeltaTable.isDeltaTable(spark, predictions_path):
        dt = DeltaTable.forPath(spark, predictions_path)
        dt.alias("existing").merge(
            result_spark.alias("new"),
            "existing.match_id = new.match_id",
        ).whenNotMatchedInsertAll().execute()
    else:
        result_spark.write.format("delta").mode("overwrite").save(predictions_path)

    logger.info("Wrote %d predictions to %s", count, predictions_path)
    return count


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    predict()
