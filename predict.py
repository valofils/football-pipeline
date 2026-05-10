"""
ml/predict.py
-------------
v10 (unchanged in v11) — scores unscored matches and merges predictions
into delta/predictions via whenNotMatchedInsertAll (idempotent).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import mlflow
import pandas as pd
from mlflow import MlflowClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ml.train import build_features

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DELTA_PATH = os.getenv("DELTA_PATH", "s3a://football-data/delta/matches")
PREDICTIONS_PATH = os.getenv(
    "PREDICTIONS_DELTA_PATH", "s3a://football-data/delta/predictions"
)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = "football_outcome_predictor"

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
# Load model
# ---------------------------------------------------------------------------
def load_model():
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
    versions = client.get_latest_versions(MODEL_NAME)
    if not versions:
        raise RuntimeError(f"No registered versions found for model '{MODEL_NAME}'")
    latest = versions[-1]
    model = mlflow.xgboost.load_model(f"runs:/{latest.run_id}/model")
    return model, latest.run_id


# ---------------------------------------------------------------------------
# Predict
# ---------------------------------------------------------------------------
def predict(
    delta_path: str = DELTA_PATH,
    predictions_path: str = PREDICTIONS_PATH,
    spark: SparkSession | None = None,
) -> int:
    """Score unscored matches and merge into delta/predictions.

    Returns count of new predictions written.
    """
    from delta import DeltaTable, configure_spark_with_delta_pip

    if spark is None:
        builder = (
            SparkSession.builder.appName("football-predict")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    model, run_id = load_model()

    matches_df = (
        spark.read.format("delta")
        .load(delta_path)
        .select("match_id", "season", "home_team", "away_team", "home_goals", "away_goals")
        .dropna()
    )

    if DeltaTable.isDeltaTable(spark, predictions_path):
        existing_ids = spark.read.format("delta").load(predictions_path).select("match_id")
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
        (
            dt.alias("existing")
            .merge(result_spark.alias("new"), "existing.match_id = new.match_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        result_spark.write.format("delta").mode("overwrite").save(predictions_path)

    logger.info("Wrote %d predictions to %s", count, predictions_path)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    predict()
