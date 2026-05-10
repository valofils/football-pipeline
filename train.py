"""
ml/train.py
-----------
Train a match-outcome classifier on the Delta table and log the
artefact + metrics to MLflow (tracking server + S3 artefact store).

Outcome labels
  0 = away win   (home_goals < away_goals)
  1 = draw       (home_goals == away_goals)
  2 = home win   (home_goals > away_goals)
"""

from __future__ import annotations

import os
import logging
from typing import Tuple

import mlflow
import mlflow.xgboost
import pandas as pd
import xgboost as xgb
from pyspark.sql import SparkSession
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "football_outcome_predictor")
DELTA_MATCHES_PATH = os.getenv(
    "DELTA_MATCHES_PATH", "s3a://football-data-lake/delta/matches"
)
MODEL_ALIAS = "football_outcome_predictor"

XGB_PARAMS = {
    "n_estimators": int(os.getenv("XGB_N_ESTIMATORS", "200")),
    "max_depth": int(os.getenv("XGB_MAX_DEPTH", "4")),
    "learning_rate": float(os.getenv("XGB_LR", "0.05")),
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "use_label_encoder": False,
    "eval_metric": "mlogloss",
    "random_state": 42,
}


# ---------------------------------------------------------------------------
# Spark helpers
# ---------------------------------------------------------------------------
def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("football-ml-train")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def load_delta(spark: SparkSession, path: str) -> pd.DataFrame:
    return (
        spark.read.format("delta")
        .load(path)
        .select("season", "home_team", "away_team", "home_goals", "away_goals")
        .dropna()
        .toPandas()
    )


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------
def build_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """Return feature matrix X and label series y."""
    le_home = LabelEncoder()
    le_away = LabelEncoder()
    le_season = LabelEncoder()

    df = df.copy()
    df["home_team_enc"] = le_home.fit_transform(df["home_team"])
    df["away_team_enc"] = le_away.fit_transform(df["away_team"])
    df["season_enc"] = le_season.fit_transform(df["season"])
    df["goal_diff_hist"] = df["home_goals"] - df["away_goals"]  # historical proxy

    X = df[["home_team_enc", "away_team_enc", "season_enc", "goal_diff_hist"]]
    y = df.apply(_label, axis=1)
    return X, y


def _label(row: pd.Series) -> int:
    if row["home_goals"] > row["away_goals"]:
        return 2  # home win
    if row["home_goals"] == row["away_goals"]:
        return 1  # draw
    return 0  # away win


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------
def train(delta_path: str = DELTA_MATCHES_PATH, run_name: str | None = None) -> str:
    """
    Train, evaluate, log to MLflow, register model.
    Returns the MLflow run_id.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    spark = get_spark()
    logger.info("Loading Delta table from %s", delta_path)
    df = load_delta(spark, delta_path)
    logger.info("Loaded %d rows", len(df))

    X, y = build_features(df)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    with mlflow.start_run(run_name=run_name or "xgb_train") as run:
        mlflow.log_params(XGB_PARAMS)
        mlflow.log_param("train_rows", len(X_train))
        mlflow.log_param("test_rows", len(X_test))
        mlflow.log_param("delta_path", delta_path)

        model = xgb.XGBClassifier(**XGB_PARAMS)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred = model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")

        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_weighted", f1)
        logger.info("accuracy=%.4f  f1_weighted=%.4f", acc, f1)

        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_ALIAS,
        )
        logger.info("Model logged to MLflow run %s", run.info.run_id)
        return run.info.run_id


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    train()
