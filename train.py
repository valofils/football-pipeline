"""
ml/train.py
-----------
v10 (unchanged in v11) — XGBoost match-outcome predictor.

Reads from delta/matches, engineers 4 features, trains an XGBoost
classifier, logs to MLflow, and registers the model under the alias
'football_outcome_predictor'.

Labels:
    0 = away win
    1 = draw
    2 = home win
"""

from __future__ import annotations

import logging
import os

import mlflow
import mlflow.xgboost
import pandas as pd
from mlflow import MlflowClient
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DELTA_PATH = os.getenv("DELTA_PATH", "s3a://football-data/delta/matches")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "football_outcome")
MODEL_NAME = "football_outcome_predictor"


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------
def build_features(df: pd.DataFrame):
    """Returns (X, y) numpy arrays from a matches DataFrame."""
    team_enc = LabelEncoder()
    season_enc = LabelEncoder()

    all_teams = pd.concat([df["home_team"], df["away_team"]])
    team_enc.fit(all_teams)
    season_enc.fit(df["season"])

    df = df.copy()
    df["home_team_enc"] = team_enc.transform(df["home_team"])
    df["away_team_enc"] = team_enc.transform(df["away_team"])
    df["season_enc"] = season_enc.transform(df["season"])
    df["goal_diff_hist"] = df["home_goals"] - df["away_goals"]

    # Label: 2=home win, 1=draw, 0=away win
    df["label"] = df.apply(
        lambda r: 2 if r["home_goals"] > r["away_goals"]
        else (1 if r["home_goals"] == r["away_goals"] else 0),
        axis=1,
    )

    feature_cols = ["home_team_enc", "away_team_enc", "season_enc", "goal_diff_hist"]
    X = df[feature_cols].values
    y = df["label"].values
    return X, y


# ---------------------------------------------------------------------------
# Train
# ---------------------------------------------------------------------------
def train(
    run_name: str = "football_outcome_run",
    spark=None,
) -> str:
    """Train model, log to MLflow, register, return run_id."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    if spark is None:
        builder = (
            SparkSession.builder.appName("football-train")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    df = (
        spark.read.format("delta")
        .load(DELTA_PATH)
        .select("match_id", "season", "home_team", "away_team", "home_goals", "away_goals")
        .dropna()
        .toPandas()
    )

    X, y = build_features(df)

    params = {
        "n_estimators": int(os.getenv("XGB_N_ESTIMATORS", "100")),
        "max_depth": int(os.getenv("XGB_MAX_DEPTH", "4")),
        "learning_rate": float(os.getenv("XGB_LR", "0.1")),
        "use_label_encoder": False,
        "eval_metric": "mlogloss",
        "num_class": 3,
        "objective": "multi:softprob",
    }

    with mlflow.start_run(run_name=run_name) as run:
        model = XGBClassifier(**params)
        model.fit(X, y)

        train_acc = float((model.predict(X) == y).mean())

        mlflow.log_params(params)
        mlflow.log_metric("train_accuracy", train_acc)
        mlflow.xgboost.log_model(model, artifact_path="model", registered_model_name=MODEL_NAME)

        run_id = run.info.run_id

    # Set alias on the latest version
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
    versions = client.get_latest_versions(MODEL_NAME, stages=["None"])
    if versions:
        client.set_registered_model_alias(MODEL_NAME, "champion", versions[-1].version)

    logger.info("Training complete. run_id=%s, accuracy=%.4f", run_id, train_acc)
    return run_id


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    train()
