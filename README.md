# football-pipeline-v10 — MLOps (MLflow + XGBoost)

Adds a match-outcome predictor trained on the Delta table and tracked
via MLflow. Builds directly on v9 (Great Expectations) with no schema
changes to existing tables.

---

## What changed from v9

| Component | v9 | v10 |
|---|---|---|
| `ml_train` task | — | trains XGBoost, logs metrics + model to MLflow |
| `predict` task | — | scores new matches, merges to `delta/predictions` |
| MLflow tracking server | — | new Docker service on port 5000 |
| DAG chain | `… >> dbt_run` | `… >> [dbt_run, ml_train] >> predict` |
| New packages | — | `mlflow==2.13.0`, `xgboost==2.0.3`, `scikit-learn==1.5.0` |
| Tests | 50 (10 classes) | **60 (12 classes)** |
| New files | — | `ml/train.py`, `ml/predict.py`, `mlflow/mlflow.env`, `scripts/init_mlflow_db.sql` |

---

## Architecture

```
kafka_ingest
     │
  validate  (Great Expectations checkpoint → S3 Data Docs)
     │
load_postgres
     ├── dbt_run          (existing analytical models)
     └── ml_train         ← NEW: XGBoost, logs to MLflow
              │
           predict        ← NEW: scores matches → delta/predictions
```

---

## New concepts

### Outcome labels
| Value | Meaning |
|---|---|
| 0 | Away win |
| 1 | Draw |
| 2 | Home win |

### Features used
`home_team_enc`, `away_team_enc`, `season_enc`, `goal_diff_hist`
(all derived from existing Delta columns — no new data sources).

### MLflow Model Registry
`ml_train` registers the model under the alias
`football_outcome_predictor`. `predict` fetches the latest version via
`MlflowClient.get_latest_versions()` and calls `predict_proba` to emit
per-class probabilities alongside the hard label.

### Prediction Delta table
Written to `s3a://football-data-lake/delta/predictions`. New rows are
merged (`whenNotMatchedInsertAll`) so re-runs are idempotent.

---

## Running locally

```bash
# Start all services (includes MLflow on :5000)
docker compose up -d

# Trigger DAG manually
airflow dags trigger football_pipeline

# Open MLflow UI
open http://localhost:5000

# Run tests
pytest tests/ -v --cov=dags --cov=ml --cov-report=term-missing
# Expected: 60 passed
```

---

## MLflow UI quick tour

| Screen | What to look for |
|---|---|
| Experiments → `football_outcome_predictor` | Every DAG run creates one MLflow run |
| Run detail → Metrics | `accuracy`, `f1_weighted` |
| Run detail → Params | XGB hyperparameters + `train_rows`, `delta_path` |
| Models → `football_outcome_predictor` | Registered versions; latest auto-loaded by `predict` |
| Run detail → Artifacts → `model/` | `model.xgb`, `conda.yaml`, `MLmodel` |

---

## Full stack (end of v10)

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Messaging | Apache Kafka 3.7 · confluent-kafka 2.4.0 |
| DataFrames | PySpark 3.5.1 |
| Storage format | Delta Lake 3.2.0 |
| Serialisation | JSON · PyArrow 15 · Parquet/Snappy |
| Database | PostgreSQL 16 · psycopg2 · JDBC · AWS RDS |
| Object storage | AWS S3 · s3a:// |
| Modelling | dbt-core 1.8.3 · dbt-postgres |
| **ML** | **XGBoost 2.0.3 · scikit-learn 1.5.0** |
| **Experiment tracking** | **MLflow 2.13.0 (PostgreSQL backend · S3 artefacts)** |
| Data quality | Great Expectations 0.18.19 |
| Testing | pytest · pytest-cov · moto |
| Orchestration | Apache Airflow 2.9.1 · LocalExecutor |
| Infrastructure | Docker Compose · Terraform 1.7+ |
